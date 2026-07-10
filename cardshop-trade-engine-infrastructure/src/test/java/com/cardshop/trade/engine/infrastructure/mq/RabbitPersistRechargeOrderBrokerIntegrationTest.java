package com.cardshop.trade.engine.infrastructure.mq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.PersistSource;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineInfrastructureConfiguration;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

/**
 * RabbitMQ 真实 broker 集成测试。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「7. RabbitMQ 持久化消息层」和
 * 「8.3 异步落库 Consumer」。该测试不会默认连接本机 RabbitMQ，只有显式设置
 * {@code TRADE_ENGINE_RABBITMQ_IT_ENABLED=true} 时才执行，避免普通单元测试依赖外部中间件。
 *
 * <p>执行示例：
 * {@code TRADE_ENGINE_RABBITMQ_IT_ENABLED=true TRADE_ENGINE_RABBITMQ_HOST=127.0.0.1 mvn -pl cardshop-trade-engine-infrastructure -Dtest=RabbitPersistRechargeOrderBrokerIntegrationTest test}
 */
class RabbitPersistRechargeOrderBrokerIntegrationTest {

    private static final String ENABLED_ENV = "TRADE_ENGINE_RABBITMQ_IT_ENABLED";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private TradeEngineProperties properties;
    private CachingConnectionFactory connectionFactory;
    private RabbitAdmin rabbitAdmin;
    private RabbitTemplate rabbitTemplate;
    private SimpleMessageListenerContainer container;

    @BeforeEach
    void setUp() {
        assumeTrue(Boolean.parseBoolean(env(ENABLED_ENV, "false")),
                () -> "设置 " + ENABLED_ENV + "=true 后执行 RabbitMQ 真实 broker 集成测试");

        String namespace = "trade.engine.it." + UUID.randomUUID().toString().replace("-", "");
        properties = integrationProperties(namespace);
        connectionFactory = connectionFactory();
        rabbitAdmin = new RabbitAdmin(connectionFactory);
        rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setReceiveTimeout(500);
        declarePersistTopology();
    }

    @AfterEach
    void tearDown() {
        if (container != null) {
            container.stop();
            container.destroy();
        }
        if (rabbitAdmin != null && properties != null) {
            rabbitAdmin.deleteQueue(properties.getMq().getPersistQueue());
            rabbitAdmin.deleteQueue(properties.getMq().getPersistRetryQueue());
            rabbitAdmin.deleteQueue(properties.getMq().getPersistDeadLetterQueue());
            rabbitAdmin.deleteExchange(properties.getMq().getPersistExchange());
            rabbitAdmin.deleteExchange(properties.getMq().getPersistRetryExchange());
            rabbitAdmin.deleteExchange(properties.getMq().getPersistDeadLetterExchange());
        }
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @Test
    void declaresQuorumMainQueueDlxAndDlqOnRealBroker() throws Exception {
        try (BrokerChannel brokerChannel = brokerChannel()) {
            assertThat(brokerChannel.channel.queueDeclarePassive(properties.getMq().getPersistQueue()).getQueue())
                    .isEqualTo(properties.getMq().getPersistQueue());
            assertThat(brokerChannel.channel.queueDeclarePassive(properties.getMq().getPersistDeadLetterQueue()).getQueue())
                    .isEqualTo(properties.getMq().getPersistDeadLetterQueue());
        }

        rabbitTemplate.send(
                properties.getMq().getPersistExchange(),
                properties.getMq().getPersistRoutingKey(),
                persistentMessage("route-probe"));

        Message message = receiveFrom(properties.getMq().getPersistQueue(), Duration.ofSeconds(5));
        assertThat(message).isNotNull();
        assertThat(new String(message.getBody(), StandardCharsets.UTF_8)).isEqualTo("route-probe");
    }

    @Test
    void listenerPublishesToRetryQueueAndThenAckRemovesMessage() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch secondDeliveryAcked = new CountDownLatch(1);
        AtomicReference<String> messageId = new AtomicReference<>();
        RabbitPersistRechargeOrderMessageListener listener = new RabbitPersistRechargeOrderMessageListener(
                objectMapper,
                message -> {
                    messageId.set(message.messageId());
                    if (attempts.incrementAndGet() == 1) {
                        return PersistRechargeOrderResult.persistFailed(
                                PersistSource.REDIS_SNAPSHOT,
                                "MYSQL_PERSIST_FAILED",
                                "integration test forces republish");
                    }
                    secondDeliveryAcked.countDown();
                    return PersistRechargeOrderResult.persisted(PersistSource.REDIS_SNAPSHOT);
                },
                retryPublisher(),
                maxRetry());
        startContainer((message, channel) -> listener.onMessage(message, channel));

        sendPersistMessage("persist-it-requeue");

        assertThat(secondDeliveryAcked.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(attempts.get()).isEqualTo(2);
        assertThat(messageId.get()).isEqualTo("persist-it-requeue");
        assertThat(waitUntil(
                () -> queueMessageCount(properties.getMq().getPersistQueue()) == 0,
                Duration.ofSeconds(5))).isTrue();
    }

    @Test
    void malformedPayloadIsRejectedWithoutRequeueAndRoutedToDlq() throws Exception {
        RabbitPersistRechargeOrderMessageListener listener = new RabbitPersistRechargeOrderMessageListener(
                objectMapper,
                ignored -> {
                    throw new AssertionError("反序列化失败时不应调用 application consumer");
                },
                retryPublisher(),
                maxRetry());
        startContainer((message, channel) -> listener.onMessage(message, channel));

        rabbitTemplate.send(
                properties.getMq().getPersistExchange(),
                properties.getMq().getPersistRoutingKey(),
                persistentMessage("{bad-json"));

        Message dlqMessage = receiveFrom(properties.getMq().getPersistDeadLetterQueue(), Duration.ofSeconds(10));
        assertThat(dlqMessage).isNotNull();
        assertThat(new String(dlqMessage.getBody(), StandardCharsets.UTF_8)).isEqualTo("{bad-json");
        assertThat(waitUntil(
                () -> queueMessageCount(properties.getMq().getPersistQueue()) == 0,
                Duration.ofSeconds(5))).isTrue();
    }

    @Test
    void unackedMessageIsReplayedAfterConsumerChannelCloses() throws Exception {
        sendPersistMessage("persist-it-restart");

        try (BrokerChannel brokerChannel = brokerChannel()) {
            GetResponse firstDelivery = waitForBasicGet(
                    brokerChannel.channel,
                    properties.getMq().getPersistQueue(),
                    false,
                    Duration.ofSeconds(5));
            assertThat(firstDelivery).isNotNull();

            // 模拟 Consumer 进程在 manual ack 前退出：关闭 channel/connection 后，broker 必须重新投递未 ack 消息。
        }

        CountDownLatch replayAcked = new CountDownLatch(1);
        AtomicReference<String> replayedMessageId = new AtomicReference<>();
        RabbitPersistRechargeOrderMessageListener listener = new RabbitPersistRechargeOrderMessageListener(
                objectMapper,
                message -> {
                    replayedMessageId.set(message.messageId());
                    replayAcked.countDown();
                    return PersistRechargeOrderResult.persisted(PersistSource.REDIS_SNAPSHOT);
                },
                retryPublisher(),
                maxRetry());
        startContainer((message, channel) -> listener.onMessage(message, channel));

        assertThat(replayAcked.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(replayedMessageId.get()).isEqualTo("persist-it-restart");
        assertThat(waitUntil(
                () -> queueMessageCount(properties.getMq().getPersistQueue()) == 0,
                Duration.ofSeconds(5))).isTrue();
    }

    private void declarePersistTopology() {
        var declarables = new TradeEngineInfrastructureConfiguration().rechargePersistDeclarables(properties);
        for (Declarable declarable : declarables.getDeclarables()) {
            if (declarable instanceof Exchange exchange) {
                rabbitAdmin.declareExchange(exchange);
            } else if (declarable instanceof Queue queue) {
                rabbitAdmin.declareQueue(queue);
            } else if (declarable instanceof Binding binding) {
                rabbitAdmin.declareBinding(binding);
            }
        }
    }

    private void startContainer(ChannelAwareMessageListener messageListener) {
        container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(properties.getMq().getPersistQueue());
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setConcurrentConsumers(1);
        container.setPrefetchCount(1);
        container.setShutdownTimeout(500);
        container.setMessageListener(messageListener);
        container.start();
    }

    private void sendPersistMessage(String messageId) throws Exception {
        rabbitTemplate.send(
                properties.getMq().getPersistExchange(),
                properties.getMq().getPersistRoutingKey(),
                persistentMessage(objectMapper.writeValueAsString(new PersistTradeOrderMessage(
                        messageId,
                        "A36",
                        "ORDER-" + messageId,
                        "OUT-" + messageId,
                        "request-hash-" + messageId,
                        1,
                        Map.of(
                                "order_id", "ORDER-" + messageId,
                                "out_order_sn", "OUT-" + messageId,
                                "persist_status", "PENDING_MQ")))));
    }

    private Message persistentMessage(String payload) {
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        return new Message(payload.getBytes(StandardCharsets.UTF_8), messageProperties);
    }

    private RabbitPersistRechargeOrderMessageListener.RetryPublisher retryPublisher() {
        return (body, nextRetryCount) -> {
            MessageProperties messageProperties = new MessageProperties();
            messageProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            messageProperties.setHeader(RabbitPersistRechargeOrderMessageListener.RETRY_COUNT_HEADER, nextRetryCount);
            CorrelationData correlationData = new CorrelationData("retry-it-" + UUID.randomUUID());
            rabbitTemplate.send(
                    properties.getMq().getPersistRetryExchange(),
                    properties.getMq().getPersistRetryRoutingKey(),
                    new Message(body, messageProperties),
                    correlationData);
            return correlationData.getFuture()
                    .orTimeout(5, TimeUnit.SECONDS)
                    .thenApply(confirm -> {
                        if (confirm == null || !confirm.isAck()) {
                            String reason = confirm == null ? "missing confirm" : confirm.getReason();
                            throw new IllegalStateException("retry publish confirm failed: " + reason);
                        }
                        return null;
                    });
        };
    }

    private int maxRetry() {
        return properties.getMq().getConsumerMaxRetry();
    }

    private Message receiveFrom(String queue, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        Message message;
        do {
            message = rabbitTemplate.receive(queue);
            if (message != null) {
                return message;
            }
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        return null;
    }

    private GetResponse waitForBasicGet(
            Channel channel,
            String queue,
            boolean autoAck,
            Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        GetResponse response;
        do {
            response = channel.basicGet(queue, autoAck);
            if (response != null) {
                return response;
            }
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        return null;
    }

    private int queueMessageCount(String queue) {
        return rabbitAdmin.getQueueInfo(queue).getMessageCount();
    }

    private boolean waitUntil(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        do {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        return condition.getAsBoolean();
    }

    private BrokerChannel brokerChannel() throws Exception {
        com.rabbitmq.client.Connection connection = rawConnectionFactory().newConnection();
        Channel channel = connection.createChannel();
        return new BrokerChannel(connection, channel);
    }

    private CachingConnectionFactory connectionFactory() {
        CachingConnectionFactory factory = new CachingConnectionFactory(
                env("TRADE_ENGINE_RABBITMQ_HOST", "127.0.0.1"),
                Integer.parseInt(env("TRADE_ENGINE_RABBITMQ_PORT", "5672")));
        factory.setUsername(env("TRADE_ENGINE_RABBITMQ_USERNAME", "guest"));
        factory.setPassword(env("TRADE_ENGINE_RABBITMQ_PASSWORD", "guest"));
        factory.setVirtualHost(env("TRADE_ENGINE_RABBITMQ_VHOST", "/"));
        factory.setPublisherConfirmType(ConfirmType.CORRELATED);
        factory.getRabbitConnectionFactory().setConnectionTimeout(
                Integer.parseInt(env("TRADE_ENGINE_RABBITMQ_CONNECTION_TIMEOUT_MS", "3000")));
        return factory;
    }

    private com.rabbitmq.client.ConnectionFactory rawConnectionFactory() {
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setHost(env("TRADE_ENGINE_RABBITMQ_HOST", "127.0.0.1"));
        factory.setPort(Integer.parseInt(env("TRADE_ENGINE_RABBITMQ_PORT", "5672")));
        factory.setUsername(env("TRADE_ENGINE_RABBITMQ_USERNAME", "guest"));
        factory.setPassword(env("TRADE_ENGINE_RABBITMQ_PASSWORD", "guest"));
        factory.setVirtualHost(env("TRADE_ENGINE_RABBITMQ_VHOST", "/"));
        factory.setConnectionTimeout(Integer.parseInt(env("TRADE_ENGINE_RABBITMQ_CONNECTION_TIMEOUT_MS", "3000")));
        return factory;
    }

    private TradeEngineProperties integrationProperties(String namespace) {
        TradeEngineProperties result = new TradeEngineProperties();
        result.getMq().setPersistExchange(namespace + ".exchange");
        result.getMq().setPersistRoutingKey(namespace + ".persist");
        result.getMq().setPersistQueue(namespace + ".queue");
        result.getMq().setPersistRetryExchange(namespace + ".retry.exchange");
        result.getMq().setPersistRetryRoutingKey(namespace + ".retry");
        result.getMq().setPersistRetryQueue(namespace + ".retry.queue");
        result.getMq().setConsumerRetryDelayMs(200);
        result.getMq().setPersistDeadLetterExchange(namespace + ".dlx");
        result.getMq().setPersistDeadLetterQueue(namespace + ".dlq");
        result.getMq().setPersistDeadLetterRoutingKey(namespace + ".dlq");
        return result;
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }

    private record BrokerChannel(com.rabbitmq.client.Connection connection, Channel channel) implements AutoCloseable {

        @Override
        public void close() throws Exception {
            channel.close();
            connection.close();
        }
    }
}
