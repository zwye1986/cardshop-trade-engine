package com.cardshop.trade.engine.infrastructure.mq;

import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * RabbitMQ 持久化消息 listener 适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。
 * 本类只负责 RabbitMQ 协议边界：反序列化 `PersistTradeOrderMessage`、调用 application 层 Consumer 核心服务、
 * 再根据 `PersistRechargeOrderResult.shouldAck` 执行 manual ack/nack。MySQL 事务、Redis 快照选择和
 * `markDbPersisted/markPersistFailed` 必须保持在 application service 中，避免 MQ 适配层复制业务状态机。
 */
public class RabbitPersistRechargeOrderMessageListener {

    private static final Logger log = LoggerFactory.getLogger(RabbitPersistRechargeOrderMessageListener.class);
    static final String RETRY_COUNT_HEADER = "x-retry-count";

    private final ObjectMapper objectMapper;
    private final Function<PersistTradeOrderMessage, PersistRechargeOrderResult> consumer;
    private final RetryPublisher retryPublisher;
    private final int maxRetry;

    /**
     * 装配 listener。
     *
     * <p>失败重试通过 retry exchange 投入 retry queue，等待 publisher confirm ack 后再 ack 原消息；
     * retry queue TTL 到期后由 broker DLX 投回主 exchange，避免立即热重投。
     */
    public RabbitPersistRechargeOrderMessageListener(
            ObjectMapper objectMapper,
            PersistRechargeOrderConsumerService consumerService,
            RabbitTemplate rabbitTemplate,
            String retryExchange,
            String retryRoutingKey,
            int maxRetry) {
        this(objectMapper, consumerService::persist, rabbitRetryPublisher(rabbitTemplate, retryExchange, retryRoutingKey), maxRetry);
    }

    RabbitPersistRechargeOrderMessageListener(
            ObjectMapper objectMapper,
            Function<PersistTradeOrderMessage, PersistRechargeOrderResult> consumer,
            RetryPublisher retryPublisher,
            int maxRetry) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
        this.retryPublisher = Objects.requireNonNull(retryPublisher, "retryPublisher must not be null");
        if (maxRetry < 1) {
            throw new IllegalArgumentException("maxRetry must be >= 1");
        }
        this.maxRetry = maxRetry;
    }

    @FunctionalInterface
    interface RetryPublisher {

        CompletableFuture<Void> publish(byte[] body, int nextRetryCount);
    }

    private static RetryPublisher rabbitRetryPublisher(RabbitTemplate rabbitTemplate, String exchange, String routingKey) {
        Objects.requireNonNull(rabbitTemplate, "rabbitTemplate must not be null");
        return (body, nextRetryCount) -> {
            MessageProperties properties = new MessageProperties();
            properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            properties.setHeader(RETRY_COUNT_HEADER, nextRetryCount);
            CorrelationData correlationData = new CorrelationData("persist-retry-" + UUID.randomUUID());
            rabbitTemplate.send(exchange, routingKey, new Message(body, properties), correlationData);
            return correlationData.getFuture()
                    .orTimeout(5, TimeUnit.SECONDS)
                    .thenApply(confirm -> {
                        if (confirm == null || !confirm.isAck()) {
                            String reason = confirm == null ? "missing confirm" : confirm.getReason();
                            throw new IllegalStateException("RabbitMQ retry publish confirm failed: " + reason);
                        }
                        if (correlationData.getReturned() != null) {
                            throw new IllegalStateException("RabbitMQ retry publish returned by broker");
                        }
                        return null;
                    });
        };
    }

    /**
     * 消费 Redis 订单异步落库消息。
     *
     * <p>反序列化失败代表消息体不是当前协议能理解的 `PersistTradeOrderMessage`，继续 requeue 只会形成毒消息循环，
     * 因此记录 error 后 `basicReject(requeue=false)`，交给 DLQ 排查。业务处理失败或异常时进入有界退避重试：
     * 未达 {@code maxRetry} 则投递到 retry queue 并等待 confirm 后 ack 原消息；retry queue TTL 到期后投回主队列。
     * 达到阈值后 `basicReject(requeue=false)` 进入最终 DLQ，由 `rechargePersistMysqlRetryJob` 或人工排查处理。
     */
    @RabbitListener(queues = "${trade.engine.mq.persist-queue:recharge.persist.mysql.queue}", ackMode = "MANUAL")
    public void onMessage(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        int retryCount = currentRetryCount(message);
        PersistTradeOrderMessage persistMessage;
        try {
            persistMessage = objectMapper.readValue(message.getBody(), PersistTradeOrderMessage.class);
        } catch (Exception ex) {
            // 反序列化失败代表消息体不是当前协议能理解的 PersistTradeOrderMessage：Jackson IOException、
            // null body 触发的 IllegalArgumentException 或其他解析异常都属于不可重试的毒消息，继续 requeue
            // 只会形成死循环，因此记录 error 后 basicReject(requeue=false) 交给 DLQ 排查。
            log.error("RabbitMQ持久化消息反序列化失败 deliveryTag={}, retryCount={}, payloadBytes={}",
                    deliveryTag,
                    retryCount,
                    message.getBody() == null ? 0 : message.getBody().length,
                    ex);
            channel.basicReject(deliveryTag, false);
            return;
        }

        try {
            PersistRechargeOrderResult result = consumer.apply(persistMessage);
            if (result.shouldAck()) {
                channel.basicAck(deliveryTag, false);
                return;
            }
            log.error("RabbitMQ持久化消息处理需要重试 orgCode={}, outOrderSn={}, orderId={}, messageId={}, retryCount={}, status={}, failCode={}, failReason={}",
                    persistMessage.orgCode(),
                    persistMessage.outOrderSn(),
                    persistMessage.orderId(),
                    persistMessage.messageId(),
                    retryCount,
                    result.status(),
                    result.failCode(),
                    result.failReason());
            retryOrDeadLetter(channel, deliveryTag, message, retryCount, persistMessage, result.failReason());
        } catch (RuntimeException ex) {
            log.error("RabbitMQ持久化消息处理异常 orgCode={}, outOrderSn={}, orderId={}, messageId={}, retryCount={}",
                    persistMessage.orgCode(),
                    persistMessage.outOrderSn(),
                    persistMessage.orderId(),
                    persistMessage.messageId(),
                    retryCount,
                    ex);
            retryOrDeadLetter(channel, deliveryTag, message, retryCount, persistMessage, ex.getMessage());
        }
    }

    /**
     * 业务失败或处理异常后的有界重试。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「12.5 故障模式与处理」。
     * {@code retryCount} 使用 {@value #RETRY_COUNT_HEADER} 累计：未达 {@code maxRetry} 时发布到 retry queue，
     * 等待 RabbitMQ confirm ack 后 ack 原消息；达到 {@code maxRetry} 后拒绝原消息并进入主队列 DLQ。
     */
    private void retryOrDeadLetter(
            Channel channel,
            long deliveryTag,
            Message message,
            int retryCount,
            PersistTradeOrderMessage persistMessage,
            String failReason) throws IOException {
        if (retryCount >= maxRetry) {
            log.error("RabbitMQ持久化消息重试次数耗尽转入DLQ orgCode={}, outOrderSn={}, orderId={}, messageId={}, retryCount={}, maxRetry={}",
                    persistMessage.orgCode(),
                    persistMessage.outOrderSn(),
                    persistMessage.orderId(),
                    persistMessage.messageId(),
                    retryCount,
                    maxRetry);
            channel.basicReject(deliveryTag, false);
            return;
        }
        int nextRetryCount = retryCount + 1;
        try {
            retryPublisher.publish(message.getBody(), nextRetryCount).join();
            channel.basicAck(deliveryTag, false);
        } catch (RuntimeException ex) {
            log.error("RabbitMQ持久化消息投递retry queue失败回退broker requeue orgCode={}, outOrderSn={}, orderId={}, messageId={}, nextRetryCount={}",
                    persistMessage.orgCode(),
                    persistMessage.outOrderSn(),
                    persistMessage.orderId(),
                    persistMessage.messageId(),
                    nextRetryCount,
                    ex);
            channel.basicNack(deliveryTag, false, true);
        }
    }

    private static int currentRetryCount(Message message) {
        Object raw = message.getMessageProperties().getHeader(RETRY_COUNT_HEADER);
        if (raw instanceof Number number) {
            return number.intValue();
        }
        return 0;
    }
}
