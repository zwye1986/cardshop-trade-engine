package com.cardshop.trade.engine.infrastructure.mq;

import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderBatchResult;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * RabbitMQ 持久化消息批 listener 适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。本类只负责 RabbitMQ
 * 批协议边界：反序列化批消息、调用 application 层批 Consumer、按批结果执行 manual ack/retry/DLQ。
 * MySQL 批事务、Redis 快照选择、Redis mark 和幂等仍保持在 application service 和 persistence adapter 中。
 */
public class RabbitPersistRechargeOrderBatchMessageListener {

    private static final Logger log = LoggerFactory.getLogger(RabbitPersistRechargeOrderBatchMessageListener.class);
    private static final String RESULT_SUCCESS = "success";
    private static final String RESULT_FAILED = "failed";

    private final ObjectMapper objectMapper;
    private final Function<List<PersistTradeOrderMessage>, PersistRechargeOrderBatchResult> consumer;
    private final RabbitPersistRechargeOrderMessageListener.RetryPublisher retryPublisher;
    private final int maxRetry;
    private final TradeEngineObservabilityMetrics observabilityMetrics;

    /**
     * 装配批 listener。
     *
     * <p>失败重试沿用单条 listener 的 retry exchange/queue：先把仍可重试的消息发布到 retry queue，
     * 并在 publish confirm 成功后再 ack 对应原消息；重试次数耗尽的消息单独 reject 进 DLQ。
     * 若中途 publish 失败，已成功进 retry 的原消息仍 ack，未发布成功的原消息再 requeue，避免
     * “retry 副本 + 主队列 requeue” 双份并存。
     */
    public RabbitPersistRechargeOrderBatchMessageListener(
            ObjectMapper objectMapper,
            PersistRechargeOrderConsumerService consumerService,
            RabbitTemplate rabbitTemplate,
            String retryExchange,
            String retryRoutingKey,
            int maxRetry) {
        this(
                objectMapper,
                consumerService::persistBatch,
                RabbitPersistRechargeOrderMessageListener.rabbitRetryPublisher(
                        rabbitTemplate,
                        retryExchange,
                        retryRoutingKey),
                maxRetry,
                TradeEngineObservabilityMetrics.noop());
    }

    public RabbitPersistRechargeOrderBatchMessageListener(
            ObjectMapper objectMapper,
            PersistRechargeOrderConsumerService consumerService,
            RabbitTemplate rabbitTemplate,
            String retryExchange,
            String retryRoutingKey,
            int maxRetry,
            TradeEngineObservabilityMetrics observabilityMetrics) {
        this(
                objectMapper,
                consumerService::persistBatch,
                RabbitPersistRechargeOrderMessageListener.rabbitRetryPublisher(
                        rabbitTemplate,
                        retryExchange,
                        retryRoutingKey),
                maxRetry,
                observabilityMetrics);
    }

    RabbitPersistRechargeOrderBatchMessageListener(
            ObjectMapper objectMapper,
            Function<List<PersistTradeOrderMessage>, PersistRechargeOrderBatchResult> consumer,
            RabbitPersistRechargeOrderMessageListener.RetryPublisher retryPublisher,
            int maxRetry) {
        this(objectMapper, consumer, retryPublisher, maxRetry, TradeEngineObservabilityMetrics.noop());
    }

    RabbitPersistRechargeOrderBatchMessageListener(
            ObjectMapper objectMapper,
            Function<List<PersistTradeOrderMessage>, PersistRechargeOrderBatchResult> consumer,
            RabbitPersistRechargeOrderMessageListener.RetryPublisher retryPublisher,
            int maxRetry,
            TradeEngineObservabilityMetrics observabilityMetrics) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
        this.retryPublisher = Objects.requireNonNull(retryPublisher, "retryPublisher must not be null");
        if (maxRetry < 1) {
            throw new IllegalArgumentException("maxRetry must be >= 1");
        }
        this.maxRetry = maxRetry;
        this.observabilityMetrics = observabilityMetrics == null
                ? TradeEngineObservabilityMetrics.noop()
                : observabilityMetrics;
    }

    /**
     * 消费 Redis 订单异步落库批消息。
     *
     * <p>阶段 1 MySQL 批事务仍整批原子：解析失败、批事务失败或 application 层返回不可 ack，都进入
     * retry/DLQ 分流。DLQ 与 retry 按单条 retryCount 判定，避免毒消息拖死同批新鲜消息；retry 发布
     * 采用“已成功进 retry 的原消息 ack、未成功的原消息 requeue”，避免重复投递。
     */
    @RabbitListener(queues = "${trade.engine.mq.persist-queue:recharge.persist.mysql.queue}", ackMode = "MANUAL")
    public void onMessage(List<Message> messages, Channel channel) throws IOException {
        if (messages == null || messages.isEmpty()) {
            /*
             * 正常容器不应投递空批，且空 List 没有 deliveryTag 无从 ack。
             * 这里只打防御性 warn，不作为协议错误处理。
             */
            log.warn("RabbitMQ批量持久化收到空批，忽略处理");
            return;
        }
        observabilityMetrics.recordPersistBatchSize(messages.size());
        long lastDeliveryTag = lastDeliveryTag(messages);
        List<PersistTradeOrderMessage> persistMessages = new ArrayList<>(messages.size());
        for (int index = 0; index < messages.size(); index++) {
            Message message = messages.get(index);
            try {
                persistMessages.add(objectMapper.readValue(message.getBody(), PersistTradeOrderMessage.class));
            } catch (Exception ex) {
                byte[] body = message.getBody() == null ? new byte[0] : message.getBody();
                log.error(
                        "RabbitMQ批量持久化消息反序列化失败 batchSize={}, failedIndex={}, failedDeliveryTag={}, "
                                + "bodyLength={}, bodySha256={}, firstDeliveryTag={}, lastDeliveryTag={}",
                        messages.size(),
                        index,
                        message.getMessageProperties().getDeliveryTag(),
                        body.length,
                        sha256Hex(body),
                        firstDeliveryTag(messages),
                        lastDeliveryTag,
                        ex);
                observabilityMetrics.recordPersistBatchOrders(messages.size(), RESULT_FAILED);
                retryOrDeadLetterBatch(channel, messages, "batch deserialize failed: " + ex.getMessage());
                return;
            }
        }

        try {
            PersistRechargeOrderBatchResult result = consumer.apply(List.copyOf(persistMessages));
            if (result.shouldAck()) {
                observabilityMetrics.recordPersistBatchOrders(messages.size(), RESULT_SUCCESS);
                channel.basicAck(lastDeliveryTag, true);
                return;
            }
            log.error("RabbitMQ批量持久化消息处理需要重试 batchSize={}, firstMessageId={}, lastMessageId={}, failCode={}, failReason={}",
                    messages.size(),
                    persistMessages.getFirst().messageId(),
                    persistMessages.getLast().messageId(),
                    result.failCode(),
                    result.failReason());
            observabilityMetrics.recordPersistBatchOrders(messages.size(), RESULT_FAILED);
            retryOrDeadLetterBatch(channel, messages, result.failReason());
        } catch (RuntimeException ex) {
            log.error("RabbitMQ批量持久化消息处理异常 batchSize={}, firstMessageId={}, lastMessageId={}",
                    messages.size(),
                    persistMessages.getFirst().messageId(),
                    persistMessages.getLast().messageId(),
                    ex);
            observabilityMetrics.recordPersistBatchOrders(messages.size(), RESULT_FAILED);
            retryOrDeadLetterBatch(channel, messages, ex.getMessage());
        }
    }

    private void retryOrDeadLetterBatch(
            Channel channel,
            List<Message> messages,
            String failReason) throws IOException {
        List<Message> deadLetterMessages = new ArrayList<>();
        List<Message> retryableMessages = new ArrayList<>();
        for (Message message : messages) {
            if (RabbitPersistRechargeOrderMessageListener.currentRetryCount(message) >= maxRetry) {
                deadLetterMessages.add(message);
            } else {
                retryableMessages.add(message);
            }
        }

        if (!deadLetterMessages.isEmpty()) {
            log.error("RabbitMQ批量持久化消息部分转入DLQ deadLetterCount={}, retryableCount={}, maxRetry={}, failReason={}",
                    deadLetterMessages.size(),
                    retryableMessages.size(),
                    maxRetry,
                    failReason);
            for (Message message : deadLetterMessages) {
                channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
            }
        }
        if (retryableMessages.isEmpty()) {
            return;
        }

        List<Message> publishedToRetry = new ArrayList<>();
        try {
            for (Message message : retryableMessages) {
                int nextRetryCount = RabbitPersistRechargeOrderMessageListener.currentRetryCount(message) + 1;
                retryPublisher.publish(message.getBody(), nextRetryCount).join();
                publishedToRetry.add(message);
            }
            ackMessages(channel, publishedToRetry);
        } catch (RuntimeException ex) {
            log.error("RabbitMQ批量持久化消息投递retry queue部分失败 batchSize={}, publishedCount={}, failReason={}",
                    retryableMessages.size(),
                    publishedToRetry.size(),
                    failReason,
                    ex);
            /*
             * 已 confirm 进入 retry queue 的原消息必须 ack，否则 nack+requeue 会与 retry 副本并存。
             * 尚未发布成功的原消息再 requeue，由 broker 重新投递。
             * 参见 docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」。
             */
            ackMessages(channel, publishedToRetry);
            for (Message message : retryableMessages) {
                if (!publishedToRetry.contains(message)) {
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
                }
            }
        }
    }

    private static void ackMessages(Channel channel, List<Message> messages) throws IOException {
        for (Message message : messages) {
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        }
    }

    private static long firstDeliveryTag(List<Message> messages) {
        return messages.getFirst().getMessageProperties().getDeliveryTag();
    }

    private static long lastDeliveryTag(List<Message> messages) {
        return messages.getLast().getMessageProperties().getDeliveryTag();
    }

    static String sha256Hex(byte[] body) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(body == null ? new byte[0] : body));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is required for batch poison-message logging", ex);
        }
    }
}
