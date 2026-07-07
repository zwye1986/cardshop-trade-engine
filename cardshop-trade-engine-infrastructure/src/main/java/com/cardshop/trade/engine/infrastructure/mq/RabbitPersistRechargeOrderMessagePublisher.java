package com.cardshop.trade.engine.infrastructure.mq;

import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;

/**
 * RabbitMQ Redis 订单异步落库消息发布器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.9 RabbitMQ 声明契约」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「14.3 Redis 成功但 MQ confirm 失败协议」。
 * `RabbitTemplate.convertAndSend` 只代表消息交给客户端发送流程，接口返回前必须等待 publisher confirm ack；
 * 若 confirm 持续失败，{@link #isAvailable()} 会打开熔断，让下单链路在 Redis 写入前 fail closed。
 */
public class RabbitPersistRechargeOrderMessagePublisher implements PersistRechargeOrderMessagePublisher {

    private static final Logger log = LoggerFactory.getLogger(RabbitPersistRechargeOrderMessagePublisher.class);

    private final RabbitPersistMessageOperations operations;
    private final ObjectMapper objectMapper;
    private final TradeEngineProperties properties;
    private final Clock clock;
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private volatile Instant breakerOpenUntil = Instant.EPOCH;

    public RabbitPersistRechargeOrderMessagePublisher(
            RabbitPersistMessageOperations operations,
            ObjectMapper objectMapper,
            TradeEngineProperties properties,
            Clock clock) {
        this.operations = operations;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public boolean isAvailable() {
        if (!properties.getMq().getBreaker().isEnabled()) {
            return true;
        }
        return !clock.instant().isBefore(breakerOpenUntil);
    }

    @Override
    public CompletionStage<PersistMessagePublishResult> publish(PersistTradeOrderMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        if (!isAvailable()) {
            return CompletableFuture.completedFuture(PersistMessagePublishResult.failed(
                    message.messageId(),
                    0,
                    "MQ_BREAKER_OPEN",
                    "RabbitMQ 发布熔断打开"));
        }
        String payload;
        try {
            payload = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException ex) {
            log.error("Redis订单持久化消息序列化失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}",
                    message.orgCode(), message.outOrderSn(), message.orderId(), message.messageId(), ex);
            return CompletableFuture.completedFuture(PersistMessagePublishResult.failed(
                    message.messageId(),
                    0,
                    "MESSAGE_SERIALIZE_FAILED",
                    "持久化消息序列化失败"));
        }
        return publishAttempt(message, payload, 1);
    }

    private CompletionStage<PersistMessagePublishResult> publishAttempt(
            PersistTradeOrderMessage message,
            String payload,
            int attempt) {
        var correlationData = new CorrelationData(message.messageId() + "-" + attempt);
        try {
            operations.convertAndSend(
                    properties.getMq().getPersistExchange(),
                    properties.getMq().getPersistRoutingKey(),
                    payload,
                    correlationData);
        } catch (RuntimeException ex) {
            log.error("Redis订单持久化消息发送异常 orgCode={}, outOrderSn={}, orderId={}, messageId={}, attempt={}",
                    message.orgCode(), message.outOrderSn(), message.orderId(), message.messageId(), attempt, ex);
            return retryOrFail(message, payload, attempt, "PUBLISH_EXCEPTION", ex.getMessage());
        }

        long timeoutMs = Math.max(1L, properties.getMq().getPublishConfirmTimeoutMs());
        return correlationData.getFuture()
                .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                .handle((confirm, ex) -> {
                    if (ex == null && confirm != null && confirm.isAck()) {
                        ReturnedMessage returned = correlationData.getReturned();
                        if (returned != null) {
                            String failReason = "RabbitMQ mandatory return: replyCode=%d, replyText=%s, exchange=%s, routingKey=%s"
                                    .formatted(
                                            returned.getReplyCode(),
                                            defaultText(returned.getReplyText(), "NO_ROUTE"),
                                            returned.getExchange(),
                                            returned.getRoutingKey());
                            log.warn("Redis订单持久化消息mandatory return orgCode={}, outOrderSn={}, orderId={}, messageId={}, attempt={}, failReason={}",
                                    message.orgCode(),
                                    message.outOrderSn(),
                                    message.orderId(),
                                    message.messageId(),
                                    attempt,
                                    failReason);
                            return PersistMessagePublishResult.failed(
                                    message.messageId(),
                                    attempt,
                                    "MANDATORY_RETURN",
                                    failReason);
                        }
                        recordSuccess();
                        return PersistMessagePublishResult.confirmed(message.messageId(), attempt);
                    }
                    String failCode = ex == null ? "CONFIRM_NACK" : "CONFIRM_TIMEOUT";
                    String failReason = ex == null && confirm != null
                            ? defaultText(confirm.getReason(), "RabbitMQ confirm nack")
                            : ex == null ? "RabbitMQ confirm missing" : ex.getMessage();
                    log.warn("Redis订单持久化消息confirm失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}, attempt={}, failCode={}, failReason={}",
                            message.orgCode(),
                            message.outOrderSn(),
                            message.orderId(),
                            message.messageId(),
                            attempt,
                            failCode,
                            failReason);
                    return PersistMessagePublishResult.failed(message.messageId(), attempt, failCode, failReason);
                })
                .thenCompose(result -> handlePublishAttemptResult(message, payload, attempt, result));
    }

    private CompletionStage<PersistMessagePublishResult> handlePublishAttemptResult(
            PersistTradeOrderMessage message,
            String payload,
            int attempt,
            PersistMessagePublishResult result) {
        if (result.confirmed()) {
            return CompletableFuture.completedFuture(result);
        }
        if ("MANDATORY_RETURN".equals(result.failCode())) {
            return failWithoutRetryForMandatoryReturn(result);
        }
        return retryOrFail(message, payload, attempt, result.failCode(), result.failReason());
    }

    /**
     * mandatory return 说明 exchange 已接收发布动作，但 routing key 无法路由到持久化队列。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「11. RabbitMQ 高可用设计」和
     * 「14.3 Redis 成功但 MQ confirm 失败协议」。这类问题通常来自 exchange/queue 绑定或 routing key 配置错误，
     * 不是短暂网络抖动；继续重试只会拉长下单响应并制造重复 return 日志，因此直接失败并计入 breaker，
     * 让后续新订单在 Redis Lua 前 fail closed。
     */
    private CompletableFuture<PersistMessagePublishResult> failWithoutRetryForMandatoryReturn(
            PersistMessagePublishResult result) {
        recordFailure();
        return CompletableFuture.completedFuture(result);
    }

    private CompletionStage<PersistMessagePublishResult> retryOrFail(
            PersistTradeOrderMessage message,
            String payload,
            int attempt,
            String failCode,
            String failReason) {
        int maxAttempts = Math.max(1, properties.getMq().getPublishRetryTimes());
        if (attempt < maxAttempts) {
            return publishAttempt(message, payload, attempt + 1);
        }
        recordFailure();
        return CompletableFuture.completedFuture(PersistMessagePublishResult.failed(
                message.messageId(),
                attempt,
                failCode,
                defaultText(failReason, failCode)));
    }

    private void recordSuccess() {
        consecutiveFailures.set(0);
        breakerOpenUntil = Instant.EPOCH;
    }

    private void recordFailure() {
        var breaker = properties.getMq().getBreaker();
        if (!breaker.isEnabled()) {
            return;
        }
        int failures = consecutiveFailures.incrementAndGet();
        if (failures >= Math.max(1, breaker.getFailureThreshold())) {
            breakerOpenUntil = clock.instant().plusSeconds(Math.max(1, breaker.getOpenSeconds()));
            // TODO(D5): 实现 rollingWindowSeconds 与 halfOpenPermits 的完整滑动窗口/半开探测；参见 docs/交行直充业务系统高可用方案设计.md「14.3 Redis 成功但 MQ confirm 失败协议」。
            log.warn("RabbitMQ发布熔断打开 failures={}, openSeconds={}",
                    failures, breaker.getOpenSeconds());
        }
    }

    private static String defaultText(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
