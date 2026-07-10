package com.cardshop.trade.engine.infrastructure.mq;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.PersistSource;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * RabbitMQ 持久化消息 listener 适配层测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
 * listener 只负责协议反序列化和 manual ack/nack；快照选择、MySQL 事务和 Redis mark 语义必须留在
 * {@code PersistRechargeOrderConsumerService} 中，避免 RabbitMQ 适配层承载业务分支。
 */
class RabbitPersistRechargeOrderMessageListenerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void acksMessageWhenPersistConsumerAllowsAck() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        var listener = listener(ignored -> PersistRechargeOrderResult.persisted(PersistSource.REDIS_SNAPSHOT));

        listener.onMessage(message(validPayload(), 101L), channel.proxy());

        assertThat(channel.ackTag).isEqualTo(101L);
        assertThat(channel.nackTag).isNull();
        assertThat(channel.rejectTag).isNull();
    }

    @Test
    void republishesAndAcksOriginalWhenPersistConsumerRequiresRetry() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        AtomicInteger republishedRetryCount = new AtomicInteger(-1);
        var listener = listener(
                ignored -> PersistRechargeOrderResult.persistFailed(
                        PersistSource.MQ_SNAPSHOT_FALLBACK,
                        "MYSQL_PERSIST_FAILED",
                        "db unavailable"),
                (body, next) -> {
                    republishedRetryCount.set(next);
                    return CompletableFuture.completedFuture(null);
                },
                3);

        listener.onMessage(message(validPayload(), 102L), channel.proxy());

        assertThat(channel.ackTag).isEqualTo(102L);
        assertThat(republishedRetryCount.get()).isEqualTo(1);
        assertThat(channel.nackTag).isNull();
        assertThat(channel.rejectTag).isNull();
    }

    @Test
    void rejectsToDeadLetterWhenRetryCountExhausted() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        AtomicInteger republishedRetryCount = new AtomicInteger(-1);
        var listener = listener(
                ignored -> PersistRechargeOrderResult.persistFailed(
                        PersistSource.MQ_SNAPSHOT_FALLBACK,
                        "MYSQL_PERSIST_FAILED",
                        "db unavailable"),
                (body, next) -> {
                    republishedRetryCount.set(next);
                    return CompletableFuture.completedFuture(null);
                },
                3);

        listener.onMessage(messageWithRetry(validPayload(), 104L, 3), channel.proxy());

        assertThat(channel.rejectTag).isEqualTo(104L);
        assertThat(channel.rejectRequeue).isFalse();
        assertThat(channel.ackTag).isNull();
        assertThat(channel.nackTag).isNull();
        assertThat(republishedRetryCount.get()).isEqualTo(-1);
    }

    @Test
    void nacksOriginalWithRequeueWhenRetryPublishFails() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        var listener = listener(
                ignored -> PersistRechargeOrderResult.persistFailed(
                        PersistSource.MQ_SNAPSHOT_FALLBACK,
                        "MYSQL_PERSIST_FAILED",
                        "db unavailable"),
                (body, next) -> CompletableFuture.failedFuture(new IllegalStateException("retry broker unavailable")),
                3);

        listener.onMessage(message(validPayload(), 105L), channel.proxy());

        assertThat(channel.nackTag).isEqualTo(105L);
        assertThat(channel.nackRequeue).isTrue();
        assertThat(channel.ackTag).isNull();
        assertThat(channel.rejectTag).isNull();
    }

    @Test
    void rejectsMalformedPayloadWithoutRequeue() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        var listener = listener(ignored -> {
            throw new AssertionError("consumer should not be called for malformed JSON");
        });

        listener.onMessage(message("{bad-json", 103L), channel.proxy());

        assertThat(channel.ackTag).isNull();
        assertThat(channel.nackTag).isNull();
        assertThat(channel.rejectTag).isEqualTo(103L);
        assertThat(channel.rejectRequeue).isFalse();
    }

    private RabbitPersistRechargeOrderMessageListener listener(
            Function<PersistTradeOrderMessage, PersistRechargeOrderResult> consumer) {
        return listener(consumer, (body, next) -> CompletableFuture.completedFuture(null), 3);
    }

    private RabbitPersistRechargeOrderMessageListener listener(
            Function<PersistTradeOrderMessage, PersistRechargeOrderResult> consumer,
            RabbitPersistRechargeOrderMessageListener.RetryPublisher retryPublisher,
            int maxRetry) {
        return new RabbitPersistRechargeOrderMessageListener(objectMapper, consumer, retryPublisher, maxRetry);
    }

    private String validPayload() throws Exception {
        return objectMapper.writeValueAsString(new PersistTradeOrderMessage(
                "persist-msg-1",
                "A36",
                "ORDER-1001",
                "OUT-1001",
                "request-hash",
                1,
                Map.of(
                        "order_id", "ORDER-1001",
                        "out_order_sn", "OUT-1001",
                        "persist_status", "PENDING_MQ")));
    }

    private static Message message(String payload, long deliveryTag) {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(deliveryTag);
        return new Message(payload.getBytes(StandardCharsets.UTF_8), properties);
    }

    private static Message messageWithRetry(String payload, long deliveryTag, int retryCount) {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(deliveryTag);
        properties.setHeader(RabbitPersistRechargeOrderMessageListener.RETRY_COUNT_HEADER, retryCount);
        return new Message(payload.getBytes(StandardCharsets.UTF_8), properties);
    }

    private static final class RecordingChannel {

        private Long ackTag;
        private Long nackTag;
        private Boolean nackRequeue;
        private Long rejectTag;
        private Boolean rejectRequeue;

        private Channel proxy() {
            return (Channel) Proxy.newProxyInstance(
                    Channel.class.getClassLoader(),
                    new Class<?>[]{Channel.class},
                    (ignored, method, args) -> switch (method.getName()) {
                        case "basicAck" -> {
                            ackTag = (Long) args[0];
                            yield null;
                        }
                        case "basicNack" -> {
                            nackTag = (Long) args[0];
                            nackRequeue = (Boolean) args[2];
                            yield null;
                        }
                        case "basicReject" -> {
                            rejectTag = (Long) args[0];
                            rejectRequeue = (Boolean) args[1];
                            yield null;
                        }
                        case "isOpen" -> true;
                        default -> defaultValue(method.getReturnType());
                    });
        }

        private static Object defaultValue(Class<?> returnType) {
            if (!returnType.isPrimitive()) {
                return null;
            }
            if (returnType == boolean.class) {
                return false;
            }
            if (returnType == int.class) {
                return 0;
            }
            if (returnType == long.class) {
                return 0L;
            }
            return null;
        }
    }
}
