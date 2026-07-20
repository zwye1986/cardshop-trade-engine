package com.cardshop.trade.engine.infrastructure.mq;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderBatchResult;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

/**
 * RabbitMQ 批量持久化消息 listener 适配层测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。批 listener
 * 只负责批反序列化和整批 manual ack/retry/DLQ，快照选择、批 MySQL 事务和 Redis mark 仍留在 application 层。
 */
@ExtendWith(OutputCaptureExtension.class)
class RabbitPersistRechargeOrderBatchMessageListenerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void acksWholeBatchWhenConsumerAllowsAck() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        var listener = listener(ignored -> PersistRechargeOrderBatchResult.persisted(2, 2, 0));

        listener.onMessage(List.of(message(validPayload("1"), 101L), message(validPayload("2"), 102L)), channel.proxy());

        assertThat(channel.ackTags).containsExactly(102L);
        assertThat(channel.ackMultiples).containsExactly(true);
        assertThat(channel.nackTags).isEmpty();
        assertThat(channel.rejectTags).isEmpty();
    }

    @Test
    void republishesEveryMessageAndAcksOriginalBatchWhenConsumerRequiresRetry() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        List<Integer> retryCounts = new ArrayList<>();
        var listener = listener(
                ignored -> PersistRechargeOrderBatchResult.persistFailed(2, "MYSQL_PERSIST_FAILED", "db unavailable"),
                (body, next) -> {
                    retryCounts.add(next);
                    return CompletableFuture.completedFuture(null);
                },
                3);

        listener.onMessage(List.of(message(validPayload("1"), 201L), message(validPayload("2"), 202L)), channel.proxy());

        assertThat(retryCounts).containsExactly(1, 1);
        assertThat(channel.ackTags).containsExactly(201L, 202L);
        assertThat(channel.ackMultiples).containsExactly(false, false);
        assertThat(channel.nackTags).isEmpty();
        assertThat(channel.rejectTags).isEmpty();
    }

    @Test
    void acksPublishedMessagesAndRequeuesRemainderWhenRetryPublishFailsMidBatch() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        AtomicInteger publishAttempts = new AtomicInteger();
        var listener = listener(
                ignored -> PersistRechargeOrderBatchResult.persistFailed(2, "MYSQL_PERSIST_FAILED", "db unavailable"),
                (body, next) -> {
                    if (publishAttempts.incrementAndGet() == 1) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return CompletableFuture.failedFuture(new IllegalStateException("retry broker unavailable"));
                },
                3);

        listener.onMessage(List.of(message(validPayload("1"), 301L), message(validPayload("2"), 302L)), channel.proxy());

        assertThat(channel.ackTags).containsExactly(301L);
        assertThat(channel.ackMultiples).containsExactly(false);
        assertThat(channel.nackTags).containsExactly(302L);
        assertThat(channel.nackMultiples).containsExactly(false);
        assertThat(channel.nackRequeues).containsExactly(true);
        assertThat(channel.rejectTags).isEmpty();
    }

    @Test
    void rejectsEveryMessageToDeadLetterWhenRetryCountExhausted() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        List<Integer> retryCounts = new ArrayList<>();
        var listener = listener(
                ignored -> PersistRechargeOrderBatchResult.persistFailed(2, "MYSQL_PERSIST_FAILED", "db unavailable"),
                (body, next) -> {
                    retryCounts.add(next);
                    return CompletableFuture.completedFuture(null);
                },
                3);

        listener.onMessage(List.of(
                messageWithRetry(validPayload("1"), 401L, 3),
                messageWithRetry(validPayload("2"), 402L, 3)), channel.proxy());

        assertThat(channel.rejectTags).containsExactly(401L, 402L);
        assertThat(channel.rejectRequeues).containsExactly(false, false);
        assertThat(retryCounts).isEmpty();
        assertThat(channel.ackTags).isEmpty();
        assertThat(channel.nackTags).isEmpty();
    }

    @Test
    void rejectsOnlyExhaustedMessagesAndRetriesFreshOnesInSameBatch() throws Exception {
        RecordingChannel channel = new RecordingChannel();
        List<Integer> retryCounts = new ArrayList<>();
        var listener = listener(
                ignored -> PersistRechargeOrderBatchResult.persistFailed(2, "MYSQL_PERSIST_FAILED", "db unavailable"),
                (body, next) -> {
                    retryCounts.add(next);
                    return CompletableFuture.completedFuture(null);
                },
                3);

        listener.onMessage(List.of(
                messageWithRetry(validPayload("1"), 411L, 3),
                message(validPayload("2"), 412L)), channel.proxy());

        assertThat(channel.rejectTags).containsExactly(411L);
        assertThat(retryCounts).containsExactly(1);
        assertThat(channel.ackTags).containsExactly(412L);
        assertThat(channel.nackTags).isEmpty();
    }

    @Test
    void malformedPayloadRetriesTheWholeBatchBeforeConsumerIsCalled(CapturedOutput output) throws Exception {
        RecordingChannel channel = new RecordingChannel();
        List<String> republishedBodies = new ArrayList<>();
        var listener = listener(
                ignored -> {
                    throw new AssertionError("consumer should not be called when batch contains malformed JSON");
                },
                (body, next) -> {
                    republishedBodies.add(new String(body, StandardCharsets.UTF_8));
                    return CompletableFuture.completedFuture(null);
                },
                3);
        String badPayload = "{bad-json";

        listener.onMessage(List.of(message(badPayload, 501L), message(validPayload("2"), 502L)), channel.proxy());

        assertThat(republishedBodies).containsExactly(badPayload, validPayload("2"));
        assertThat(channel.ackTags).containsExactly(501L, 502L);
        assertThat(channel.rejectTags).isEmpty();
        assertThat(output.getOut() + output.getErr())
                .contains("failedIndex=0")
                .contains("failedDeliveryTag=501")
                .contains("bodyLength=" + badPayload.getBytes(StandardCharsets.UTF_8).length)
                .contains("bodySha256="
                        + RabbitPersistRechargeOrderBatchMessageListener.sha256Hex(
                                badPayload.getBytes(StandardCharsets.UTF_8)))
                .doesNotContain(validPayload("2"));
    }

    @Test
    void emptyBatchIsIgnoredWithoutAck(CapturedOutput output) throws Exception {
        RecordingChannel channel = new RecordingChannel();
        var listener = listener(ignored -> {
            throw new AssertionError("consumer should not be called for empty batch");
        });

        listener.onMessage(List.of(), channel.proxy());

        assertThat(channel.ackTags).isEmpty();
        assertThat(channel.nackTags).isEmpty();
        assertThat(channel.rejectTags).isEmpty();
        assertThat(output.getOut() + output.getErr()).contains("收到空批");
    }

    private RabbitPersistRechargeOrderBatchMessageListener listener(
            Function<List<PersistTradeOrderMessage>, PersistRechargeOrderBatchResult> consumer) {
        return listener(consumer, (body, next) -> CompletableFuture.completedFuture(null), 3);
    }

    private RabbitPersistRechargeOrderBatchMessageListener listener(
            Function<List<PersistTradeOrderMessage>, PersistRechargeOrderBatchResult> consumer,
            RabbitPersistRechargeOrderMessageListener.RetryPublisher retryPublisher,
            int maxRetry) {
        return new RabbitPersistRechargeOrderBatchMessageListener(objectMapper, consumer, retryPublisher, maxRetry);
    }

    private String validPayload(String suffix) throws Exception {
        return objectMapper.writeValueAsString(new PersistTradeOrderMessage(
                "persist-msg-" + suffix,
                "A36",
                "ORDER-100" + suffix,
                "OUT-100" + suffix,
                "request-hash-" + suffix,
                1,
                Map.of(
                        "order_id", "ORDER-100" + suffix,
                        "out_order_sn", "OUT-100" + suffix,
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

        private final List<Long> ackTags = new ArrayList<>();
        private final List<Boolean> ackMultiples = new ArrayList<>();
        private final List<Long> nackTags = new ArrayList<>();
        private final List<Boolean> nackMultiples = new ArrayList<>();
        private final List<Boolean> nackRequeues = new ArrayList<>();
        private final List<Long> rejectTags = new ArrayList<>();
        private final List<Boolean> rejectRequeues = new ArrayList<>();

        private Channel proxy() {
            return (Channel) Proxy.newProxyInstance(
                    Channel.class.getClassLoader(),
                    new Class<?>[]{Channel.class},
                    (ignored, method, args) -> switch (method.getName()) {
                        case "basicAck" -> {
                            ackTags.add((Long) args[0]);
                            ackMultiples.add((Boolean) args[1]);
                            yield null;
                        }
                        case "basicNack" -> {
                            nackTags.add((Long) args[0]);
                            nackMultiples.add((Boolean) args[1]);
                            nackRequeues.add((Boolean) args[2]);
                            yield null;
                        }
                        case "basicReject" -> {
                            rejectTags.add((Long) args[0]);
                            rejectRequeues.add((Boolean) args[1]);
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
