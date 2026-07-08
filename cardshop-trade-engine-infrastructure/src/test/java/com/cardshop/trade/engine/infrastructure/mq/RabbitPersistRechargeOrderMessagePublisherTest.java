package com.cardshop.trade.engine.infrastructure.mq;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;

/**
 * RabbitMQ 持久化消息发布器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.9 RabbitMQ 声明契约」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「14.3 Redis 成功但 MQ confirm 失败协议」。
 * 下单链路不能把 `convertAndSend` 调用成功误认为 broker 已确认，因此测试直接驱动 CorrelationData future。
 */
class RabbitPersistRechargeOrderMessagePublisherTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-06T05:00:00Z"), ZoneOffset.UTC);

    @Test
    void returnsConfirmedOnlyAfterRabbitConfirmAck() {
        var operations = new RecordingRabbitPersistMessageOperations(true);
        var publisher = publisher(operations, properties(1, 500));

        PersistMessagePublishResult result = publisher.publish(message()).toCompletableFuture().join();

        assertThat(result.confirmed()).isTrue();
        assertThat(result.messageId()).isEqualTo("persist-msg-1");
        assertThat(result.attemptCount()).isEqualTo(1);
        assertThat(operations.calls).isEqualTo(1);
        assertThat(operations.exchange).isEqualTo("recharge.persist.exchange");
        assertThat(operations.routingKey).isEqualTo("recharge.persist.mysql");
        assertThat(operations.payload).contains("\"messageId\":\"persist-msg-1\"");
    }

    @Test
    void retriesAndReturnsFailedWhenRabbitConfirmNackPersists() {
        var operations = new RecordingRabbitPersistMessageOperations(false);
        var publisher = publisher(operations, properties(2, 500));

        PersistMessagePublishResult result = publisher.publish(message()).toCompletableFuture().join();

        assertThat(result.confirmed()).isFalse();
        assertThat(result.messageId()).isEqualTo("persist-msg-1");
        assertThat(result.attemptCount()).isEqualTo(2);
        assertThat(result.failCode()).isEqualTo("CONFIRM_NACK");
        assertThat(operations.calls).isEqualTo(2);
    }

    @Test
    void handlesConfirmFailureRetryOnMqConfirmExecutor() {
        var operations = new RecordingRabbitPersistMessageOperations(false);
        var mqConfirmExecutor = new QueuedNamedExecutor("trade-mq-confirm-test");
        var publisher = publisher(operations, properties(2, 500), mqConfirmExecutor);

        var resultFuture = publisher.publish(message()).toCompletableFuture();

        assertThat(resultFuture).isNotDone();
        assertThat(operations.calls).isEqualTo(1);
        assertThat(operations.publishThreads).containsExactly(Thread.currentThread().getName());
        assertThat(mqConfirmExecutor.queuedTaskCount()).isEqualTo(1);

        mqConfirmExecutor.runNext();
        assertThat(resultFuture).isNotDone();
        assertThat(operations.calls).isEqualTo(2);
        assertThat(operations.publishThreads.get(1)).isEqualTo("trade-mq-confirm-test");

        mqConfirmExecutor.runNext();
        PersistMessagePublishResult result = resultFuture.join();

        assertThat(result.confirmed()).isFalse();
        assertThat(result.attemptCount()).isEqualTo(2);
        assertThat(result.failCode()).isEqualTo("CONFIRM_NACK");
    }

    @Test
    void treatsMandatoryReturnAsPublishFailureEvenWhenConfirmAckArrives() {
        var operations = new RecordingRabbitPersistMessageOperations(true);
        operations.returned = true;
        var publisher = publisher(operations, properties(3, 500));

        PersistMessagePublishResult result = publisher.publish(message()).toCompletableFuture().join();

        assertThat(result.confirmed()).isFalse();
        assertThat(result.failCode()).isEqualTo("MANDATORY_RETURN");
        assertThat(result.failReason()).contains("NO_ROUTE");
        assertThat(result.attemptCount()).isEqualTo(1);
        assertThat(operations.calls).isEqualTo(1);
    }

    @Test
    void opensBreakerAfterConfiguredFailures() {
        var operations = new RecordingRabbitPersistMessageOperations(false);
        var properties = properties(1, 500);
        properties.getMq().getBreaker().setFailureThreshold(1);
        properties.getMq().getBreaker().setOpenSeconds(30);
        var publisher = publisher(operations, properties);

        PersistMessagePublishResult result = publisher.publish(message()).toCompletableFuture().join();

        assertThat(result.confirmed()).isFalse();
        assertThat(publisher.isAvailable()).isFalse();
    }

    @Test
    void opensBreakerWhenBrokerIsUnavailableDuringPublish() {
        var operations = new RecordingRabbitPersistMessageOperations(true);
        operations.publishFailure = new IllegalStateException("broker unreachable");
        var properties = properties(1, 500);
        properties.getMq().getBreaker().setFailureThreshold(1);
        properties.getMq().getBreaker().setOpenSeconds(30);
        var publisher = publisher(operations, properties);

        PersistMessagePublishResult result = publisher.publish(message()).toCompletableFuture().join();

        assertThat(result.confirmed()).isFalse();
        assertThat(result.failCode()).isEqualTo("PUBLISH_EXCEPTION");
        assertThat(result.failReason()).contains("broker unreachable");
        assertThat(publisher.isAvailable()).isFalse();
    }

    private static RabbitPersistRechargeOrderMessagePublisher publisher(
            RabbitPersistMessageOperations operations,
            TradeEngineProperties properties) {
        return publisher(operations, properties, Runnable::run);
    }

    private static RabbitPersistRechargeOrderMessagePublisher publisher(
            RabbitPersistMessageOperations operations,
            TradeEngineProperties properties,
            Executor mqConfirmExecutor) {
        return new RabbitPersistRechargeOrderMessagePublisher(
                operations,
                new ObjectMapper(),
                properties,
                CLOCK,
                mqConfirmExecutor);
    }

    private static TradeEngineProperties properties(int retryTimes, int confirmTimeoutMs) {
        var properties = new TradeEngineProperties();
        properties.getMq().setPublishRetryTimes(retryTimes);
        properties.getMq().setPublishConfirmTimeoutMs(confirmTimeoutMs);
        return properties;
    }

    private static PersistTradeOrderMessage message() {
        return new PersistTradeOrderMessage(
                "persist-msg-1",
                "A36",
                "202607051337458961474292",
                "OUT-1001",
                "hash-v1",
                1,
                Map.of("order_id", "202607051337458961474292", "persist_status", "PENDING_MQ"));
    }

    private static final class RecordingRabbitPersistMessageOperations implements RabbitPersistMessageOperations {

        private final boolean ack;
        private int calls;
        private String exchange;
        private String routingKey;
        private String payload;
        private final List<String> publishThreads = new ArrayList<>();
        private boolean returned;
        private RuntimeException publishFailure;

        private RecordingRabbitPersistMessageOperations(boolean ack) {
            this.ack = ack;
        }

        @Override
        public void convertAndSend(
                String exchange,
                String routingKey,
                String payload,
                CorrelationData correlationData) {
            this.calls++;
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.payload = payload;
            this.publishThreads.add(Thread.currentThread().getName());
            if (publishFailure != null) {
                throw publishFailure;
            }
            if (returned) {
                correlationData.setReturned(new ReturnedMessage(
                        new Message(payload.getBytes(), new MessageProperties()),
                        312,
                        "NO_ROUTE",
                        exchange,
                        routingKey));
            }
            correlationData.getFuture().complete(new CorrelationData.Confirm(ack, ack ? null : "nack-for-test"));
        }
    }

    private static final class QueuedNamedExecutor implements Executor {

        private final String threadName;
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        private QueuedNamedExecutor(String threadName) {
            this.threadName = threadName;
        }

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        private int queuedTaskCount() {
            return tasks.size();
        }

        private void runNext() {
            Runnable task = tasks.remove();
            String originalName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(threadName);
                task.run();
            } finally {
                Thread.currentThread().setName(originalName);
            }
        }
    }
}
