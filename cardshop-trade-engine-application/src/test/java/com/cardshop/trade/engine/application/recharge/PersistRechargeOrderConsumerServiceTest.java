package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/**
 * Redis/MQ 到 MySQL 异步落库 Consumer 核心编排测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
 * D7A 先验证不绑定 RabbitMQ listener 的核心服务：快照选择、事务写库端口调用和 Redis mark 调用顺序。
 */
class PersistRechargeOrderConsumerServiceTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-07T01:30:00Z"), ZoneOffset.UTC);

    @Test
    void persistsMqInitialSnapshotAndMarksRedisAfterMysqlCommit() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED);
        assertThat(result.shouldAck()).isTrue();
        assertThat(persistencePort.lastAggregate.persistSource()).isEqualTo(PersistSource.MQ_SNAPSHOT_FALLBACK);
        assertThat(persistencePort.lastAggregate.snapshot().productName()).isEqualTo("交行话费100元");
        assertThat(reliabilityPort.markDbPersistedCalls).isEqualTo(1);
        // 对应详细设计「8.3 异步落库 Consumer」：Redis 快照缺失时，MQ initialOrderSnapshot 的版本就是已落库版本。
        assertThat(reliabilityPort.lastPersistedVersion).isEqualTo(1L);
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
        assertThat(persistencePort.events).containsExactly("persist");
        assertThat(reliabilityPort.events).containsExactly("markDbPersisted");
    }

    @Test
    void prefersRedisSnapshotWhenRedisHasNewerOrderVersion() {
        PersistOrderSnapshot redisSnapshot = snapshot(2L, "Redis更新商品名", true);
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.of(redisSnapshot));
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED);
        assertThat(persistencePort.lastAggregate.persistSource()).isEqualTo(PersistSource.REDIS_SNAPSHOT);
        assertThat(persistencePort.lastAggregate.snapshot().orderVersion()).isEqualTo(2L);
        assertThat(persistencePort.lastAggregate.snapshot().productName()).isEqualTo("Redis更新商品名");
        // 对应详细设计「8.3 异步落库 Consumer」：选择 Redis 更新版本快照后，markDbPersisted 必须推进到该快照版本。
        assertThat(reliabilityPort.lastPersistedVersion).isEqualTo(2L);
    }

    @Test
    void fallsBackToMqInitialSnapshotWhenRedisSnapshotReadFails() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        snapshotPort.failure = new IllegalStateException("redis snapshot malformed");
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED);
        assertThat(result.shouldAck()).isTrue();
        assertThat(persistencePort.lastAggregate.persistSource()).isEqualTo(PersistSource.MQ_SNAPSHOT_FALLBACK);
        assertThat(persistencePort.lastAggregate.snapshot().productName()).isEqualTo("交行话费100元");
        assertThat(reliabilityPort.markDbPersistedCalls).isEqualTo(1);
        // Redis 快照读取异常会降级使用 MQ 初始快照，因此 persistedVersion 仍应等于 MQ 消息中的 orderVersion。
        assertThat(reliabilityPort.lastPersistedVersion).isEqualTo(1L);
    }

    @Test
    void marksPersistFailedAndKeepsMessageRetryableWhenMysqlWriteFails() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        persistencePort.failure = new IllegalStateException("db unavailable");
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSIST_FAILED);
        assertThat(result.shouldAck()).isFalse();
        assertThat(result.failCode()).isEqualTo("MYSQL_PERSIST_FAILED");
        assertThat(reliabilityPort.markDbPersistedCalls).isZero();
        assertThat(reliabilityPort.markPersistFailedCalls).isEqualTo(1);
        assertThat(reliabilityPort.lastPersistFailedReason).contains("db unavailable");
    }

    @Test
    void persistsMysqlOnlyConfigChangedSnapshotWithoutRedisMark() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(mysqlOnlyConfigChangedMessage());

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED);
        assertThat(result.shouldAck()).isTrue();
        assertThat(persistencePort.lastAggregate.snapshot().validationStatus()).isEqualTo("FAILED");
        assertThat(persistencePort.lastAggregate.snapshot().failCode()).isEqualTo("MERCHANT_CONFIG_STALE");
        assertThat(persistencePort.lastAggregate.snapshot().failReason()).isEqualTo("商户配置缺失或版本不一致");
        assertThat(reliabilityPort.markDbPersistedCalls).isZero();
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
        assertThat(persistencePort.events).containsExactly("persist");
        assertThat(reliabilityPort.events).isEmpty();
    }

    @Test
    void skipsPersistFailedRedisMarkForMysqlOnlyConfigChangedSnapshotWhenMysqlWriteFails() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        persistencePort.failure = new IllegalStateException("db unavailable");
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(mysqlOnlyConfigChangedMessage());

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSIST_FAILED);
        assertThat(result.shouldAck()).isFalse();
        assertThat(reliabilityPort.markDbPersistedCalls).isZero();
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
    }

    @Test
    void returnsAckedRedisMarkFailureWhenMarkDbPersistedTimesOutAfterMysqlCommit() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        reliabilityPort.markDbPersistedFuture = new CompletableFuture<>();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK,
                Duration.ofMillis(10));

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED_REDIS_MARK_FAILED);
        assertThat(result.shouldAck()).isTrue();
        assertThat(result.failCode()).isEqualTo(PersistRechargeOrderResult.FAIL_CODE_REDIS_MARK_DB_PERSISTED_FAILED);
        assertThat(result.failReason()).contains("timeout");
        assertThat(reliabilityPort.markDbPersistedCalls).isEqualTo(1);
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
        assertThat(reliabilityPort.markDbPersistedFuture.toCompletableFuture()).isCancelled();
    }

    @Test
    void cancelsMarkPersistFailedFutureWhenRedisMarkTimesOutAfterMysqlFailure() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        persistencePort.failure = new IllegalStateException("db unavailable");
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        reliabilityPort.markPersistFailedFuture = new CompletableFuture<>();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK,
                Duration.ofMillis(10));

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSIST_FAILED);
        assertThat(result.shouldAck()).isFalse();
        assertThat(reliabilityPort.markPersistFailedCalls).isEqualTo(1);
        assertThat(reliabilityPort.markPersistFailedFuture.toCompletableFuture()).isCancelled();
    }

    @Test
    void persistsSnapshotWithBlankNotifyUrlSoCallbackCanStayDisabled() {
        PersistOrderSnapshot redisSnapshot = snapshot(2L, "Redis更新商品名", true, "");
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.of(redisSnapshot));
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED);
        assertThat(persistencePort.lastAggregate.snapshot().notifyUrl()).isEmpty();
        assertThat(reliabilityPort.markDbPersistedCalls).isEqualTo(1);
    }

    @Test
    void truncatesMysqlExceptionReasonBeforeWritingPersistFailedState() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        persistencePort.failure = new IllegalStateException("x".repeat(1_500));
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSIST_FAILED);
        assertThat(result.failReason()).endsWith("...[truncated]");
        assertThat(result.failReason()).hasSizeLessThanOrEqualTo(520);
        assertThat(reliabilityPort.lastPersistFailedReason).isEqualTo(result.failReason());
    }

    private static PersistTradeOrderMessage message(long orderVersion) {
        return new PersistTradeOrderMessage(
                "persist-ORDER-1001-01",
                "A36",
                "ORDER-1001",
                "OUT-1001",
                "request-hash-1001",
                orderVersion,
                Map.ofEntries(
                        Map.entry("order_id", "ORDER-1001"),
                        Map.entry("org_code", "A36"),
                        Map.entry("app_id", "app-gray"),
                        Map.entry("out_order_sn", "OUT-1001"),
                        Map.entry("product_id", "PARENT-100"),
                        Map.entry("selected_product_id", "SKU-100"),
                        Map.entry("account", "13800138000"),
                        Map.entry("notify_url", "https://merchant.example/callback"),
                        Map.entry("num", 1),
                        Map.entry("order_price", 5200L),
                        Map.entry("product_name", "交行话费100元"),
                        Map.entry("product_value", 10000L),
                        Map.entry("product_type", 1),
                        Map.entry("coupon_type", "1"),
                        Map.entry("supplier_channel", "BOCOM"),
                        Map.entry("request_hash", "request-hash-1001"),
                        Map.entry("request_hash_version", "v1"),
                        Map.entry("request_hash_alg", "SHA-256"),
                        Map.entry("order_status", "ACCEPTED"),
                        Map.entry("validation_status", "PASSED"),
                        Map.entry("fail_code", ""),
                        Map.entry("fail_reason", ""),
                        Map.entry("dispatch_allowed", true),
                        Map.entry("balance_reserved", true),
                        Map.entry("persist_status", "PERSIST_QUEUED"),
                        Map.entry("order_version", orderVersion),
                        Map.entry("persisted_version", 0L),
                        Map.entry("mq_publish_count", 1)));
    }

    private static PersistTradeOrderMessage mysqlOnlyConfigChangedMessage() {
        return new PersistTradeOrderMessage(
                "persist-ORDER-1001-01",
                "A36",
                "ORDER-1001",
                "OUT-1001",
                "request-hash-1001",
                1L,
                Map.ofEntries(
                        Map.entry("order_id", "ORDER-1001"),
                        Map.entry("org_code", "A36"),
                        Map.entry("app_id", "app-gray"),
                        Map.entry("out_order_sn", "OUT-1001"),
                        Map.entry("product_id", "PARENT-100"),
                        Map.entry("selected_product_id", "SKU-100"),
                        Map.entry("account", "13800138000"),
                        Map.entry("notify_url", "https://merchant.example/callback"),
                        Map.entry("num", 1),
                        Map.entry("order_price", 5200L),
                        Map.entry("product_name", "交行话费100元"),
                        Map.entry("product_value", 10000L),
                        Map.entry("product_type", 1),
                        Map.entry("coupon_type", "1"),
                        Map.entry("supplier_channel", "BOCOM"),
                        Map.entry("request_hash", "request-hash-1001"),
                        Map.entry("request_hash_version", "v1"),
                        Map.entry("request_hash_alg", "SHA-256"),
                        Map.entry("order_status", "VALIDATION_FAILED"),
                        Map.entry("validation_status", "FAILED"),
                        Map.entry("fail_code", "MERCHANT_CONFIG_STALE"),
                        Map.entry("fail_reason", "商户配置缺失或版本不一致"),
                        Map.entry("dispatch_allowed", false),
                        Map.entry("balance_reserved", false),
                        Map.entry("persist_status", "PENDING_MQ"),
                        Map.entry("order_version", 1L),
                        Map.entry("persisted_version", 0L),
                        Map.entry("mq_publish_count", 1),
                        Map.entry("redis_order_written", false)));
    }

    private static PersistOrderSnapshot snapshot(long orderVersion, String productName, boolean dispatchAllowed) {
        return snapshot(orderVersion, productName, dispatchAllowed, "https://merchant.example/callback");
    }

    private static PersistOrderSnapshot snapshot(
            long orderVersion,
            String productName,
            boolean dispatchAllowed,
            String notifyUrl) {
        return new PersistOrderSnapshot(
                "A36",
                "app-gray",
                "ORDER-1001",
                "OUT-1001",
                "PARENT-100",
                "SKU-100",
                "13800138000",
                notifyUrl,
                1,
                5200L,
                productName,
                10000L,
                1,
                "1",
                "BOCOM",
                "request-hash-1001",
                "v1",
                "SHA-256",
                "ACCEPTED",
                "PASSED",
                "",
                "",
                dispatchAllowed,
                dispatchAllowed,
                "PERSIST_QUEUED",
                orderVersion,
                0L,
                1);
    }

    private static final class RecordingSnapshotPort implements PersistOrderSnapshotPort {

        private final Optional<PersistOrderSnapshot> snapshot;
        private RuntimeException failure;

        private RecordingSnapshotPort(Optional<PersistOrderSnapshot> snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public Optional<PersistOrderSnapshot> findLatestSnapshot(String orgCode, String outOrderSn, String orderId) {
            if (failure != null) {
                throw failure;
            }
            return snapshot;
        }
    }

    private static final class RecordingPersistencePort implements RechargeOrderPersistencePort {

        private final List<String> events = new ArrayList<>();
        private OrderPersistAggregate lastAggregate;
        private RuntimeException failure;

        @Override
        public void persist(OrderPersistAggregate aggregate) {
            events.add("persist");
            lastAggregate = aggregate;
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class RecordingReliabilityPort implements RechargeAcceptReliabilityPort {

        private final List<String> events = new ArrayList<>();
        private int markDbPersistedCalls;
        private int markPersistFailedCalls;
        private long lastPersistedVersion = -1L;
        private String lastPersistFailedReason;
        private CompletionStage<MarkPersistStateResult> markDbPersistedFuture =
                CompletableFuture.completedFuture(MarkPersistStateResult.success());
        private CompletionStage<MarkPersistStateResult> markPersistFailedFuture =
                CompletableFuture.completedFuture(MarkPersistStateResult.success());

        @Override
        public CompletionStage<ReplicationWaitResult> waitForReplication(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult) {
            throw new UnsupportedOperationException("D7A consumer does not call WAIT");
        }

        @Override
        public CompletionStage<CancelUnconfirmedAcceptResult> cancelUnconfirmedAccept(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String reason) {
            throw new UnsupportedOperationException("D7A consumer does not cancel accept");
        }

        @Override
        public CompletionStage<MarkPersistQueuedResult> markPersistQueued(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String persistMessageId,
                int publishCount) {
            throw new UnsupportedOperationException("D7A consumer does not mark queued");
        }

        @Override
        public CompletionStage<MarkPersistStateResult> markDbPersisted(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                long persistedVersion,
                long nowEpochMillis) {
            events.add("markDbPersisted");
            markDbPersistedCalls++;
            lastPersistedVersion = persistedVersion;
            return markDbPersistedFuture;
        }

        @Override
        public CompletionStage<MarkPersistStateResult> markPersistFailed(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String failCode,
                String failReason,
                int retryCount,
                long nextRetryEpochMillis,
                long nowEpochMillis) {
            events.add("markPersistFailed");
            markPersistFailedCalls++;
            lastPersistFailedReason = failReason;
            return markPersistFailedFuture;
        }
    }
}
