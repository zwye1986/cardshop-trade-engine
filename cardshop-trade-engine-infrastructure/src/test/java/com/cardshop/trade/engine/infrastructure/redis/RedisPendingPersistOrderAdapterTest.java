package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.PendingPersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.PersistedOrderStatePort;
import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Redis pending MQ 补投适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.11 XXL-Job Handler 契约」。这里不启动真实
 * Redis，而是用内存操作桩验证 key 协议、快照重建和状态回写，真实 Redis WAIT/HA 行为留给集成测试覆盖。
 */
class RedisPendingPersistOrderAdapterTest {

    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(20_000), ZoneOffset.UTC);

    @Test
    void selectsDuePendingOrderFromOrgIndexAndOrderHash() {
        RechargeRedisKeyBuilder keyBuilder = new RechargeRedisKeyBuilder("test", 8);
        FakePendingRedisOperations operations = new FakePendingRedisOperations();
        RedisPendingPersistOrderAdapter adapter = new RedisPendingPersistOrderAdapter(
                operations,
                keyBuilder,
                8,
                Duration.ofSeconds(30),
                Duration.ofSeconds(120),
                PersistedOrderStatePort.noop(),
                CLOCK);

        seedPendingOrder(operations, keyBuilder, "A05", "ORDER-1", "OUT-1", "persist-1", 10_000);

        List<PendingPersistOrderSnapshot> snapshots = adapter.selectDueForShard(1, 0, 10, "worker-A");

        assertThat(snapshots).hasSize(1);
        PendingPersistOrderSnapshot snapshot = snapshots.getFirst();
        assertThat(snapshot.orgCode()).isEqualTo("A05");
        assertThat(snapshot.orderId()).isEqualTo("ORDER-1");
        assertThat(snapshot.outOrderSn()).isEqualTo("OUT-1");
        assertThat(snapshot.messageId()).isEqualTo("persist-1");
        assertThat(snapshot.requestHash()).isEqualTo("hash-1");
        assertThat(snapshot.initialOrderSnapshot())
                .containsEntry("selected_product_id", "363")
                .containsEntry("persist_status", "PENDING_MQ");
        assertThat(operations.string(keyBuilder.persistClaimKey("A05", "ORDER-1"))).startsWith("worker-A:");
    }

    @Test
    void skipsPendingOrderWhenAnotherWorkerAlreadyHoldsClaim() {
        RechargeRedisKeyBuilder keyBuilder = new RechargeRedisKeyBuilder("test", 8);
        FakePendingRedisOperations operations = new FakePendingRedisOperations();
        RedisPendingPersistOrderAdapter adapter = new RedisPendingPersistOrderAdapter(
                operations,
                keyBuilder,
                8,
                Duration.ofSeconds(30),
                Duration.ofSeconds(120),
                PersistedOrderStatePort.noop(),
                CLOCK);
        seedPendingOrder(operations, keyBuilder, "A05", "ORDER-1", "OUT-1", "persist-1", 10_000);
        operations.setIfAbsent(
                keyBuilder.persistClaimKey("A05", "ORDER-1"),
                "worker-other:ORDER-1",
                Duration.ofSeconds(120));

        List<PendingPersistOrderSnapshot> snapshots = adapter.selectDueForShard(1, 0, 10, "worker-A");

        assertThat(snapshots).isEmpty();
        assertThat(operations.string(keyBuilder.persistClaimKey("A05", "ORDER-1"))).isEqualTo("worker-other:ORDER-1");
    }

    @Test
    void repairsRedisAndSkipsRepublishWhenMysqlAlreadyPersisted() {
        RechargeRedisKeyBuilder keyBuilder = new RechargeRedisKeyBuilder("test", 8);
        FakePendingRedisOperations operations = new FakePendingRedisOperations();
        PersistedOrderStatePort persistedStatePort = (orgCode, outOrderSn, orderId, orderVersion) -> true;
        RedisPendingPersistOrderAdapter adapter = new RedisPendingPersistOrderAdapter(
                operations,
                keyBuilder,
                8,
                Duration.ofSeconds(30),
                Duration.ofSeconds(120),
                persistedStatePort,
                CLOCK);
        seedPendingOrder(operations, keyBuilder, "A05", "ORDER-1", "OUT-1", "persist-1", 10_000);

        List<PendingPersistOrderSnapshot> snapshots = adapter.selectDueForShard(1, 0, 10, "worker-A");

        assertThat(snapshots).isEmpty();
        assertThat(operations.hash(keyBuilder.orderKey("A05", "OUT-1")))
                .containsEntry("persist_status", "DB_PERSISTED")
                .containsEntry("persisted_version", "1");
        assertThat(operations.hash(keyBuilder.idempotentKey("A05", "OUT-1")))
                .containsEntry("persist_status", "DB_PERSISTED");
        assertThat(operations.zset(keyBuilder.pendingKey("A05"))).doesNotContainKey("ORDER-1");
        assertThat(operations.zset(keyBuilder.pendingOrgIndexKey(keyBuilder.pendingOrgBucket("A05"))))
                .doesNotContainKey("A05");
    }

    @Test
    void markRepublishedDefersPendingAndIncrementsPublishCount() {
        RechargeRedisKeyBuilder keyBuilder = new RechargeRedisKeyBuilder("test", 8);
        FakePendingRedisOperations operations = new FakePendingRedisOperations();
        RedisPendingPersistOrderAdapter adapter = new RedisPendingPersistOrderAdapter(
                operations,
                keyBuilder,
                8,
                Duration.ofSeconds(30),
                Duration.ofSeconds(120),
                PersistedOrderStatePort.noop(),
                CLOCK);
        seedPendingOrder(operations, keyBuilder, "A05", "ORDER-1", "OUT-1", "", 10_000);
        operations.hash(keyBuilder.orderKey("A05", "OUT-1")).put("mq_publish_count", "3");
        PendingPersistOrderSnapshot snapshot = adapter.selectDueForShard(1, 0, 10, "worker-A").getFirst();

        adapter.markRepublished(snapshot, PersistMessagePublishResult.confirmed("persist-repub-1", 2), "worker-A");

        assertThat(operations.hash(keyBuilder.orderKey("A05", "OUT-1")))
                .containsEntry("persist_status", "PERSIST_QUEUED")
                .containsEntry("persist_msg_id", "persist-repub-1")
                .containsEntry("mq_publish_count", "4")
                .containsEntry("last_republish_worker", "worker-A");
        assertThat(operations.zset(keyBuilder.pendingKey("A05"))).containsEntry("ORDER-1", 50_000L);
        assertThat(operations.zset(keyBuilder.pendingOrgIndexKey(keyBuilder.pendingOrgBucket("A05"))))
                .containsEntry("A05", 50_000L);
        assertThat(operations.hash(keyBuilder.idempotentKey("A05", "OUT-1")))
                .containsEntry("persist_status", "PERSIST_QUEUED")
                .containsEntry("persist_msg_id", "persist-repub-1");
    }

    @Test
    void markRepublishedDoesNotOverwriteDbPersistedStateWhenConsumerWinsRace() {
        RechargeRedisKeyBuilder keyBuilder = new RechargeRedisKeyBuilder("test", 8);
        FakePendingRedisOperations operations = new FakePendingRedisOperations();
        RedisPendingPersistOrderAdapter adapter = new RedisPendingPersistOrderAdapter(
                operations,
                keyBuilder,
                8,
                Duration.ofSeconds(30),
                Duration.ofSeconds(120),
                PersistedOrderStatePort.noop(),
                CLOCK);
        seedPendingOrder(operations, keyBuilder, "A05", "ORDER-1", "OUT-1", "", 10_000);
        PendingPersistOrderSnapshot snapshot = adapter.selectDueForShard(1, 0, 10, "worker-A").getFirst();
        operations.hash(keyBuilder.orderKey("A05", "OUT-1")).put("persist_status", "DB_PERSISTED");
        operations.hash(keyBuilder.orderKey("A05", "OUT-1")).put("persisted_version", "1");
        operations.hash(keyBuilder.idempotentKey("A05", "OUT-1")).put("persist_status", "DB_PERSISTED");

        adapter.markRepublished(snapshot, PersistMessagePublishResult.confirmed("persist-repub-1", 2), "worker-A");

        assertThat(operations.hash(keyBuilder.orderKey("A05", "OUT-1")))
                .containsEntry("persist_status", "DB_PERSISTED")
                .containsEntry("persisted_version", "1")
                .containsEntry("last_republish_worker", "worker-A")
                .doesNotContainEntry("persist_status", "PERSIST_QUEUED");
        assertThat(operations.hash(keyBuilder.idempotentKey("A05", "OUT-1")))
                .containsEntry("persist_status", "DB_PERSISTED");
        assertThat(operations.zset(keyBuilder.pendingKey("A05"))).doesNotContainKey("ORDER-1");
        assertThat(operations.zset(keyBuilder.pendingOrgIndexKey(keyBuilder.pendingOrgBucket("A05"))))
                .doesNotContainKey("A05");
    }

    @Test
    void markRepublishFailedDefersNextRetryInPendingAndOrgIndex() {
        RechargeRedisKeyBuilder keyBuilder = new RechargeRedisKeyBuilder("test", 8);
        FakePendingRedisOperations operations = new FakePendingRedisOperations();
        RedisPendingPersistOrderAdapter adapter = new RedisPendingPersistOrderAdapter(
                operations,
                keyBuilder,
                8,
                Duration.ofSeconds(30),
                Duration.ofSeconds(120),
                PersistedOrderStatePort.noop(),
                CLOCK);
        seedPendingOrder(operations, keyBuilder, "A05", "ORDER-1", "OUT-1", "", 10_000);
        PendingPersistOrderSnapshot snapshot = adapter.selectDueForShard(1, 0, 10, "worker-A").getFirst();

        adapter.markRepublishFailed(snapshot, "broker unavailable", "worker-A");

        assertThat(operations.hash(keyBuilder.orderKey("A05", "OUT-1")))
                .containsEntry("persist_status", "PERSIST_FAILED")
                .containsEntry("last_error_code", "MQ_REPUBLISH_FAILED")
                .containsEntry("last_error", "broker unavailable")
                .containsEntry("next_retry_time", "50000");
        assertThat(operations.zset(keyBuilder.pendingKey("A05"))).containsEntry("ORDER-1", 50_000L);
        assertThat(operations.zset(keyBuilder.pendingOrgIndexKey(keyBuilder.pendingOrgBucket("A05"))))
                .containsEntry("A05", 50_000L);
    }

    @Test
    void rejectsShardTotalGreaterThanPendingOrgBucketCount() {
        RechargeRedisKeyBuilder keyBuilder = new RechargeRedisKeyBuilder("test", 2);
        FakePendingRedisOperations operations = new FakePendingRedisOperations();
        RedisPendingPersistOrderAdapter adapter = new RedisPendingPersistOrderAdapter(
                operations,
                keyBuilder,
                2,
                Duration.ofSeconds(30),
                Duration.ofSeconds(120),
                PersistedOrderStatePort.noop(),
                CLOCK);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> adapter.selectDueForShard(4, 0, 10, "worker-A"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pendingOrgBucketCount");
    }

    @Test
    void reactiveRedisRangeByScorePushesLimitDownToRedisCommand() throws Exception {
        String adapter = Files.readString(Path.of(
                "src/main/java/com/cardshop/trade/engine/infrastructure/redis/RedisPendingPersistOrderAdapter.java"));

        assertThat(adapter)
                .contains("Limit.limit().count(Math.max(1, limit))")
                .doesNotContain(".rangeByScore(key, Range.closed((double) minScore, (double) maxScore))\n                    .take(");
    }

    private static void seedPendingOrder(
            FakePendingRedisOperations operations,
            RechargeRedisKeyBuilder keyBuilder,
            String orgCode,
            String orderId,
            String outOrderSn,
            String persistMessageId,
            long dueTime) {
        operations.addToZSet(
                keyBuilder.pendingOrgIndexKey(keyBuilder.pendingOrgBucket(orgCode)),
                dueTime,
                orgCode);
        operations.addToZSet(keyBuilder.pendingKey(orgCode), dueTime, orderId);
        operations.putAll(keyBuilder.reservationKey(orgCode, orderId), Map.of(
                "order_id", orderId,
                "out_order_sn", outOrderSn,
                "request_hash", "hash-1"));

        Map<String, String> order = new LinkedHashMap<>();
        order.put("order_id", orderId);
        order.put("org_code", orgCode);
        order.put("app_id", "app-A05");
        order.put("out_order_sn", outOrderSn);
        order.put("product_id", "362");
        order.put("selected_product_id", "363");
        order.put("account", "13800000000");
        order.put("notify_url", "https://notify.example.test/callback");
        order.put("num", "1");
        order.put("order_price", "1890");
        order.put("product_name", "话费直充");
        order.put("product_value", "2000");
        order.put("product_type", "1");
        order.put("coupon_type", "");
        order.put("supplier_channel", "fake");
        order.put("request_hash", "hash-1");
        order.put("request_hash_version", "v1");
        order.put("request_hash_alg", "SHA-256");
        order.put("order_status", "ACCEPTED");
        order.put("validation_status", "PASSED");
        order.put("fail_code", "");
        order.put("fail_reason", "");
        order.put("dispatch_allowed", "1");
        order.put("balance_reserved", "1");
        order.put("persist_status", "PENDING_MQ");
        order.put("persist_msg_id", persistMessageId);
        order.put("order_version", "1");
        operations.putAll(keyBuilder.orderKey(orgCode, outOrderSn), order);
        operations.putAll(keyBuilder.idempotentKey(orgCode, outOrderSn), Map.of(
                "request_hash", "hash-1",
                "order_id", orderId,
                "persist_status", "PENDING_MQ"));
    }

    private static final class FakePendingRedisOperations
            implements RedisPendingPersistOrderAdapter.PendingRedisOperations {

        private final Map<String, Map<String, String>> hashes = new LinkedHashMap<>();
        private final Map<String, Map<String, Long>> zsets = new LinkedHashMap<>();
        private final Map<String, String> strings = new LinkedHashMap<>();

        @Override
        public List<String> rangeByScore(String key, long minScore, long maxScore, int limit) {
            return zset(key).entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .filter(entry -> entry.getValue() >= minScore && entry.getValue() <= maxScore)
                    .limit(limit)
                    .map(Map.Entry::getKey)
                    .toList();
        }

        @Override
        public Map<String, String> entries(String key) {
            return Map.copyOf(hash(key));
        }

        @Override
        public void putAll(String key, Map<String, String> fields) {
            hash(key).putAll(fields);
        }

        @Override
        public void addToZSet(String key, long score, String value) {
            zset(key).put(value, score);
        }

        @Override
        public boolean setIfAbsent(String key, String value, Duration ttl) {
            if (strings.containsKey(key)) {
                return false;
            }
            strings.put(key, value);
            return true;
        }

        @Override
        public void removeFromZSet(String key, String value) {
            zset(key).remove(value);
        }

        @Override
        public Long score(String key, String value) {
            return zset(key).get(value);
        }

        Map<String, String> hash(String key) {
            return hashes.computeIfAbsent(key, ignored -> new LinkedHashMap<>());
        }

        Map<String, Long> zset(String key) {
            return zsets.computeIfAbsent(key, ignored -> new LinkedHashMap<>());
        }

        String string(String key) {
            return strings.get(key);
        }
    }
}
