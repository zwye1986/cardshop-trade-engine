package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.v1.V1ProductCacheSettings;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.application.v1.V1ProductSelectionCacheEntry.CacheKind;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * V1 父商品选品 Redis 适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。测试固定完整字段、
 * 逻辑时间和刷新锁 fencing 合同，真实 Lua 原子性由 Testcontainers 用例补充证明。
 */
class RedisV1ProductSelectionCacheAdapterTest {

    private static final V1ProductCacheSettings SETTINGS = new V1ProductCacheSettings(
            Duration.ofMinutes(25), Duration.ofMinutes(30), Duration.ofMinutes(60),
            Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofMinutes(1));
    private static final RechargeRedisKeyBuilder KEY_BUILDER =
            new RechargeRedisKeyBuilder("cardshop:trade", 128);
    private static final Instant LOADED_AT = Instant.parse("2026-07-15T03:00:00Z");

    @Test
    void writesAndReadsCompleteSnapshotFields() {
        FakeOperations operations = new FakeOperations();
        RedisV1ProductSelectionCacheAdapter adapter = adapter(operations);
        operations.lockToken = "token-1";

        boolean written = adapter.writeSnapshotIfLockOwned(
                "A39", "80100022", "token-1", selection(), LOADED_AT);

        assertThat(written).isTrue();
        assertThat(operations.selectionKey)
                .isEqualTo("cardshop:trade:recharge:{A39}:product-selection:80100022");
        assertThat(operations.childKey)
                .isEqualTo("cardshop:trade:recharge:{A39}:product:80100023:config");
        assertThat(operations.snapshotFields).containsAllEntriesOf(Map.ofEntries(
                Map.entry("product_id", "80100022"),
                Map.entry("selected_product_id", "80100023"),
                Map.entry("supplier_channel", "MINISO"),
                Map.entry("product_name", "名创直充100"),
                Map.entry("coupon_type", "codekey"),
                Map.entry("product_type", "1"),
                Map.entry("product_status", "ENABLED"),
                Map.entry("sale_price", "10000"),
                Map.entry("face_value", "10000"),
                Map.entry("base_price", "9000"),
                Map.entry("product_version", "1"),
                Map.entry("selection_fingerprint", selection().cacheFingerprint()),
                Map.entry("validation_status", "PASSED"),
                Map.entry("cache_kind", "SNAPSHOT"),
                Map.entry("loaded_at", String.valueOf(LOADED_AT.toEpochMilli())),
                Map.entry("refresh_after", String.valueOf(LOADED_AT.plus(Duration.ofMinutes(25)).toEpochMilli())),
                Map.entry("expire_at", String.valueOf(LOADED_AT.plus(Duration.ofMinutes(30)).toEpochMilli()))));
        assertThat(operations.childFields).containsExactlyInAnyOrderEntriesOf(Map.of(
                "status", "ENABLED", "version", "1", "price", "10000", "product_type", "1"));
        assertThat(operations.snapshotTtl).isEqualTo(Duration.ofMinutes(60));

        operations.hashToRead = new LinkedHashMap<>(operations.snapshotFields);
        var entry = adapter.find("A39", "80100022").orElseThrow();
        assertThat(entry.cacheKind()).isEqualTo(CacheKind.SNAPSHOT);
        assertThat(entry.selection()).isEqualTo(selection());
        assertThat(entry.loadedAt()).isEqualTo(LOADED_AT);
    }

    @Test
    void rejectsSnapshotWriteWhenRefreshLockTokenIsNoLongerOwned() {
        FakeOperations operations = new FakeOperations();
        operations.completeSnapshotResult = false;
        RedisV1ProductSelectionCacheAdapter adapter = adapter(operations);

        assertThat(adapter.writeSnapshotIfLockOwned(
                "A39", "80100022", "expired-token", selection(), LOADED_AT)).isFalse();
    }

    @Test
    void writesAndReadsNegativeCacheWithOneMinuteTtl() {
        FakeOperations operations = new FakeOperations();
        RedisV1ProductSelectionCacheAdapter adapter = adapter(operations);
        V1ProductSelection denied = V1ProductSelection.failed(
                "80100022", "PRODUCT_NO_PERMISSION", "商品无权限", 1L);

        assertThat(adapter.writeNegativeIfLockOwned(
                "A39", "80100022", "token-1", denied, LOADED_AT)).isTrue();
        assertThat(operations.snapshotFields).containsEntry("cache_kind", "NEGATIVE");
        assertThat(operations.snapshotFields).containsEntry("fail_code", "PRODUCT_NO_PERMISSION");
        assertThat(operations.snapshotTtl).isEqualTo(Duration.ofMinutes(1));

        operations.hashToRead = new LinkedHashMap<>(operations.snapshotFields);
        var entry = adapter.find("A39", "80100022").orElseThrow();
        assertThat(entry.negative()).isTrue();
        assertThat(entry.selection().failReason()).isEqualTo("商品无权限");
    }

    @Test
    void malformedHashIsTreatedAsCacheMiss() {
        FakeOperations operations = new FakeOperations();
        operations.hashToRead = Map.of("cache_kind", "SNAPSHOT", "loaded_at", "not-a-number");

        assertThat(adapter(operations).find("A39", "80100022")).isEmpty();
    }

    @Test
    void legacyHashWithoutSelectionFingerprintIsTreatedAsCacheMiss() {
        FakeOperations operations = new FakeOperations();
        adapter(operations).writeSnapshotIfLockOwned(
                "A39", "80100022", "token-1", selection(), LOADED_AT);
        Map<String, String> legacyFields = new LinkedHashMap<>(operations.snapshotFields);
        legacyFields.remove("selection_fingerprint");
        operations.hashToRead = legacyFields;

        assertThat(adapter(operations).find("A39", "80100022")).isEmpty();
    }

    @Test
    void acquiresAndReleasesTokenizedRefreshLock() {
        FakeOperations operations = new FakeOperations();
        RedisV1ProductSelectionCacheAdapter adapter = adapter(operations);

        Optional<String> token = adapter.tryAcquireRefreshLock("A39", "80100022");
        adapter.releaseRefreshLock("A39", "80100022", token.orElseThrow());

        assertThat(operations.lockKey)
                .isEqualTo("cardshop:trade:recharge:{A39}:product-selection:80100022:refresh-lock");
        assertThat(operations.lockTtl).isEqualTo(Duration.ofSeconds(5));
        assertThat(operations.releasedToken).isEqualTo(token.orElseThrow());
    }

    @Test
    void renewsRefreshLockOnlyForCurrentToken() {
        FakeOperations operations = new FakeOperations();
        RedisV1ProductSelectionCacheAdapter adapter = adapter(operations);
        operations.renewResult = true;

        boolean renewed = adapter.renewRefreshLock("A39", "80100022", "token-1");

        assertThat(renewed).isTrue();
        assertThat(operations.renewedToken).isEqualTo("token-1");
        assertThat(operations.renewTtl).isEqualTo(Duration.ofSeconds(5));
    }

    private static RedisV1ProductSelectionCacheAdapter adapter(FakeOperations operations) {
        return new RedisV1ProductSelectionCacheAdapter(operations, KEY_BUILDER, SETTINGS);
    }

    private static V1ProductSelection selection() {
        return V1ProductSelection.passed(
                "80100022", "80100023", "MINISO", "名创直充100", "codekey",
                1, 10000L, 10000L, 9000L, 1L);
    }

    private static final class FakeOperations implements RedisV1ProductSelectionCacheAdapter.CacheOperations {

        private Map<String, String> hashToRead = Map.of();
        private String lockToken;
        private String lockKey;
        private Duration lockTtl;
        private String releasedToken;
        private String renewedToken;
        private Duration renewTtl;
        private String selectionKey;
        private String childKey;
        private Map<String, String> snapshotFields = Map.of();
        private Map<String, String> childFields = Map.of();
        private Duration snapshotTtl;
        private boolean completeSnapshotResult = true;
        private boolean renewResult;

        @Override
        public Map<String, String> readHash(String key) {
            return hashToRead;
        }

        @Override
        public boolean setIfAbsent(String key, String token, Duration ttl) {
            lockKey = key;
            lockToken = token;
            lockTtl = ttl;
            return true;
        }

        @Override
        public boolean renewIfOwned(String key, String token, Duration ttl) {
            lockKey = key;
            renewedToken = token;
            renewTtl = ttl;
            return renewResult;
        }

        @Override
        public boolean completeSnapshot(
                String selectionKey,
                String childKey,
                String lockKey,
                String token,
                Map<String, String> snapshotFields,
                Map<String, String> childFields,
                Duration physicalTtl) {
            this.selectionKey = selectionKey;
            this.childKey = childKey;
            this.lockKey = lockKey;
            this.snapshotFields = new LinkedHashMap<>(snapshotFields);
            this.childFields = new LinkedHashMap<>(childFields);
            this.snapshotTtl = physicalTtl;
            return completeSnapshotResult;
        }

        @Override
        public boolean completeNegative(
                String selectionKey,
                String lockKey,
                String token,
                Map<String, String> snapshotFields,
                Duration ttl) {
            this.selectionKey = selectionKey;
            this.lockKey = lockKey;
            this.snapshotFields = new LinkedHashMap<>(snapshotFields);
            this.snapshotTtl = ttl;
            return true;
        }

        @Override
        public void releaseIfOwned(String key, String token) {
            lockKey = key;
            releasedToken = token;
        }
    }
}
