package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Persist Consumer Redis 完整快照适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
 * 查单快照字段不足以落库，Consumer 需要读取完整 Redis hash 字段并映射为 `PersistOrderSnapshot`。
 */
class RedisPersistOrderSnapshotAdapterTest {

    @Test
    void mapsRedisOrderHashToPersistSnapshot() {
        RedisPersistOrderSnapshotAdapter adapter = new RedisPersistOrderSnapshotAdapter(
                key -> Map.ofEntries(
                        Map.entry("order_id", "ORDER-1001"),
                        Map.entry("org_code", "A36"),
                        Map.entry("app_id", "app-gray"),
                        Map.entry("signing_key_id", "bound-key"),
                        Map.entry("out_order_sn", "OUT-1001"),
                        Map.entry("product_id", "PARENT-100"),
                        Map.entry("selected_product_id", "SKU-100"),
                        Map.entry("account", "13800138000"),
                        Map.entry("notify_url", "https://merchant.example/callback"),
                        Map.entry("num", "1"),
                        Map.entry("order_price", "5200"),
                        Map.entry("product_name", "交行话费100元"),
                        Map.entry("product_value", "10000"),
                        Map.entry("product_type", "1"),
                        Map.entry("coupon_type", "1"),
                        Map.entry("supplier_channel", "BOCOM"),
                        Map.entry("request_hash", "request-hash-1001"),
                        Map.entry("request_hash_version", "v1"),
                        Map.entry("request_hash_alg", "SHA-256"),
                        Map.entry("order_status", "ACCEPTED"),
                        Map.entry("validation_status", "PASSED"),
                        Map.entry("fail_code", ""),
                        Map.entry("fail_reason", ""),
                        Map.entry("dispatch_allowed", "1"),
                        Map.entry("balance_reserved", "1"),
                        Map.entry("persist_status", "PERSIST_QUEUED"),
                        Map.entry("order_version", "2"),
                        Map.entry("persisted_version", "0"),
                        Map.entry("mq_publish_count", "1")),
                new RechargeRedisKeyBuilder("cardshop:trade", 128));

        var snapshot = adapter.findLatestSnapshot("A36", "OUT-1001", "ORDER-1001");

        assertThat(snapshot).isPresent();
        assertThat(java.util.Arrays.stream(snapshot.get().getClass().getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getName)).doesNotContain("signingKeyId");
        assertThat(snapshot.get().orderId()).isEqualTo("ORDER-1001");
        assertThat(snapshot.get().productName()).isEqualTo("交行话费100元");
        assertThat(snapshot.get().orderVersion()).isEqualTo(2L);
        assertThat(snapshot.get().dispatchAllowed()).isTrue();
    }

    @Test
    void returnsEmptyWhenOrderIdDoesNotMatch() {
        RedisPersistOrderSnapshotAdapter adapter = new RedisPersistOrderSnapshotAdapter(
                key -> Map.of(
                        "order_id", "ORDER-OTHER",
                        "out_order_sn", "OUT-1001"),
                new RechargeRedisKeyBuilder("cardshop:trade", 128));

        assertThat(adapter.findLatestSnapshot("A36", "OUT-1001", "ORDER-1001")).isEmpty();
    }

    @Test
    void returnsEmptyWhenRedisNumericFieldIsMalformed() {
        RedisPersistOrderSnapshotAdapter adapter = new RedisPersistOrderSnapshotAdapter(
                key -> Map.ofEntries(
                        Map.entry("order_id", "ORDER-1001"),
                        Map.entry("org_code", "A36"),
                        Map.entry("out_order_sn", "OUT-1001"),
                        Map.entry("product_id", "PARENT-100"),
                        Map.entry("selected_product_id", "SKU-100"),
                        Map.entry("account", "13800138000"),
                        Map.entry("notify_url", "https://merchant.example/callback"),
                        Map.entry("num", "N/A"),
                        Map.entry("request_hash", "request-hash-1001"),
                        Map.entry("order_version", "1")),
                new RechargeRedisKeyBuilder("cardshop:trade", 128));

        assertThat(adapter.findLatestSnapshot("A36", "OUT-1001", "ORDER-1001")).isEmpty();
    }
}
