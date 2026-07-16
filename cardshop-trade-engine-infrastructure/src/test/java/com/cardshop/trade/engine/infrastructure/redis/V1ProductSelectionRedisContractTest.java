package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * V1 父商品选品 Redis 字段合同测试。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。字段名和参数顺序
 * 同时被 Java hash 编解码、原子刷新 Lua、比较修复 Lua 和接单 Lua 使用，必须由单一 Java 合同集中声明。
 */
class V1ProductSelectionRedisContractTest {

    @Test
    void declaresCompleteSnapshotAndChildFieldOrder() {
        assertThat(V1ProductSelectionRedisContract.SNAPSHOT_FIELD_ORDER).containsExactly(
                "product_id", "selected_product_id", "supplier_channel", "product_name", "coupon_type",
                "product_type", "product_status", "sale_price", "face_value", "base_price", "product_version",
                "selection_fingerprint", "product_snapshot_available", "validation_status", "fail_code",
                "fail_reason", "loaded_at", "refresh_after", "expire_at", "cache_kind");
        assertThat(V1ProductSelectionRedisContract.PARENT_MATCH_FIELD_ORDER).containsExactly(
                "selected_product_id", "sale_price", "product_version", "product_type", "selection_fingerprint");
        assertThat(V1ProductSelectionRedisContract.CHILD_FIELD_ORDER).containsExactly(
                "status", "version", "price", "product_type");
    }

    @Test
    void normalizesMissingOrNonPositiveDurationToConfiguredDefault() {
        Duration fallback = Duration.ofSeconds(3);

        assertThat(V1ProductSelectionRedisContract.positiveOrDefault(null, fallback)).isEqualTo(fallback);
        assertThat(V1ProductSelectionRedisContract.positiveOrDefault(Duration.ZERO, fallback)).isEqualTo(fallback);
        assertThat(V1ProductSelectionRedisContract.positiveOrDefault(Duration.ofSeconds(-1), fallback))
                .isEqualTo(fallback);
        assertThat(V1ProductSelectionRedisContract.positiveOrDefault(Duration.ofSeconds(2), fallback))
                .isEqualTo(Duration.ofSeconds(2));
    }
}
