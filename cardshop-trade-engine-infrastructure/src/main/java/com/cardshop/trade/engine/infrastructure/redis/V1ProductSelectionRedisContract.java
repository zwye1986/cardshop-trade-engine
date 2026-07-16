package com.cardshop.trade.engine.infrastructure.redis;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * V1 父商品选品快照与实际子商品配置的 Redis 字段合同。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。这些字段名和顺序
 * 同时约束 Java hash 编解码、缓存完成 Lua、比较修复 Lua 和接单 Lua；业务字段变更必须先修改本合同，
 * 再由契约测试确认所有 Lua 消费方同步完成，避免位置参数与 hash 字段发生静默漂移。
 */
final class V1ProductSelectionRedisContract {

    static final String STATUS_ENABLED = "ENABLED";

    static final String FIELD_PRODUCT_ID = "product_id";
    static final String FIELD_SELECTED_PRODUCT_ID = "selected_product_id";
    static final String FIELD_SUPPLIER_CHANNEL = "supplier_channel";
    static final String FIELD_PRODUCT_NAME = "product_name";
    static final String FIELD_COUPON_TYPE = "coupon_type";
    static final String FIELD_PRODUCT_TYPE = "product_type";
    static final String FIELD_PRODUCT_STATUS = "product_status";
    static final String FIELD_SALE_PRICE = "sale_price";
    static final String FIELD_FACE_VALUE = "face_value";
    static final String FIELD_BASE_PRICE = "base_price";
    static final String FIELD_PRODUCT_VERSION = "product_version";
    static final String FIELD_SELECTION_FINGERPRINT = "selection_fingerprint";
    static final String FIELD_PRODUCT_SNAPSHOT_AVAILABLE = "product_snapshot_available";
    static final String FIELD_VALIDATION_STATUS = "validation_status";
    static final String FIELD_FAIL_CODE = "fail_code";
    static final String FIELD_FAIL_REASON = "fail_reason";
    static final String FIELD_LOADED_AT = "loaded_at";
    static final String FIELD_REFRESH_AFTER = "refresh_after";
    static final String FIELD_EXPIRE_AT = "expire_at";
    static final String FIELD_CACHE_KIND = "cache_kind";

    static final String CHILD_FIELD_STATUS = "status";
    static final String CHILD_FIELD_VERSION = "version";
    static final String CHILD_FIELD_PRICE = "price";

    static final List<String> SNAPSHOT_FIELD_ORDER = List.of(
            FIELD_PRODUCT_ID,
            FIELD_SELECTED_PRODUCT_ID,
            FIELD_SUPPLIER_CHANNEL,
            FIELD_PRODUCT_NAME,
            FIELD_COUPON_TYPE,
            FIELD_PRODUCT_TYPE,
            FIELD_PRODUCT_STATUS,
            FIELD_SALE_PRICE,
            FIELD_FACE_VALUE,
            FIELD_BASE_PRICE,
            FIELD_PRODUCT_VERSION,
            FIELD_SELECTION_FINGERPRINT,
            FIELD_PRODUCT_SNAPSHOT_AVAILABLE,
            FIELD_VALIDATION_STATUS,
            FIELD_FAIL_CODE,
            FIELD_FAIL_REASON,
            FIELD_LOADED_AT,
            FIELD_REFRESH_AFTER,
            FIELD_EXPIRE_AT,
            FIELD_CACHE_KIND);

    static final List<String> PARENT_MATCH_FIELD_ORDER = List.of(
            FIELD_SELECTED_PRODUCT_ID,
            FIELD_SALE_PRICE,
            FIELD_PRODUCT_VERSION,
            FIELD_PRODUCT_TYPE,
            FIELD_SELECTION_FINGERPRINT);

    static final List<String> CHILD_FIELD_ORDER = List.of(
            CHILD_FIELD_STATUS,
            CHILD_FIELD_VERSION,
            CHILD_FIELD_PRICE,
            FIELD_PRODUCT_TYPE);

    private V1ProductSelectionRedisContract() {
    }

    static String value(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    static Duration positiveOrDefault(Duration value, Duration fallback) {
        Objects.requireNonNull(fallback, "fallback must not be null");
        return value == null || value.isZero() || value.isNegative() ? fallback : value;
    }

    static void appendFields(List<String> target, Map<String, String> fields, List<String> order) {
        for (String field : order) {
            target.add(Objects.requireNonNull(fields.get(field), "missing Redis field: " + field));
        }
    }
}
