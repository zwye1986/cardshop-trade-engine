package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 等待自动核查的 Redis 接单异常记录。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
 * 这是 application 层读取 `recharge_accept_anomaly` 后使用的稳定模型；它只表达核查所需的业务定位字段，
 * 不暴露 MyBatis DO 或表字段命名。
 */
public record AcceptAnomalyCase(
        String anomalyId,
        AcceptAnomalyType anomalyType,
        String orgCode,
        String outOrderSn,
        String orderId,
        String requestHash,
        String persistMsgId,
        Map<String, Object> rawContext) {

    public AcceptAnomalyCase {
        if (!hasText(anomalyId)) {
            throw new IllegalArgumentException("anomalyId must not be blank");
        }
        anomalyType = Objects.requireNonNull(anomalyType, "anomalyType must not be null");
        if (!hasText(orgCode)) {
            throw new IllegalArgumentException("orgCode must not be blank");
        }
        if (!hasText(outOrderSn)) {
            throw new IllegalArgumentException("outOrderSn must not be blank");
        }
        orderId = normalize(orderId);
        requestHash = normalize(requestHash);
        persistMsgId = normalize(persistMsgId);
        rawContext = immutableContext(rawContext);
    }

    private static Map<String, Object> immutableContext(Map<String, Object> rawContext) {
        if (rawContext == null || rawContext.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> copy = new LinkedHashMap<>();
        rawContext.forEach((key, value) -> {
            if (!hasText(key)) {
                throw new IllegalArgumentException("rawContext key must not be blank");
            }
            copy.put(key, value);
        });
        return Collections.unmodifiableMap(copy);
    }

    private static String normalize(String value) {
        return value == null ? "" : value;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
