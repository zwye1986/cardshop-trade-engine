package com.cardshop.trade.engine.application.recharge;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Redis 接单异常核查证据。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
 * 三个 Map 会分别写入 `redis_check_result`、`mysql_check_result`、`mq_check_result`。
 * 当前 D5B 只完成读侧可判定的最小闭环，后续 D7/D8 接入真实 MQ confirm/Consumer 后可扩展 MQ 证据字段。
 */
public record AcceptAnomalyCheckEvidence(
        Map<String, Object> redisCheckResult,
        Map<String, Object> mysqlCheckResult,
        Map<String, Object> mqCheckResult) {

    public AcceptAnomalyCheckEvidence {
        redisCheckResult = immutable(redisCheckResult);
        mysqlCheckResult = immutable(mysqlCheckResult);
        mqCheckResult = immutable(mqCheckResult);
    }

    private static Map<String, Object> immutable(Map<String, Object> value) {
        if (value == null || value.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(value));
    }
}
