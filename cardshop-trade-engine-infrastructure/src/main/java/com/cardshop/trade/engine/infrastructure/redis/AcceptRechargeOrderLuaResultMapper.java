package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.logging.LogStormGuard.LogDecision;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * `accept_recharge_order.lua` 返回值映射器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
 * Lua 返回扁平 key-value 数组，Java 侧在这里集中做类型归一，避免接口层或用例层依赖 Redis 原始返回结构。
 */
final class AcceptRechargeOrderLuaResultMapper {

    private static final Logger log = LoggerFactory.getLogger(AcceptRechargeOrderLuaResultMapper.class);
    private static final String LUA_TRUE = "1";
    private static final String FIELD_CODE = "code";
    private static final String FIELD_ORDER_ID = "order_id";
    private static final String FIELD_VALIDATION_STATUS = "validation_status";
    private static final String FIELD_FAIL_CODE = "fail_code";
    private static final String FIELD_FAIL_REASON = "fail_reason";
    private static final String FIELD_DISPATCH_ALLOWED = "dispatch_allowed";
    private static final String FIELD_BALANCE_RESERVED = "balance_reserved";
    private static final String FIELD_ORDER_VERSION = "order_version";
    private static final String LUA_STATUS_UNKNOWN = "UNKNOWN";
    private static final String LOG_EVENT_UNKNOWN_STATUS = "accept-lua-result.unknown-status";
    private static final String LOG_EVENT_VERSION_PARSE_FAILED = "accept-lua-result.version-parse.failed";

    private final LogStormGuard logStormGuard = LogStormGuard.defaultGuard();

    AcceptRechargeOrderResult map(Object rawResult) {
        Map<String, String> values = toMap(rawResult);
        AcceptRechargeOrderStatus status = parseStatus(values.get(FIELD_CODE));
        return new AcceptRechargeOrderResult(
                status,
                values.get(FIELD_ORDER_ID),
                values.get(FIELD_VALIDATION_STATUS),
                values.get(FIELD_FAIL_CODE),
                values.get(FIELD_FAIL_REASON),
                LUA_TRUE.equals(values.get(FIELD_DISPATCH_ALLOWED)),
                LUA_TRUE.equals(values.get(FIELD_BALANCE_RESERVED)),
                parseLong(values.get(FIELD_ORDER_VERSION)));
    }

    private static Map<String, String> toMap(Object rawResult) {
        Map<String, String> values = new HashMap<>();
        if (!(rawResult instanceof List<?> list)) {
            values.put(FIELD_CODE, LUA_STATUS_UNKNOWN);
            values.put(FIELD_FAIL_CODE, AcceptRechargeOrderResult.FAIL_CODE_UNKNOWN);
            values.put(FIELD_FAIL_REASON, "Lua 返回结构不是数组");
            return values;
        }
        for (int i = 0; i + 1 < list.size(); i += 2) {
            values.put(String.valueOf(list.get(i)), String.valueOf(list.get(i + 1)));
        }
        return values;
    }

    private AcceptRechargeOrderStatus parseStatus(String code) {
        if (code == null || code.isBlank()) {
            return AcceptRechargeOrderStatus.UNKNOWN;
        }
        try {
            return AcceptRechargeOrderStatus.valueOf(code);
        } catch (IllegalArgumentException ex) {
            LogDecision decision = logStormGuard.shouldLog(LOG_EVENT_UNKNOWN_STATUS);
            if (decision.shouldLog()) {
                log.error("Redis接单Lua返回未知状态 code={}, occurrence={}, sampled={}",
                        code,
                        decision.occurrence(),
                        decision.sampledOccurrence(),
                        ex);
            }
            return AcceptRechargeOrderStatus.UNKNOWN;
        }
    }

    private long parseLong(String value) {
        if (value == null || value.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            LogDecision decision = logStormGuard.shouldLog(LOG_EVENT_VERSION_PARSE_FAILED);
            if (decision.shouldLog()) {
                log.error("Redis接单Lua订单版本解析失败 value={}, occurrence={}, sampled={}",
                        value,
                        decision.occurrence(),
                        decision.sampledOccurrence(),
                        ex);
            }
            return 0L;
        }
    }
}
