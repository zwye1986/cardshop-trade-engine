package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.logging.LogStormGuard.LogDecision;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import java.nio.charset.StandardCharsets;
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
        list = flattenSingleNestedList(list);
        for (int i = 0; i + 1 < list.size(); i += 2) {
            String key = toRedisScalarString(list.get(i));
            if (key == null || key.isBlank()) {
                continue;
            }
            values.put(key, toRedisScalarString(list.get(i + 1)));
        }
        return values;
    }

    /**
     * 兼容 Lettuce 原生 reactive `eval(..., ScriptOutputType.MULTI)` 的单元素嵌套数组返回。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」以及
     * 「20.13.8 异步落库 Consumer」。`LettuceRechargeRedisSessionFactory` 为了在同一 Redis 连接上执行
     * accept Lua 与 WAIT，使用 Lettuce 原生命令并对返回流执行 `collectList()`；真实 Redis 返回的 Lua 数组可能先作为
     * 一个 MULTI 元素发出，收集后形态变成 `List<List<String>>`。若不在基础设施层展开，Java 会得到空字段映射，
     * 把 Redis 已经写入的 `ACCEPTED` 误判为 `UNKNOWN`，进而跳过 WAIT/MQ confirm，形成 Redis `PENDING_MQ`
     * 悬挂订单。这里仅展开“单元素且该元素仍是 List”的形态，避免把合法扁平 key-value 数组误改。
     */
    private static List<?> flattenSingleNestedList(List<?> list) {
        if (list.size() == 1 && list.getFirst() instanceof List<?> nested) {
            return nested;
        }
        return list;
    }

    /**
     * 将 Redis Lua 返回的单个标量归一为 UTF-8 字符串。
     *
     * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」以及
     * {@code docs/交行直充业务系统高可用方案设计.md}「14.3 Redis 成功但 MQ confirm 失败协议」。
     * 这里不能直接使用 {@link String#valueOf(Object)}：不同 Redis 执行路径可能把 bulk string
     * 返回成 {@code String}、{@code byte[]} 或数字类型。若 {@code byte[]} 被格式化成 JVM 对象标识
     * （例如 {@code [B@...}），应用会把已经写入 Redis 的 {@code ACCEPTED} 误判为 {@code UNKNOWN}，
     * 从而跳过 WAIT 与 RabbitMQ confirm，形成“Redis 有订单、接口返回系统繁忙、MySQL 无落库”的危险窗口。
     * 因此所有 key/value 必须先在基础设施层完成类型归一，再交给应用层判断接单结果。
     */
    private static String toRedisScalarString(Object rawValue) {
        if (rawValue == null) {
            return null;
        }
        if (rawValue instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(rawValue);
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
