package com.cardshop.trade.engine.infrastructure.redis;

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

    AcceptRechargeOrderResult map(Object rawResult) {
        Map<String, String> values = toMap(rawResult);
        AcceptRechargeOrderStatus status = parseStatus(values.get("code"));
        return new AcceptRechargeOrderResult(
                status,
                values.get("order_id"),
                values.get("validation_status"),
                values.get("fail_code"),
                values.get("fail_reason"),
                "1".equals(values.get("dispatch_allowed")),
                "1".equals(values.get("balance_reserved")),
                parseLong(values.get("order_version")));
    }

    private static Map<String, String> toMap(Object rawResult) {
        Map<String, String> values = new HashMap<>();
        if (!(rawResult instanceof List<?> list)) {
            values.put("code", "UNKNOWN");
            values.put("fail_reason", "Lua 返回结构不是数组");
            return values;
        }
        for (int i = 0; i + 1 < list.size(); i += 2) {
            values.put(String.valueOf(list.get(i)), String.valueOf(list.get(i + 1)));
        }
        return values;
    }

    private static AcceptRechargeOrderStatus parseStatus(String code) {
        if (code == null || code.isBlank()) {
            return AcceptRechargeOrderStatus.UNKNOWN;
        }
        try {
            return AcceptRechargeOrderStatus.valueOf(code);
        } catch (IllegalArgumentException ex) {
            log.error("Redis接单Lua返回未知状态 code={}", code, ex);
            return AcceptRechargeOrderStatus.UNKNOWN;
        }
    }

    private static long parseLong(String value) {
        if (value == null || value.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            log.error("Redis接单Lua订单版本解析失败 value={}", value, ex);
            return 0L;
        }
    }
}
