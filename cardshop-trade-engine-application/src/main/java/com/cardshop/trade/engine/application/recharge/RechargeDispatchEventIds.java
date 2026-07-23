package com.cardshop.trade.engine.application.recharge;

import java.time.Duration;

/**
 * 供应商履约 outbox 事件号与默认调度节奏契约。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。落库事务写入
 * {@code recharge_dispatch_outbox} 与落库后 timer 热唤醒必须使用同一套 event_id 和初始 due 时间规则；
 * 否则 outbox 持久事实仍在，但 timer 会唤醒错误事件号或早于 due 时间触发，压测热路径会退化为低频扫描补漏。
 */
public final class RechargeDispatchEventIds {

    private static final String DISPATCH_EVENT_PREFIX = "dispatch-";
    private static final String SUPPLIER_QUERY_EVENT_PREFIX = "supplier-query-";
    private static final Duration DEFAULT_DISPATCH_INITIAL_DELAY = Duration.ofSeconds(1);

    private RechargeDispatchEventIds() {}

    public static String dispatchEventId(String orderId) {
        return DISPATCH_EVENT_PREFIX + requireText(orderId, "orderId");
    }

    public static String supplierQueryEventId(String orderId) {
        return SUPPLIER_QUERY_EVENT_PREFIX + requireText(orderId, "orderId");
    }

    public static Duration dispatchInitialDelay() {
        return DEFAULT_DISPATCH_INITIAL_DELAY;
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
