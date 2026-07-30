package com.cardshop.trade.engine.application.recharge;

/**
 * 供应商履约 outbox 事件号契约。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。落库事务写入
 * {@code recharge_dispatch_outbox} 与落库后 timer 热唤醒必须使用同一套 event_id；
 * 否则 outbox 持久事实仍在，但 timer 会唤醒错误事件号，压测热路径会退化为低频扫描补漏。
 *
 * <p>首次派单延迟由 {@code trade.engine.fulfillment.dispatch-initial-delay-seconds} 配置，
 * 不在本类硬编码；落库 {@code next_retry_time} 与 timer 调度必须共用同一配置值。
 */
public final class RechargeDispatchEventIds {

    private static final String DISPATCH_EVENT_PREFIX = "dispatch-";
    private static final String SUPPLIER_QUERY_EVENT_PREFIX = "supplier-query-";

    private RechargeDispatchEventIds() {}

    public static String dispatchEventId(String orderId) {
        return DISPATCH_EVENT_PREFIX + requireText(orderId, "orderId");
    }

    public static String supplierQueryEventId(String orderId) {
        return SUPPLIER_QUERY_EVENT_PREFIX + requireText(orderId, "orderId");
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
