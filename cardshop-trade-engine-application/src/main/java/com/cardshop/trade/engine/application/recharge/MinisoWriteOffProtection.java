package com.cardshop.trade.engine.application.recharge;

/** 名创作废与释放预扣的核销证据门禁；参见docs/交行直充业务系统详细设计.md「9.5」。 */
public interface MinisoWriteOffProtection {
    /** 持久转作废事件为人工，并收敛CANCEL_REQUESTED；true表示禁止继续作废/退款。 */
    boolean blockInvalidation(String eventId, String orderId);

    /** 已持有订单事务的最终作废确认必须复核最新核销证据，冲突抛异常使终态整体回滚。 */
    void assertVoidAllowed(String orderId);
}
