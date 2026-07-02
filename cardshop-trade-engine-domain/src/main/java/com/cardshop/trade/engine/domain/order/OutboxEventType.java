package com.cardshop.trade.engine.domain.order;

/**
 * MySQL outbox 可靠事件类型。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md} 的「6.6 新增表」。
 * outbox 只负责 MySQL 订单事实提交后的后续可靠动作，不参与 Redis 到 MySQL 的落库补投。
 */
public enum OutboxEventType {
    SUPPLIER_DISPATCH,
    SUPPLIER_QUERY,
    INVALID_ORDER,
    BALANCE_DELTA_FLUSH,
    BALANCE_RELEASE,
    CACHE_REPAIR
}
