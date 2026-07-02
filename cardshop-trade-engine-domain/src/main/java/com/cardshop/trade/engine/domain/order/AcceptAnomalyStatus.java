package com.cardshop.trade.engine.domain.order;

/**
 * Redis 受理状态未知异常的核查状态。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.13.6 Redis Lua 下单结果」。
 * 该状态只用于 `recharge_accept_anomaly`，最终订单事实仍要回写 Redis/MySQL 或转人工。
 */
public enum AcceptAnomalyStatus {
    INIT,
    CHECKING,
    RESOLVED_ACCEPTED,
    RESOLVED_NOT_ACCEPTED,
    MANUAL_REQUIRED
}
