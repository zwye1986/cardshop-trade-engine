package com.cardshop.trade.engine.domain.order;

/**
 * Redis 受理状态未知异常类型。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「6. Redis 受理与缓存层」。
 */
public enum AcceptAnomalyType {
    WAIT_CANCEL_FAILED,
    LUA_TIMEOUT,
    REDIS_BUSY,
    ACCEPT_STATE_UNKNOWN
}
