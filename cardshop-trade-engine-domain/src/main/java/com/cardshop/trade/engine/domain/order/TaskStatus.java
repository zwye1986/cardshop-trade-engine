package com.cardshop.trade.engine.domain.order;

/**
 * outbox、通知任务等可抢占后台任务的通用状态。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md} 的「6.6 新增表」。
 * `PROCESSING` 必须结合 `locked_by/locked_until/version` 判断租约是否仍有效。
 */
public enum TaskStatus {
    INIT,
    PROCESSING,
    SUCCESS,
    FAILED
}
