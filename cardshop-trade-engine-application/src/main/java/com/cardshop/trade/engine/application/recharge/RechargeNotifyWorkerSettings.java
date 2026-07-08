package com.cardshop.trade.engine.application.recharge;

import java.time.Duration;
import java.util.Objects;

/**
 * 回调 Worker 租约和退避配置。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」。
 *
 * @param claimLease 回调任务抢占租约时长，对应 {@code recharge_notify_task.locked_until}
 * @param retryBackoff 回调失败后的下一次重试退避时间
 */
public record RechargeNotifyWorkerSettings(Duration claimLease, Duration retryBackoff) {

    public RechargeNotifyWorkerSettings {
        claimLease = positive(claimLease, "claimLease");
        retryBackoff = positive(retryBackoff, "retryBackoff");
    }

    private static Duration positive(Duration value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }
}
