package com.cardshop.trade.engine.application.recharge;

import java.time.Duration;
import java.util.Objects;

/**
 * outbox 履约 Worker 的租约、重试和限频参数。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」。这些参数只影响后台推进节奏，
 * 不改变 Redis 接单和 MySQL 最终事实语义。
 *
 * @param claimLease outbox 抢占租约时长，对应 {@code locked_until} 的推进窗口
 * @param retryBackoff 普通失败后的下一次重试退避时间
 * @param frequencyRetryDelay 同账号限频命中后的下一次重试延迟
 * @param frequencyLockTtl 同账号直充限频锁 TTL
 * @param frequencySuccessMinimumHold 订单进入终态后保留限频锁的最短时间
 */
public record RechargeOutboxWorkerSettings(
        Duration claimLease,
        Duration retryBackoff,
        Duration frequencyRetryDelay,
        Duration frequencyLockTtl,
        Duration frequencySuccessMinimumHold) {

    public RechargeOutboxWorkerSettings {
        claimLease = positive(claimLease, "claimLease");
        retryBackoff = positive(retryBackoff, "retryBackoff");
        frequencyRetryDelay = positive(frequencyRetryDelay, "frequencyRetryDelay");
        frequencyLockTtl = positive(frequencyLockTtl, "frequencyLockTtl");
        frequencySuccessMinimumHold = positive(frequencySuccessMinimumHold, "frequencySuccessMinimumHold");
    }

    private static Duration positive(Duration value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }
}
