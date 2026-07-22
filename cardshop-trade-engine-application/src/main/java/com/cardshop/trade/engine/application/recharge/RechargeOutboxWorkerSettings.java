package com.cardshop.trade.engine.application.recharge;

import java.time.Duration;
import java.util.Objects;

/**
 * outbox 履约 Worker 的租约、重试、限频与并行参数。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和
 * {@code docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md}。
 *
 * @param claimLease outbox 抢占租约时长
 * @param retryBackoff 普通失败后的下一次重试退避时间
 * @param frequencyRetryDelay 同账号限频命中后的下一次重试延迟
 * @param frequencyLockTtl 同账号直充限频锁 TTL
 * @param frequencySuccessMinimumHold 订单进入终态后保留限频锁的最短时间
 * @param outboxParallelism 已 claim 事件的并行执行度（1～32）
 * @param outboxScanOverfetchFactor due 扫描相对 limit 的放大倍数（≥1）
 */
public record RechargeOutboxWorkerSettings(
        Duration claimLease,
        Duration retryBackoff,
        Duration frequencyRetryDelay,
        Duration frequencyLockTtl,
        Duration frequencySuccessMinimumHold,
        int outboxParallelism,
        int outboxScanOverfetchFactor) {

    private static final int MIN_PARALLELISM = 1;
    private static final int MAX_PARALLELISM = 32;

    public RechargeOutboxWorkerSettings {
        claimLease = positive(claimLease, "claimLease");
        retryBackoff = positive(retryBackoff, "retryBackoff");
        frequencyRetryDelay = positive(frequencyRetryDelay, "frequencyRetryDelay");
        frequencyLockTtl = positive(frequencyLockTtl, "frequencyLockTtl");
        frequencySuccessMinimumHold = positive(frequencySuccessMinimumHold, "frequencySuccessMinimumHold");
        if (outboxParallelism < MIN_PARALLELISM || outboxParallelism > MAX_PARALLELISM) {
            throw new IllegalArgumentException(
                    "outboxParallelism must be between " + MIN_PARALLELISM + " and " + MAX_PARALLELISM);
        }
        if (outboxScanOverfetchFactor < 1) {
            throw new IllegalArgumentException("outboxScanOverfetchFactor must be >= 1");
        }
    }

    /**
     * 兼容旧五参数构造：并行度 1、overfetch 1（串行等价旧行为）。
     */
    public RechargeOutboxWorkerSettings(
            Duration claimLease,
            Duration retryBackoff,
            Duration frequencyRetryDelay,
            Duration frequencyLockTtl,
            Duration frequencySuccessMinimumHold) {
        this(
                claimLease,
                retryBackoff,
                frequencyRetryDelay,
                frequencyLockTtl,
                frequencySuccessMinimumHold,
                1,
                1);
    }

    private static Duration positive(Duration value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }
}
