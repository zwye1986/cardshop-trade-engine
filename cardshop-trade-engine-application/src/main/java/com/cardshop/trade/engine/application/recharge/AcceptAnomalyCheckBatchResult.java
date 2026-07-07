package com.cardshop.trade.engine.application.recharge;

/**
 * Redis 接单异常自动核查批次结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.3 Redis HA Guard」。
 * 后续 XXL-Job handler 可以把该结果写入日志或指标，用于观测异常收敛速度。
 */
public record AcceptAnomalyCheckBatchResult(
        int checked,
        int resolvedAccepted,
        int resolvedNotAccepted,
        int manualRequired,
        int failed) {

    public static AcceptAnomalyCheckBatchResult empty() {
        return new AcceptAnomalyCheckBatchResult(0, 0, 0, 0, 0);
    }
}
