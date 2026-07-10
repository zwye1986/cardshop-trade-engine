package com.cardshop.trade.engine.application.recharge;

/**
 * Redis 接单异常自动核查批次结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」。
 * 后续 XXL-Job handler 可以把该结果写入日志或指标，用于观测异常收敛速度。
 *
 * @param checked 本批完成核查的异常记录数量
 * @param resolvedAccepted 本批确认为已受理并关闭的数量
 * @param resolvedNotAccepted 本批确认为未受理并关闭的数量
 * @param manualRequired 本批转人工处理的数量
 * @param failed 本批核查过程失败、等待后续重试的数量
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
