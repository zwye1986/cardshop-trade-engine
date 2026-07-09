package com.cardshop.trade.engine.application.recharge;

/**
 * D11/D12 补偿任务单批执行结果。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17. 分片补偿与对账任务」。
 * 不同补偿任务的业务含义不同，但调度层都需要统一记录扫描、成功、失败和跳过数量，便于后续接入
 * XXL-Job 日志、Prometheus 指标和告警。
 *
 * @param scanned 本批扫描到的待处理记录数量
 * @param succeeded 本批处理成功数量
 * @param failed 本批处理失败数量
 * @param skipped 本批因抢占失败、状态已变化或无需处理而跳过数量
 */
public record RechargeCompensationBatchResult(int scanned, int succeeded, int failed, int skipped) {

    public RechargeCompensationBatchResult {
        scanned = Math.max(0, scanned);
        succeeded = Math.max(0, succeeded);
        failed = Math.max(0, failed);
        skipped = Math.max(0, skipped);
    }

    public static RechargeCompensationBatchResult empty() {
        return new RechargeCompensationBatchResult(0, 0, 0, 0);
    }

    RechargeCompensationBatchResult succeedOne() {
        return new RechargeCompensationBatchResult(scanned, succeeded + 1, failed, skipped);
    }

    RechargeCompensationBatchResult failOne() {
        return new RechargeCompensationBatchResult(scanned, succeeded, failed + 1, skipped);
    }

    RechargeCompensationBatchResult skipOne() {
        return new RechargeCompensationBatchResult(scanned, succeeded, failed, skipped + 1);
    }

    RechargeCompensationBatchResult scanned(int count) {
        return new RechargeCompensationBatchResult(count, succeeded, failed, skipped);
    }
}
