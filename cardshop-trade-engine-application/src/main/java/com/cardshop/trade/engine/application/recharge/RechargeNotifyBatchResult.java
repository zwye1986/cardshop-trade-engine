package com.cardshop.trade.engine.application.recharge;

/**
 * 回调 Worker 单批处理结果。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。
 *
 * @param scanned 本批扫描到的回调任务数量
 * @param claimed 本批成功抢占租约的回调任务数量
 * @param succeeded 本批推送成功并标记 SUCCESS 的任务数量
 * @param failed 本批推送失败并写入重试状态的任务数量
 * @param skipped 本批因抢占失败或并发处理而跳过的任务数量
 */
public record RechargeNotifyBatchResult(int scanned, int claimed, int succeeded, int failed, int skipped) {

    static RechargeNotifyBatchResult empty() {
        return new RechargeNotifyBatchResult(0, 0, 0, 0, 0);
    }

    RechargeNotifyBatchResult scanned(int count) {
        return new RechargeNotifyBatchResult(count, claimed, succeeded, failed, skipped);
    }

    RechargeNotifyBatchResult claimedOne() {
        return new RechargeNotifyBatchResult(scanned, claimed + 1, succeeded, failed, skipped);
    }

    RechargeNotifyBatchResult succeededOne() {
        return new RechargeNotifyBatchResult(scanned, claimed, succeeded + 1, failed, skipped);
    }

    RechargeNotifyBatchResult failedOne() {
        return new RechargeNotifyBatchResult(scanned, claimed, succeeded, failed + 1, skipped);
    }

    RechargeNotifyBatchResult skippedOne() {
        return new RechargeNotifyBatchResult(scanned, claimed, succeeded, failed, skipped + 1);
    }
}
