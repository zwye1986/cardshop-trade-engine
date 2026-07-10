package com.cardshop.trade.engine.application.recharge;

/**
 * outbox Worker 单批处理结果。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「10.1 XXL-Job Handler 契约」。
 *
 * @param scanned 本批扫描到的 outbox 事件数量
 * @param claimed 本批成功抢占租约的事件数量
 * @param succeeded 本批处理成功并标记 SUCCESS 的事件数量
 * @param failed 本批处理失败并写入重试状态的事件数量
 * @param skipped 本批因抢占失败或并发处理而跳过的事件数量
 */
public record RechargeOutboxBatchResult(int scanned, int claimed, int succeeded, int failed, int skipped) {

    public static RechargeOutboxBatchResult empty() {
        return new RechargeOutboxBatchResult(0, 0, 0, 0, 0);
    }

    RechargeOutboxBatchResult add(Outcome outcome) {
        return switch (outcome) {
            case SUCCEEDED -> new RechargeOutboxBatchResult(scanned, claimed, succeeded + 1, failed, skipped);
            case FAILED -> new RechargeOutboxBatchResult(scanned, claimed, succeeded, failed + 1, skipped);
            case SKIPPED -> new RechargeOutboxBatchResult(scanned, claimed, succeeded, failed, skipped + 1);
        };
    }

    RechargeOutboxBatchResult scanned(int count) {
        return new RechargeOutboxBatchResult(count, claimed, succeeded, failed, skipped);
    }

    RechargeOutboxBatchResult claimedOne() {
        return new RechargeOutboxBatchResult(scanned, claimed + 1, succeeded, failed, skipped);
    }

    enum Outcome {
        SUCCEEDED,
        FAILED,
        SKIPPED
    }
}
