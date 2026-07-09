package com.cardshop.trade.engine.application.recharge;

/**
 * 余额对账端口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.5 余额对账任务」。真实实现需要按
 * `org_code` 分片比对 MySQL 余额、未刷账 outbox、Redis available/frozen 和 pending 订单；发现差异时只登记
 * 和触发修复，不在未确认 pending/outbox 前直接释放资金。
 */
public interface RechargeBalanceReconcilePort {

    RechargeCompensationBatchResult reconcileDue(int shardTotal, int shardIndex, int limit, String workerId);

    static RechargeBalanceReconcilePort noop() {
        return (shardTotal, shardIndex, limit, workerId) -> RechargeCompensationBatchResult.empty();
    }
}
