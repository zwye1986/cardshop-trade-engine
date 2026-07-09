package com.cardshop.trade.engine.application.recharge;

/**
 * 余额 delta 刷账端口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.2 分片规则」中的“余额刷账任务特殊规则”。
 * 真实实现需要扫描并 CAS 抢占 `BALANCE_DELTA_FLUSH` outbox，在同一 MySQL 事务内用
 * `mc_ext_depart.balance = balance + delta` 原子增量更新余额并标记事件完成。按商户聚合批刷属于后续优化，
 * 首版先保证单事件幂等和事务一致性。
 */
public interface RechargeBalanceDeltaFlushPort {

    RechargeCompensationBatchResult flushDue(int shardTotal, int shardIndex, int limit, String workerId);

    static RechargeBalanceDeltaFlushPort noop() {
        return (shardTotal, shardIndex, limit, workerId) -> RechargeCompensationBatchResult.empty();
    }
}
