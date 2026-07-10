package com.cardshop.trade.engine.application.recharge;

/**
 * Redis 缓存修复和基线预热端口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.4 缓存修复任务」和
 * {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」的 Redis 基线预热边界。
 * 真实实现需要在商户级 repairing_lock 保护下重建 merchant/product/balance 基线，并按订单维度回填近期快照。
 */
public interface RechargeCacheRepairPort {

    RechargeCompensationBatchResult repairDue(int shardTotal, int shardIndex, int limit, String workerId);

    static RechargeCacheRepairPort noop() {
        return (shardTotal, shardIndex, limit, workerId) -> RechargeCompensationBatchResult.empty();
    }
}
