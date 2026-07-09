package com.cardshop.trade.engine.job.recharge;

import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairService;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import java.util.Objects;

/**
 * Redis 缓存修复和基线预热任务入口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.4 缓存修复任务」。该入口覆盖订单快照修复
 * 与新商户/全新 Redis DB 的基线预热调度边界；真实实现必须在端口内持有 repairing_lock 后再重建 Redis 基线。
 */
public class RechargeCacheRepairJob {

    private final RechargeCacheRepairService service;

    public RechargeCacheRepairJob(RechargeCacheRepairService service) {
        this.service = Objects.requireNonNull(service, "service must not be null");
    }

    public RechargeCompensationBatchResult runOnce(int shardTotal, int shardIndex, int limit, String workerId) {
        int normalizedTotal = RechargeJobParameters.shardTotal(shardTotal);
        return service.repairDue(
                normalizedTotal,
                RechargeJobParameters.shardIndex(normalizedTotal, shardIndex),
                RechargeJobParameters.limit(limit),
                RechargeJobParameters.workerId(workerId));
    }
}
