package com.cardshop.trade.engine.job.recharge;

import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcileService;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import java.util.Objects;

/**
 * 余额对账任务入口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.5 余额对账任务」。任务按商户维度分片，
 * 但差异修复必须由 application/infrastructure 端口确认 pending、outbox 和流水状态后再执行。
 */
public class RechargeBalanceReconcileJob {

    private final RechargeBalanceReconcileService service;

    public RechargeBalanceReconcileJob(RechargeBalanceReconcileService service) {
        this.service = Objects.requireNonNull(service, "service must not be null");
    }

    public RechargeCompensationBatchResult runOnce(int shardTotal, int shardIndex, int limit, String workerId) {
        int normalizedTotal = RechargeJobParameters.shardTotal(shardTotal);
        return service.reconcileDue(
                normalizedTotal,
                RechargeJobParameters.shardIndex(normalizedTotal, shardIndex),
                RechargeJobParameters.limit(limit),
                RechargeJobParameters.workerId(workerId));
    }
}
