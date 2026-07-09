package com.cardshop.trade.engine.job.recharge;

import com.cardshop.trade.engine.application.recharge.RechargeOutboxBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService;
import java.util.Objects;

/**
 * outbox 履约补偿任务入口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.2 分片规则」的 MySQL outbox 分片与抢占。
 * job facade 只归一化分片参数，最终并发正确性仍由 {@link RechargeOutboxWorkerService} 中的 outbox CAS
 * 抢占和租约恢复保证。
 */
public class RechargeOutboxPublishJob {

    private final RechargeOutboxWorkerService workerService;

    public RechargeOutboxPublishJob(RechargeOutboxWorkerService workerService) {
        this.workerService = Objects.requireNonNull(workerService, "workerService must not be null");
    }

    public RechargeOutboxBatchResult runOnce(int shardTotal, int shardIndex, int limit, String workerId) {
        int normalizedTotal = RechargeJobParameters.shardTotal(shardTotal);
        return workerService.processDue(
                normalizedTotal,
                RechargeJobParameters.shardIndex(normalizedTotal, shardIndex),
                RechargeJobParameters.limit(limit),
                RechargeJobParameters.workerId(workerId));
    }
}
