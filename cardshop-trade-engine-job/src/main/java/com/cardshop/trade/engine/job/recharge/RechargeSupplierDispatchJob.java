package com.cardshop.trade.engine.job.recharge;

import com.cardshop.trade.engine.application.recharge.RechargeOutboxBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.util.List;
import java.util.Objects;

/**
 * 供应商派单 outbox 调度门面。
 *
 * <p>参见 {@code docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md}：
 * 固定只处理 {@link OutboxEventType#SUPPLIER_DISPATCH}，与 QUERY/INVALID 调度隔离。
 * 不继承 {@link RechargeOutboxPublishJob}，避免 Spring 按父类型注入歧义。
 */
public class RechargeSupplierDispatchJob {

    private final RechargeOutboxPublishJob delegate;

    public RechargeSupplierDispatchJob(RechargeOutboxWorkerService workerService) {
        this.delegate = new RechargeOutboxPublishJob(
                Objects.requireNonNull(workerService, "workerService must not be null"));
    }

    public RechargeOutboxBatchResult runOnce(int shardTotal, int shardIndex, int limit, String workerId) {
        return runOnce(shardTotal, shardIndex, limit, workerId, -1);
    }

    public RechargeOutboxBatchResult runOnce(
            int shardTotal, int shardIndex, int limit, String workerId, int parallelism) {
        return delegate.runOnce(
                shardTotal,
                shardIndex,
                limit,
                workerId,
                List.of(OutboxEventType.SUPPLIER_DISPATCH),
                parallelism);
    }
}
