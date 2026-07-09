package com.cardshop.trade.engine.job.recharge;

import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushService;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import java.util.Objects;

/**
 * 余额 delta 刷账任务入口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.2 分片规则」的余额刷账任务特殊规则。
 * 该 facade 不直接写库，只把归一化后的 shard/limit 交给 application service；真实端口首版以 outbox CAS
 * 和 MySQL 原子增量更新保证单事件不重不漏，后续如做商户级聚合仍复用这个调度入口。
 */
public class RechargeBalanceDeltaFlushJob {

    private final RechargeBalanceDeltaFlushService service;

    public RechargeBalanceDeltaFlushJob(RechargeBalanceDeltaFlushService service) {
        this.service = Objects.requireNonNull(service, "service must not be null");
    }

    public RechargeCompensationBatchResult runOnce(int shardTotal, int shardIndex, int limit, String workerId) {
        int normalizedTotal = RechargeJobParameters.shardTotal(shardTotal);
        return service.flushDue(
                normalizedTotal,
                RechargeJobParameters.shardIndex(normalizedTotal, shardIndex),
                RechargeJobParameters.limit(limit),
                RechargeJobParameters.workerId(workerId));
    }
}
