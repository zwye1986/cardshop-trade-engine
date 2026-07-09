package com.cardshop.trade.engine.job.recharge;

import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargePersistMqRepublishService;
import java.util.Objects;

/**
 * Redis pending MQ 补投任务入口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.11 XXL-Job Handler 契约」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「17.3 MQ 补投任务」。本类暂不直接依赖 XXL-Job API，
 * 后续 handler 只需把 shard/limit/workerId 解析后委托到 {@link #runOnce(int, int, int, String)}。
 */
public class RechargePersistMqRepublishJob {

    private final RechargePersistMqRepublishService service;

    public RechargePersistMqRepublishJob(RechargePersistMqRepublishService service) {
        this.service = Objects.requireNonNull(service, "service must not be null");
    }

    public RechargeCompensationBatchResult runOnce(int shardTotal, int shardIndex, int limit, String workerId) {
        int normalizedTotal = RechargeJobParameters.shardTotal(shardTotal);
        return service.republishDue(
                normalizedTotal,
                RechargeJobParameters.shardIndex(normalizedTotal, shardIndex),
                RechargeJobParameters.limit(limit),
                RechargeJobParameters.workerId(workerId));
    }
}
