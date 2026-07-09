package com.cardshop.trade.engine.application.recharge;

import java.util.Objects;

/**
 * 余额 delta 刷账应用服务。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.2 分片规则」。application 层只保留调度
 * 到端口的稳定边界，具体 outbox CAS、商户行锁和 MySQL 事务聚合在基础设施实现中完成。
 */
public class RechargeBalanceDeltaFlushService {

    private final RechargeBalanceDeltaFlushPort port;

    public RechargeBalanceDeltaFlushService(RechargeBalanceDeltaFlushPort port) {
        this.port = Objects.requireNonNull(port, "port must not be null");
    }

    public RechargeCompensationBatchResult flushDue(int shardTotal, int shardIndex, int limit, String workerId) {
        return port.flushDue(shardTotal, shardIndex, limit, workerId);
    }
}
