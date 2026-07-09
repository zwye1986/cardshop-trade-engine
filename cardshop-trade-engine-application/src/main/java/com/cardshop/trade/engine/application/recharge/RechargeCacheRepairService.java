package com.cardshop.trade.engine.application.recharge;

import java.util.Objects;

/**
 * Redis 缓存修复和基线预热应用服务。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.4 缓存修复任务」。本服务先固定
 * `rechargeCacheRepairJob` 的 application 边界；真实修复动作必须在端口实现中持有 repairing_lock，
 * 防止预热/重建期间接口线程继续写入不完整基线。
 */
public class RechargeCacheRepairService {

    private final RechargeCacheRepairPort port;

    public RechargeCacheRepairService(RechargeCacheRepairPort port) {
        this.port = Objects.requireNonNull(port, "port must not be null");
    }

    public RechargeCompensationBatchResult repairDue(int shardTotal, int shardIndex, int limit, String workerId) {
        return port.repairDue(shardTotal, shardIndex, limit, workerId);
    }
}
