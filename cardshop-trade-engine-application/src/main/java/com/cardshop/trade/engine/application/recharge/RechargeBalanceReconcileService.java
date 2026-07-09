package com.cardshop.trade.engine.application.recharge;

import java.util.Objects;

/**
 * 余额对账应用服务。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.5 余额对账任务」。本服务保持 thin
 * application 边界，后续真实实现可在端口内补齐差异登记、商户写入关闭和缓存修复触发。
 */
public class RechargeBalanceReconcileService {

    private final RechargeBalanceReconcilePort port;

    public RechargeBalanceReconcileService(RechargeBalanceReconcilePort port) {
        this.port = Objects.requireNonNull(port, "port must not be null");
    }

    public RechargeCompensationBatchResult reconcileDue(int shardTotal, int shardIndex, int limit, String workerId) {
        return port.reconcileDue(shardTotal, shardIndex, limit, workerId);
    }
}
