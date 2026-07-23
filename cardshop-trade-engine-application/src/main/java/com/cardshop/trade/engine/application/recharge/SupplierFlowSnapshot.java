package com.cardshop.trade.engine.application.recharge;

/**
 * 供应商调用流水的最小读取快照。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。DISPATCH 事件恢复时只需要判断
 * 同订单是否已经进入过供应商调用窗口；一旦存在流水，就按“不可确认失败”转查单，避免恢复扫描盲目重复派单。
 *
 * @param flowId 供应商流水主键，对应 {@code mc_order_supplier_flow.id}
 * @param status 旧表流水状态，对应 {@code mc_order_supplier_flow.status}
 */
public record SupplierFlowSnapshot(
        long flowId,
        String status) {
}
