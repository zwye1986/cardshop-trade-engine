package com.cardshop.trade.engine.application.recharge;

/**
 * 更新 {@code mc_order_supplier_flow} 响应结果的命令。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」对供应商流水的追踪要求。
 *
 * @param flowId 供应商流水主键，对应 {@code mc_order_supplier_flow.id}
 * @param status 下单状态，对应 {@code mc_order_supplier_flow.status}
 * @param remark 流水备注，对应 {@code mc_order_supplier_flow.remark}
 * @param responseCode 供应商响应码，写入 {@code mc_order_supplier_flow.response_result}
 * @param responseMessage 供应商响应描述，写入 {@code mc_order_supplier_flow.response_result}
 * @param supplierOrderId 供应商侧订单号，写入 {@code mc_order_supplier_flow.response_result}
 * @param rawResponse 供应商响应原文，写入 {@code mc_order_supplier_flow.response_result}
 */
public record SupplierFlowUpdateCommand(
        long flowId,
        String status,
        String remark,
        String responseCode,
        String responseMessage,
        String supplierOrderId,
        String rawResponse) {
}
