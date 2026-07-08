package com.cardshop.trade.engine.application.recharge;

import java.util.Map;

/**
 * 创建 {@code mc_order_supplier_flow} 的命令。
 *
 * <p>字段含义与旧表 DDL COMMENT 保持一致：订单号、接口外部订单号、渠道、下单状态、请求参数和备注。
 *
 * @param orderId 订单号，对应旧表 {@code mc_order_supplier_flow.order_Id}
 * @param outOrderSn 接口外部订单号，对应旧表 {@code mc_order_supplier_flow.out_order_no}
 * @param channel 供应商渠道，对应旧表 {@code mc_order_supplier_flow.channel}
 * @param status 下单状态，对应旧表 {@code mc_order_supplier_flow.status}
 * @param remark 流水备注，对应旧表 {@code mc_order_supplier_flow.remark}
 * @param supplierRequestId 供应商请求幂等号，写入请求参数 JSON 便于追踪
 * @param requestPayload 供应商请求参数，序列化后写入 {@code mc_order_supplier_flow.request_param}
 */
public record SupplierFlowStartCommand(
        String orderId,
        String outOrderSn,
        String channel,
        String status,
        String remark,
        String supplierRequestId,
        Map<String, Object> requestPayload) {
}
