package com.cardshop.trade.engine.application.recharge;

/**
 * 供应商 Gateway 归一化响应。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」供应商派单幂等要求。
 *
 * @param status 归一化后的供应商处理状态，只能是成功、处理中或明确失败
 * @param supplierOrderId 供应商侧订单号，用于后续查单、作废或人工排查
 * @param responseCode 供应商原始响应码，用于写入供应商流水和失败原因
 * @param responseMessage 供应商原始响应描述，用于写入供应商流水和失败原因
 * @param rawResponse 供应商响应原文，用于写入 {@code mc_order_supplier_flow.response_result}
 */
public record SupplierGatewayResponse(
        SupplierGatewayStatus status,
        String supplierOrderId,
        String responseCode,
        String responseMessage,
        String rawResponse) {

    public static SupplierGatewayResponse success(
            String supplierOrderId, String responseCode, String responseMessage, String rawResponse) {
        return new SupplierGatewayResponse(
                SupplierGatewayStatus.SUCCESS, supplierOrderId, responseCode, responseMessage, rawResponse);
    }

    public static SupplierGatewayResponse processing(
            String supplierOrderId, String responseCode, String responseMessage, String rawResponse) {
        return new SupplierGatewayResponse(
                SupplierGatewayStatus.PROCESSING, supplierOrderId, responseCode, responseMessage, rawResponse);
    }

    public static SupplierGatewayResponse failed(
            String supplierOrderId, String responseCode, String responseMessage, String rawResponse) {
        return new SupplierGatewayResponse(
                SupplierGatewayStatus.FAILED, supplierOrderId, responseCode, responseMessage, rawResponse);
    }
}
