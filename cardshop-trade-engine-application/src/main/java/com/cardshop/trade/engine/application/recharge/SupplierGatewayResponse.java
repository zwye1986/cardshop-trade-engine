package com.cardshop.trade.engine.application.recharge;

/**
 * 供应商 Gateway 归一化响应。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」供应商派单幂等要求。
 *
 * @param status 归一化后的供应商处理状态，只能是成功、处理中、明确失败或可重试失败
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

    /**
     * 供应商调用发生可重试失败。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。HTTP 5xx、超时、
     * 连接异常只能说明本次调用窗口不可确认，不能等同于供应商明确失败；Worker 收到该状态时必须保留
     * outbox 重试，不释放余额、不创建终态回调，避免把临时网络抖动放大为业务失败。
     */
    public static SupplierGatewayResponse retryableFailed(
            String supplierOrderId, String responseCode, String responseMessage, String rawResponse) {
        return new SupplierGatewayResponse(
                SupplierGatewayStatus.RETRYABLE_FAILED, supplierOrderId, responseCode, responseMessage, rawResponse);
    }
}
