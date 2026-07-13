package com.cardshop.trade.engine.application.recharge;

import java.util.Map;

/**
 * 供应商派单请求。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」供应商派单幂等协议。
 * {@code supplierRequestId} 固定使用 outbox event_id，便于宕机恢复后按同一幂等号查证。
 *
 * @param supplierRequestId 供应商请求幂等号，固定取 {@code recharge_outbox.event_id}
 * @param orderId 平台订单号，对应旧表 {@code mc_order.order_Id}
 * @param outOrderSn 商户外部订单号，对应旧表 {@code mc_order.out_orderId}
 * @param orgCode 商户编码，对应旧表 {@code mc_order.org_code}
 * @param account 直充账号，对应旧表 {@code mc_order.account}
 * @param productId 外部请求 {@code product_id}，对应旧表 {@code mc_order.parent_id}
 * @param selectedProductId 实际履约商品编码，对应旧表 {@code mc_order.produt_code}；当前 V1 语义下与 {@code productId} 相同
 * @param supplierChannel 供应商渠道编码，对应旧表 {@code mc_order.url}
 * @param orderPrice 订单金额，单位分，对应旧表 {@code mc_order.order_price}
 * @param rawPayload outbox 原始载荷展开后的补充字段，便于渠道适配器扩展私有参数
 */
public record SupplierDispatchGatewayRequest(
        String supplierRequestId,
        String orderId,
        String outOrderSn,
        String orgCode,
        String account,
        String productId,
        String selectedProductId,
        String supplierChannel,
        long orderPrice,
        Map<String, Object> rawPayload) {
}
