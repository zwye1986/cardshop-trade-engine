package com.cardshop.trade.engine.application.recharge;

import java.util.Map;

/**
 * 供应商异步查单请求。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「17.1 任务清单」。查询任务必须携带稳定的
 * outbox 事件号和订单定位字段，供应商适配器只负责把这些字段转换成渠道协议。
 *
 * @param supplierRequestId 查单请求幂等号，固定取 {@code SUPPLIER_QUERY} outbox event_id
 * @param orderId 平台订单号，对应旧表 {@code mc_order.order_Id}
 * @param outOrderSn 商户外部订单号，对应旧表 {@code mc_order.out_orderId}
 * @param orgCode 商户编码，对应旧表 {@code mc_order.org_code}
 * @param account 直充账号，对应旧表 {@code mc_order.account}
 * @param supplierChannel 供应商渠道编码，对应旧表 {@code mc_order.url}
 * @param rawPayload 查单 outbox 原始载荷展开后的补充字段
 */
public record SupplierQueryGatewayRequest(
        String supplierRequestId,
        String orderId,
        String outOrderSn,
        String orgCode,
        String account,
        String supplierChannel,
        Map<String, Object> rawPayload) {
}
