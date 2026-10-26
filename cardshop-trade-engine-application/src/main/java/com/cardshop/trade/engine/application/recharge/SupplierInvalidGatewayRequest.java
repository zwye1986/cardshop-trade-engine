package com.cardshop.trade.engine.application.recharge;

import java.util.Map;

/**
 * 供应商作废请求。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 作废设计」与「9.1 派单 Outbox」。
 * 作废通过 {@code recharge_dispatch_outbox(INVALID_ORDER)} 的 {@code event_id} 做内部幂等号，
 * 不在 V1 接口线程内同步调用供应商。
 *
 * @param supplierRequestId 供应商作废幂等号，固定取 {@code recharge_dispatch_outbox.event_id}
 * @param orderId 平台订单号，对应旧表 {@code mc_order.order_Id}
 * @param outOrderSn 商户外部订单号，对应旧表 {@code mc_order.out_orderId}
 * @param orgCode 商户编码，对应旧表 {@code mc_order.org_code}
 * @param account 直充账号，对应旧表 {@code mc_order.account}
 * @param supplierChannel 供应商渠道编码，对应旧表 {@code mc_order.url}
 * @param rawPayload 作废 outbox 原始载荷展开后的补充字段，便于渠道适配器扩展私有参数
 * @param queryOnly 已有持久作废尝试时只查证原订单，不再次发送作废；由Worker决定，不信任外部载荷
 */
public record SupplierInvalidGatewayRequest(
        String supplierRequestId,
        String orderId,
        String outOrderSn,
        String orgCode,
        String account,
        String supplierChannel,
        Map<String, Object> rawPayload,
        boolean queryOnly) {

    /** 兼容其他渠道的原始请求；MINISO恢复必须显式传queryOnly，参见详细设计2026-10-26修订。 */
    public SupplierInvalidGatewayRequest(String supplierRequestId, String orderId, String outOrderSn,
            String orgCode, String account, String supplierChannel, Map<String, Object> rawPayload) {
        this(supplierRequestId, orderId, outOrderSn, orgCode, account, supplierChannel, rawPayload, false);
    }
}
