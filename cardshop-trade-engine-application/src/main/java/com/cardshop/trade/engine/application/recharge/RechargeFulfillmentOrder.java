package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
import com.cardshop.trade.engine.domain.order.PersistStatus;
import com.cardshop.trade.engine.domain.order.ValidationStatus;

/**
 * 供应商履约所需的订单事实快照。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」。Worker 只读取
 * 派单、作废、回调必要字段，不把旧表全部字段暴露给 application 层。
 *
 * @param orderId 平台订单号，对应旧表 {@code mc_order.order_Id}
 * @param outOrderSn 商户外部订单号，对应旧表 {@code mc_order.out_orderId}
 * @param orgCode 商户编码，对应旧表 {@code mc_order.org_code}
 * @param account 直充账号，对应旧表 {@code mc_order.account}
 * @param productId 外部请求中的父商品编码，对应旧表 {@code mc_order.parent_id}
 * @param selectedProductId 实际履约子商品编码，对应旧表 {@code mc_order.produt_code}
 * @param supplierChannel 供应商渠道编码，对应旧表 {@code mc_order.url}
 * @param orderPrice 订单金额，单位分，对应旧表 {@code mc_order.order_price}
 * @param notifyUrl 商户回调地址，对应旧表 {@code mc_order.callback_url}
 * @param orderStatus 旧接口数字订单状态，对应旧表 {@code mc_order.order_status}
 * @param persistStatus Redis 到 MySQL 落库状态，对应 {@code recharge_order_ext.persist_status}
 * @param validationStatus 业务校验状态，对应 {@code recharge_order_ext.validation_status}
 * @param dispatchAllowed 是否允许供应商派单，对应 {@code recharge_order_ext.dispatch_allowed}
 * @param balanceReserved 是否完成 Redis 预扣，对应 {@code recharge_order_ext.balance_reserved}
 */
public record RechargeFulfillmentOrder(
        String orderId,
        String outOrderSn,
        String orgCode,
        String account,
        String productId,
        String selectedProductId,
        String supplierChannel,
        long orderPrice,
        String notifyUrl,
        int orderStatus,
        String persistStatus,
        String validationStatus,
        boolean dispatchAllowed,
        boolean balanceReserved) {

    public boolean dispatchable() {
        return dispatchAllowed
                && orderStatus == LegacyOrderStatusCode.ACCEPTED.code()
                && PersistStatus.DB_PERSISTED.name().equals(persistStatus)
                && ValidationStatus.PASSED.name().equals(validationStatus);
    }

    /**
     * 判断供应商失败/作废成功后的 outbox 重试是否仍需继续释放 Redis 预扣。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「14.6 订单最终失败/作废退款协议」。订单可能已在上一次
     * Worker 执行中写成失败终态，但 Redis release 或反向刷账 outbox 失败；此时再次抢占同一 outbox 不能因为
     * 订单“不再可派单”直接标成功，否则资金释放入口会丢失。
     */
    public boolean terminalFailureAwaitingBalanceRelease() {
        return balanceReserved
                && PersistStatus.DB_PERSISTED.name().equals(persistStatus)
                && (orderStatus == LegacyOrderStatusCode.FAILED.code()
                || orderStatus == LegacyOrderStatusCode.REFUNDED.code());
    }
}
