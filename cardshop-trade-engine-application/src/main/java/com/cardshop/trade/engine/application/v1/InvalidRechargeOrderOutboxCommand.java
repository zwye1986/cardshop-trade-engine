package com.cardshop.trade.engine.application.v1;

/**
 * V1 作废接口生成 INVALID_ORDER outbox 的命令。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.11 作废设计」。接口线程只写可靠事件，
 * 不直接调用供应商作废。
 *
 * @param orgCode 商户编码，对应 {@code recharge_outbox.org_code}
 * @param orderId 平台订单号，对应 {@code recharge_outbox.order_id}
 * @param outOrderSn 商户外部订单号，对应 {@code recharge_outbox.out_order_sn}
 * @param reason 作废原因，写入 {@code recharge_outbox.payload}
 */
public record InvalidRechargeOrderOutboxCommand(
        String orgCode,
        String orderId,
        String outOrderSn,
        String reason) {
}
