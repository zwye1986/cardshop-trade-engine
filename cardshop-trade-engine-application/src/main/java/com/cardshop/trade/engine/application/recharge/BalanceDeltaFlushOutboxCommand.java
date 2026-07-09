package com.cardshop.trade.engine.application.recharge;

/**
 * 余额 delta 刷账 outbox 写入命令。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.2 作废设计」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「14.7 `mc_ext_depart.balance` 刷账协议」。供应商失败、
 * 作废成功或退款完成后，必须生成正向 delta 事件，把 Redis 已释放的预扣金额同步沉淀回旧表最终余额。
 *
 * @param orgCode 商户编码，对应 {@code recharge_outbox.org_code}
 * @param orderId 平台订单号，对应 {@code recharge_outbox.order_id}
 * @param outOrderSn 商户订单号，对应 {@code recharge_outbox.out_order_sn}
 * @param deltaAmount 本次最终余额增量，释放/退款为正数，预扣沉淀为负数
 * @param deltaType 余额变动类型，例如 {@code RELEASE}、{@code REFUND}
 * @param reason 生成反向余额事件的业务原因
 */
public record BalanceDeltaFlushOutboxCommand(
        String orgCode,
        String orderId,
        String outOrderSn,
        long deltaAmount,
        String deltaType,
        String reason) {
}
