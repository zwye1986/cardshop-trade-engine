package com.cardshop.trade.engine.application.recharge;

/**
 * Redis 余额预扣释放命令。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「14.6 订单最终失败/作废退款协议」
 * 和「16.6 release.lua」。供应商最终失败、作废成功或退款完成后，必须通过 {@code releaseEventId}
 * 做幂等释放，避免 outbox 重试时重复回补 Redis 可用余额。
 *
 * @param orgCode 商户编码
 * @param orderId 平台订单号
 * @param outOrderSn 商户订单号
 * @param amount 本次释放金额，单位分
 * @param releaseType 释放类型，例如 {@code RELEASE}、{@code REFUND}
 * @param releaseEventId 按订单固定的资金操作ID，用于Redis幂等
 * @param reason 释放原因
 * @param expectedFundsStatus 经MySQL业务资格确定的退款来源资金状态
 * @param alreadyReleasedOnly 是否仅允许确认既有退款，防止已有Delta时再次增加余额
 */
public record BalanceReservationReleaseCommand(
        String orgCode,
        String orderId,
        String outOrderSn,
        long amount,
        String releaseType,
        String releaseEventId,
        String reason,
        String expectedFundsStatus,
        boolean alreadyReleasedOnly) {
    /** 低层协议测试及消费确认兼容入口；运行时退款由独立资金服务明确传入资格。 */
    public BalanceReservationReleaseCommand(String orgCode, String orderId, String outOrderSn,
            long amount, String releaseType, String releaseEventId, String reason) {
        this(orgCode, orderId, outOrderSn, amount, releaseType, releaseEventId, reason, "", false);
    }
}
