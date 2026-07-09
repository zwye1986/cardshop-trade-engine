package com.cardshop.trade.engine.application.recharge;

/**
 * Redis 余额 reservation 释放结果。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「14.6 订单最终失败/作废退款协议」。释放成功和
 * 已释放都视为幂等成功；失败结果必须让调用方保留可靠事件重试入口，不能静默吞掉资金修复动作。
 *
 * @param released 本次是否实际完成释放
 * @param alreadyReleased 释放动作是否已由历史调用完成
 * @param failCode 失败码
 * @param failReason 失败原因
 */
public record BalanceReservationReleaseResult(
        boolean released,
        boolean alreadyReleased,
        String failCode,
        String failReason) {

    public BalanceReservationReleaseResult {
        failCode = failCode == null ? "" : failCode;
        failReason = failReason == null ? "" : failReason;
    }

    public boolean succeeded() {
        return released || alreadyReleased;
    }

    public static BalanceReservationReleaseResult releasedSuccess() {
        return new BalanceReservationReleaseResult(true, false, "", "");
    }

    public static BalanceReservationReleaseResult alreadyReleasedSuccess() {
        return new BalanceReservationReleaseResult(false, true, "", "");
    }

    public static BalanceReservationReleaseResult failed(String failCode, String failReason) {
        return new BalanceReservationReleaseResult(false, false, failCode, failReason);
    }
}
