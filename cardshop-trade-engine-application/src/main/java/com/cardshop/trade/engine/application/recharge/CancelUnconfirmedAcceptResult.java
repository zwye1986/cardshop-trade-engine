package com.cardshop.trade.engine.application.recharge;

/**
 * WAIT 失败后的 Redis 未确认受理回滚结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「4. Application 用例编排层」。
 * 当 accept Lua 已执行但 WAIT 未达到副本确认要求时，只能通过 Redis Lua 原子释放余额和清理 pending；
 * 如果回滚也失败，上层不得本地释放余额，必须进入“稍后查单/异常核查”语义。
 *
 * @param rollbackConfirmed 是否确认完成回滚
 * @param failCode 回滚失败码
 * @param failReason 回滚失败原因
 */
public record CancelUnconfirmedAcceptResult(boolean rollbackConfirmed, String failCode, String failReason) {

    public static final String FAIL_CODE_REDIS_SESSION_UNAVAILABLE = "REDIS_SESSION_UNAVAILABLE";
    public static final String FAIL_CODE_CANCEL_LUA_ERROR = "CANCEL_LUA_ERROR";

    public CancelUnconfirmedAcceptResult {
        failCode = failCode == null ? "" : failCode;
        failReason = failReason == null ? "" : failReason;
    }

    public static CancelUnconfirmedAcceptResult success() {
        return new CancelUnconfirmedAcceptResult(true, "", "");
    }

    public static CancelUnconfirmedAcceptResult failed(String failCode, String failReason) {
        return new CancelUnconfirmedAcceptResult(false, failCode, failReason);
    }
}
