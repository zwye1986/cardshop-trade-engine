package com.cardshop.trade.engine.application.recharge;

/**
 * 持久订单事实身份冲突，调用方不得将其解释为订单不存在或自动认领旧事实。
 * 参见 {@code docs/交行直充业务系统详细设计.md}「8.2.1」。
 */
public class RechargeOrderIdentityConflictException extends IllegalStateException {
    public RechargeOrderIdentityConflictException(String message) {
        super(message);
    }
}
