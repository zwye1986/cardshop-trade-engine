package com.cardshop.trade.engine.application.recharge;

/**
 * Redis 余额 reservation 释放端口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「16.6 release.lua」。application 层只表达
 * “某个终态订单需要释放预扣”这一业务事实；Redis key、Lua 参数、幂等字段和异常分类由基础设施层实现。
 */
public interface BalanceReservationReleasePort {

    BalanceReservationReleaseResult release(BalanceReservationReleaseCommand command);

    static BalanceReservationReleasePort noop() {
        return command -> BalanceReservationReleaseResult.alreadyReleasedSuccess();
    }
}
