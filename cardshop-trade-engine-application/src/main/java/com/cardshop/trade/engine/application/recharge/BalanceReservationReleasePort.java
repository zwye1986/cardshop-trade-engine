package com.cardshop.trade.engine.application.recharge;

/**
 * Redis 余额 reservation 释放端口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「16.6 release.lua」。application 层只表达
 * “某个终态订单需要释放预扣”这一业务事实；Redis key、Lua 参数、幂等字段和异常分类由基础设施层实现。
 */
public interface BalanceReservationReleasePort {

    BalanceReservationReleaseResult release(BalanceReservationReleaseCommand command);

    /**
     * 幂等确认已成功订单的预扣已消费，只减少冻结额，不退款也不再次扣款。
     * 参见详细设计「9.2 成功预扣收尾最小修订」；未实现的端口失败关闭，让原Outbox保留重试入口。
     */
    default boolean confirm(RechargeFulfillmentOrder order) {
        return false;
    }

    static BalanceReservationReleasePort noop() {
        return new BalanceReservationReleasePort() {
            public BalanceReservationReleaseResult release(BalanceReservationReleaseCommand command) {
                return BalanceReservationReleaseResult.alreadyReleasedSuccess();
            }
            public boolean confirm(RechargeFulfillmentOrder order) { return true; }
        };
    }
}
