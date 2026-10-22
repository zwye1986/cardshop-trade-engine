package com.cardshop.trade.engine.application.recharge;

/**
 * 持久业务资格约束下的订单资金入口。
 *
 * <p>参见 docs/交行直充业务系统详细设计.md「2026-10-21 订单快照失效后的独立资金协议」。
 * 运行时实现必须先锁定MySQL订单并核对资格，再执行Redis资金操作；退款Delta与预扣标记同事务收尾。
 */
public interface RechargeOrderFundsPort {
    boolean confirm(RechargeFulfillmentOrder order);

    BalanceReservationReleaseResult release(RechargeFulfillmentOrder order, String reason);
}
