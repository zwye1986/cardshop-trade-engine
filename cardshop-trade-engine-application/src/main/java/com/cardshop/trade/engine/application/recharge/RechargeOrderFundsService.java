package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
import java.util.Objects;

/**
 * 消费确认与退款副作用的统一编排，不依赖Redis业务快照。
 *
 * <p>参见 docs/交行直充业务系统详细设计.md「2026-10-21 订单快照失效后的独立资金协议」。
 * 本类不创建数据库事务；运行时由持久层资金入口在同订单锁及短事务中调用。
 * Worker旧构造入口仅为独立单元测试保留直接组合，真实Bean必须注入事务资金端口。
 */
public final class RechargeOrderFundsService implements RechargeOrderFundsPort {
    public static final String RELEASE_EVENT_PREFIX = "balance-flush-release-";
    private static final String RELEASE_OPERATION = "RELEASE";
    private static final String FUNDS_RESERVED = "RESERVED";
    private static final String FUNDS_CONSUMED = "CONSUMED";
    private final BalanceReservationReleasePort redis;
    private final RechargeFulfillmentOrderPort orders;
    private final BalanceDeltaFlushOutboxPort deltas;

    public RechargeOrderFundsService(BalanceReservationReleasePort redis,
            RechargeFulfillmentOrderPort orders, BalanceDeltaFlushOutboxPort deltas) {
        this.redis = Objects.requireNonNull(redis);
        this.orders = Objects.requireNonNull(orders);
        this.deltas = Objects.requireNonNull(deltas);
    }

    @Override
    public boolean confirm(RechargeFulfillmentOrder order) {
        return redis.confirm(order);
    }

    @Override
    public BalanceReservationReleaseResult release(RechargeFulfillmentOrder order, String reason) {
        return release(order, reason, false);
    }

    /** 已有MySQL退款Delta时只允许Redis确认已退款，不能再增加一次余额。 */
    public BalanceReservationReleaseResult release(
            RechargeFulfillmentOrder order, String reason, boolean alreadyReleasedOnly) {
        String expectedFunds = order.orderStatus() == LegacyOrderStatusCode.SUCCESS.code()
                ? FUNDS_CONSUMED : FUNDS_RESERVED;
        var result = redis.release(new BalanceReservationReleaseCommand(
                order.orgCode(), order.orderId(), order.outOrderSn(), order.orderPrice(),
                RELEASE_OPERATION, RELEASE_EVENT_PREFIX + order.orderId(), reason == null ? "" : reason,
                expectedFunds, alreadyReleasedOnly));
        if (result.succeeded()) {
            deltas.enqueueBalanceDeltaFlush(new BalanceDeltaFlushOutboxCommand(
                    order.orgCode(), order.orderId(), order.outOrderSn(), order.orderPrice(),
                    RELEASE_OPERATION, reason == null ? "" : reason));
            orders.markBalanceReleased(order.orderId());
        }
        return result;
    }
}
