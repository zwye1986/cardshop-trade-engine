package com.cardshop.trade.engine.application.recharge;

/**
 * Redis/MQ 订单落 MySQL 的事务写库端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」第 6-7 步。
 * 该端口的实现必须在同一个事务内写 `mc_order`、`recharge_order_ext` 以及必要的 outbox。
 */
public interface RechargeOrderPersistencePort {

    /**
     * 持久化订单聚合。
     *
     * <p>实现必须保证幂等：重复消息不能重复写 `mc_order` 或 `SUPPLIER_DISPATCH` outbox，
     * `recharge_order_ext` 的版本和状态只能前进，不能被旧消息回退。
     */
    void persist(OrderPersistAggregate aggregate);
}
