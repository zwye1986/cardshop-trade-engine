package com.cardshop.trade.engine.application.recharge;

import java.util.List;

/**
 * Redis/MQ 订单批量落 MySQL 的事务写库端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。批端口只服务 RabbitMQ
 * 批消费灰度路径，提供两种事务边界：
 *
 * <ul>
 *   <li>{@link #persistBatchWithOutbox(List)}：默认五表单事务（订单事实、扩展状态、资金流水 + 两张 outbox），
 *   对应 outbox 同步落库模式；
 *   <li>{@link #persistBatchFactsOnly(List)}：outbox 异步落库模式（
 *   {@code trade.engine.persist.outbox-async-enabled=true}）下的第一段事实事务，只写三张事实表，
 *   两张 outbox 由 {@link RechargePersistOutboxAsyncWriter} 在事实提交后独立事务补齐。
 * </ul>
 */
public interface RechargeOrderBatchPersistencePort {

    /**
     * 在同一个 MySQL 事务内批量持久化订单聚合（含两张 outbox 表）。
     *
     * <p>实现必须保证整批原子提交；任一订单写库失败时抛出异常并回滚整批事务。重复消息、重复补投和批次重试
     * 必须依赖数据库唯一键与 upsert 语义保持幂等，不能在批路径中依赖 MyBatis BATCH 的即时 row count 做业务分支。
     */
    void persistBatchWithOutbox(List<OrderPersistAggregate> aggregates);

    /**
     * 在同一个 MySQL 事务内只批量持久化订单事实表（{@code mc_order} / {@code recharge_order_ext} /
     * {@code mc_depart_record}），不写任何 outbox 表。
     *
     * <p>仅供 outbox 异步落库模式使用：两张 outbox 必须由 {@link RechargePersistOutboxAsyncWriter}
     * 在本事务提交成功之后、MQ ack 之前补齐，语义同样要求整批原子与唯一键幂等。
     * 参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer · Outbox 异步落库模式」。
     */
    void persistBatchFactsOnly(List<OrderPersistAggregate> aggregates);
}
