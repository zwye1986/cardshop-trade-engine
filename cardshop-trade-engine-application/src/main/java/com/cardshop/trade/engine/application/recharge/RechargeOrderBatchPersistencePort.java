package com.cardshop.trade.engine.application.recharge;

import java.util.List;

/**
 * Redis/MQ 订单批量落 MySQL 的事务写库端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。批端口只服务 RabbitMQ
 * 批消费灰度路径：同一批订单共享一个 MySQL 事务，但每笔订单仍写入与单条路径相同的订单事实、扩展状态、资金流水和 outbox。
 */
public interface RechargeOrderBatchPersistencePort {

    /**
     * 在同一个 MySQL 事务内批量持久化订单聚合。
     *
     * <p>实现必须保证整批原子提交；任一订单写库失败时抛出异常并回滚整批事务。重复消息、重复补投和批次重试
     * 必须依赖数据库唯一键与 upsert 语义保持幂等，不能在批路径中依赖 MyBatis BATCH 的即时 row count 做业务分支。
     */
    void persistBatch(List<OrderPersistAggregate> aggregates);
}
