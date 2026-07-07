package com.cardshop.trade.engine.application.recharge;

/**
 * 异步落库 Consumer 单条消息处理状态。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。
 * 该状态用于后续 RabbitMQ adapter 判断 ack/nack：MySQL 已提交的结果不应再重复消费，
 * MySQL 未提交的结果应保持可重试。
 */
public enum PersistRechargeOrderStatus {

    /**
     * MySQL 事务提交成功，且 Redis `markDbPersisted` 已确认。
     */
    PERSISTED,

    /**
     * MySQL 事务已提交，但 Redis 标记失败。后续应依赖补偿任务清理 pending，不能重复插入 MySQL。
     */
    PERSISTED_REDIS_MARK_FAILED,

    /**
     * MySQL 事务未成功提交，消息仍需要重试或进入 DLQ/补偿。
     */
    PERSIST_FAILED
}
