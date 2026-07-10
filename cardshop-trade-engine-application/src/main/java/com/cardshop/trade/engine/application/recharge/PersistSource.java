package com.cardshop.trade.engine.application.recharge;

/**
 * MySQL 落库使用的订单快照来源。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
 * 该枚举会原样写入 `recharge_order_ext.persist_source`，用于排查 Redis 快照缺失时是否发生过
 * MQ initial snapshot 兜底落库。
 */
public enum PersistSource {

    /**
     * Consumer 成功读取 Redis 最新订单 hash，并以 Redis 快照作为 MySQL 事实来源。
     */
    REDIS_SNAPSHOT,

    /**
     * Redis 快照缺失或不可用，Consumer 使用 MQ 消息携带的 initial snapshot 兜底落库。
     */
    MQ_SNAPSHOT_FALLBACK
}
