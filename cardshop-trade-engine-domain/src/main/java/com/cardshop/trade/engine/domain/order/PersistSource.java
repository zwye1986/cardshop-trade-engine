package com.cardshop.trade.engine.domain.order;

/**
 * MySQL 落库使用的订单快照来源。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「8.3 异步落库 Consumer」。
 * 该字段用于排查 Redis 快照缺失、MQ 初始快照兜底等高可用窗口。
 */
public enum PersistSource {

    /**
     * Consumer 读取 Redis 最新订单快照后落库。
     */
    REDIS_SNAPSHOT,

    /**
     * Redis 快照缺失或不可读时，使用 MQ 消息携带的初始快照兜底落库。
     */
    MQ_SNAPSHOT_FALLBACK
}
