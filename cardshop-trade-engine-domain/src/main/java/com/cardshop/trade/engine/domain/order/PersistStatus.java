package com.cardshop.trade.engine.domain.order;

/**
 * Redis、RabbitMQ、MySQL 之间的订单持久化状态。
 *
 * <p>状态语义对应 {@code docs/交行直充业务系统详细设计.md} 的「5. Domain 状态与业务规则」。
 * 该枚举只描述订单事实从 Redis 受理到 MySQL 落库的技术状态，不承载供应商派单、回调或作废等业务履约状态。
 */
public enum PersistStatus {

    /**
     * Redis 已受理订单，但 RabbitMQ 落库消息尚未可靠确认。
     */
    PENDING_MQ(false),

    /**
     * RabbitMQ 已确认接收落库消息，等待 Consumer 写入 MySQL。
     */
    PERSIST_QUEUED(false),

    /**
     * Consumer 或补偿任务正在写入 MySQL。
     */
    PERSISTING(false),

    /**
     * MySQL 已持久化到当前可见版本。
     */
    DB_PERSISTED(true),

    /**
     * 最近一次 MQ 消费或 MySQL 持久化失败，仍允许后台补偿重试。
     */
    PERSIST_FAILED(false),

    /**
     * 自动补偿无法安全判定结果，需要人工核查。
     */
    MANUAL_REQUIRED(true);

    private final boolean terminal;

    PersistStatus(boolean terminal) {
        this.terminal = terminal;
    }

    /**
     * 判断状态是否已经进入自动持久化链路的终止态。
     */
    public boolean isTerminal() {
        return terminal;
    }
}
