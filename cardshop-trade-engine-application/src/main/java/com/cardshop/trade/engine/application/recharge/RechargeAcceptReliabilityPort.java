package com.cardshop.trade.engine.application.recharge;

import java.util.concurrent.CompletionStage;

/**
 * Redis 接单后的可靠性确认端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.5 下单服务实现细节」。
 * accept Lua 只负责原子写 Redis 订单事实；接口返回前还必须完成 WAIT、WAIT 失败回滚、
 * MQ confirm 成功后的 Redis 状态推进。该端口把 Redis 命令、Lua 脚本和拓扑细节隔离在 infrastructure。
 */
public interface RechargeAcceptReliabilityPort {

    /**
     * 执行 Redis WAIT，确认本次接单写入已被足够副本观察到。
     */
    CompletionStage<ReplicationWaitResult> waitForReplication(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult);

    /**
     * WAIT 未达标时回滚未确认受理。
     */
    CompletionStage<CancelUnconfirmedAcceptResult> cancelUnconfirmedAccept(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            String reason);

    /**
     * RabbitMQ confirm ack 后推进 Redis 持久化状态。
     */
    CompletionStage<MarkPersistQueuedResult> markPersistQueued(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            String persistMessageId,
            int publishCount);

    /**
     * MySQL 事务提交成功后，标记 Redis 订单已完成持久化。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。该方法必须同时清理
     * pending 和 org_index，避免补投任务重复投递已落库订单。
     */
    CompletionStage<MarkPersistStateResult> markDbPersisted(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            long nowEpochMillis);

    /**
     * MySQL 事务或 Consumer 处理失败后，刷新 Redis 中的失败原因和下次重试时间。
     *
     * <p>失败状态不能删除 pending；否则 Redis 已受理但未落库的订单会失去补偿入口。
     */
    CompletionStage<MarkPersistStateResult> markPersistFailed(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            String failCode,
            String failReason,
            int retryCount,
            long nextRetryEpochMillis,
            long nowEpochMillis);
}
