package com.cardshop.trade.engine.application.recharge;

import java.util.concurrent.CompletionStage;

/**
 * Redis 接单后的可靠性确认端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「4. Application 用例编排层」。
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
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」和
     * {@code docs/交行直充业务系统高可用方案设计.md}「14.2 Redis 与 MySQL 版本博弈」。
     * `persistedVersion` 是本次已成功写入 MySQL 的 Redis `order_version`：Redis 当前版本没有更新时才清理
     * pending 和 org_index；如果 Redis 已有更高版本，本方法只能推进 `persisted_version` 并保留补刷入口。
     */
    CompletionStage<MarkPersistStateResult> markDbPersisted(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            long persistedVersion,
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
