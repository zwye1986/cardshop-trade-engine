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
}
