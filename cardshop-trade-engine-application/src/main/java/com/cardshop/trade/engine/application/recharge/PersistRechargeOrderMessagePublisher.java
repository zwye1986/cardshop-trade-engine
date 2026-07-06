package com.cardshop.trade.engine.application.recharge;

import java.util.concurrent.CompletionStage;

/**
 * Redis 订单持久化消息发布端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.5 下单服务实现细节」第 11-14 步。
 * 应用层只关心 RabbitMQ publisher confirm 结果；exchange、routingKey、CorrelationData、
 * mandatory return 和重试细节由 infrastructure 适配器负责。
 */
public interface PersistRechargeOrderMessagePublisher {

    /**
     * MQ 发布链路当前是否可用；不可用时必须在 Redis accept Lua 前 fail closed。
     */
    boolean isAvailable();

    /**
     * 发布持久化消息并等待 publisher confirm。
     */
    CompletionStage<PersistMessagePublishResult> publish(PersistTradeOrderMessage message);
}
