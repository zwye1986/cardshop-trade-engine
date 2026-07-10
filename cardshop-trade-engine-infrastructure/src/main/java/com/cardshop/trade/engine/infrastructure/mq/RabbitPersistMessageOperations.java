package com.cardshop.trade.engine.infrastructure.mq;

import org.springframework.amqp.rabbit.connection.CorrelationData;

/**
 * RabbitTemplate 的极薄发布抽象。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「7. RabbitMQ 持久化消息层」。该接口只为单元测试替换
 * RabbitTemplate，生产语义仍由 {@link CorrelationData#getFuture()} 的 publisher confirm 决定。
 */
public interface RabbitPersistMessageOperations {

    /**
     * 发布一条 Redis 订单异步落库消息。
     */
    void convertAndSend(String exchange, String routingKey, String payload, CorrelationData correlationData);
}
