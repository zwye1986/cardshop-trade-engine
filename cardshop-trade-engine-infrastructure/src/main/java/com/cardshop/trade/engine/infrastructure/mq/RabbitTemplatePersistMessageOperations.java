package com.cardshop.trade.engine.infrastructure.mq;

import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * 基于 Spring AMQP RabbitTemplate 的持久化消息发布操作。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「10. RabbitMQ 设计落地」。本类不解释业务结果，
 * 只把 exchange、routingKey、payload 与 correlationData 交给 RabbitTemplate，confirm 由上层发布器等待。
 */
public class RabbitTemplatePersistMessageOperations implements RabbitPersistMessageOperations {

    private final RabbitTemplate rabbitTemplate;

    public RabbitTemplatePersistMessageOperations(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        /*
         * 参见 docs/交行直充业务系统详细设计.md「20.9 RabbitMQ 声明契约」。mandatory return 是确认
         * exchange 存在但 routingKey 不可路由的唯一同步证据；不开 mandatory 时，broker 可能 ack publish，
         * 但消息实际没有进入持久化队列。
         */
        this.rabbitTemplate.setMandatory(true);
    }

    @Override
    public void convertAndSend(String exchange, String routingKey, String payload, CorrelationData correlationData) {
        rabbitTemplate.convertAndSend(exchange, routingKey, payload, correlationData);
    }
}
