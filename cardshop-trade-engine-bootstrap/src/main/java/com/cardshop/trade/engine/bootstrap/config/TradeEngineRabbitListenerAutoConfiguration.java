package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessageListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * RabbitMQ 异步落库 listener 的后置自动配置。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「7. RabbitMQ 持久化消息层」和
 * 「8.3 异步落库 Consumer」。真实 dev/test/prod 环境中的 {@link RabbitTemplate}
 * 来自 Spring Boot 的 {@link RabbitAutoConfiguration}，而业务侧普通 {@code @Configuration}
 * 会早于自动配置解析；若在普通配置类上直接使用 {@code @ConditionalOnBean(RabbitTemplate.class)}，
 * 条件可能在 RabbitTemplate BeanDefinition 注册前就被判定为不满足，最终出现应用健康但 RabbitMQ
 * 管理端没有 connection/consumer 的现象。
 *
 * <p>因此 listener 的最终兜底装配放在 Boot auto-configuration 阶段，并声明在
 * {@link RabbitAutoConfiguration} 之后执行：只有 Rabbit 客户端和 application 层 Consumer 核心服务都存在时才创建
 * {@link RabbitPersistRechargeOrderMessageListener}。如果 dev/test/prod 缺少 Rabbit 自动配置或 Consumer 依赖，
 * listener 不会被静默误判为已接入，后续启动诊断和集成测试能直接定位到缺失的 Bean。
 */
@AutoConfiguration(after = {RabbitAutoConfiguration.class, TradeEngineConsumerAutoConfiguration.class})
@ConditionalOnClass({RabbitTemplate.class, RabbitPersistRechargeOrderMessageListener.class})
public class TradeEngineRabbitListenerAutoConfiguration {

    /**
     * 创建 Redis/MQ 到 MySQL 异步落库的 Rabbit listener。
     *
     * <p>这里保留 {@link ConditionalOnBean}，但它运行在 auto-configuration 阶段，已经能看到
     * {@link RabbitAutoConfiguration} 注册的 RabbitTemplate，以及 bootstrap 层装配出的
     * {@link PersistRechargeOrderConsumerService}。{@link ConditionalOnMissingBean} 用于和基础设施层的轻量 slice
     * 装配互斥，避免测试或特殊上下文显式提供 listener 时重复消费同一队列。
     */
    @Bean
    @ConditionalOnBean({RabbitTemplate.class, PersistRechargeOrderConsumerService.class})
    @ConditionalOnMissingBean(RabbitPersistRechargeOrderMessageListener.class)
    public RabbitPersistRechargeOrderMessageListener rabbitPersistRechargeOrderMessageListener(
            ObjectMapper objectMapper,
            PersistRechargeOrderConsumerService consumerService,
            RabbitTemplate rabbitTemplate,
            TradeEngineProperties properties) {
        return new RabbitPersistRechargeOrderMessageListener(
                objectMapper,
                consumerService,
                rabbitTemplate,
                properties.getMq().getPersistRetryExchange(),
                properties.getMq().getPersistRetryRoutingKey(),
                properties.getMq().getConsumerMaxRetry());
    }
}
