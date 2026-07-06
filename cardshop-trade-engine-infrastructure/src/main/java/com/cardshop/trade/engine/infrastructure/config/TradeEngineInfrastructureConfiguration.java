package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecordPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.V1TokenStorePort;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.infrastructure.mq.RabbitTemplatePersistMessageOperations;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisAcceptAnomalyRecordAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.cardshop.trade.engine.infrastructure.redis.LettuceRechargeRedisSessionFactory;
import com.cardshop.trade.engine.infrastructure.redis.RedisAcceptRechargeOrderAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisSessionAcceptReliabilityAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisV1TokenStoreAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptLoader;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptExecutor;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisSessionFactory;
import com.cardshop.trade.engine.infrastructure.redis.SpringDataRechargeRedisScriptExecutor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 基础设施层自动配置入口。
 *
 * <p>本配置类只启用 {@link TradeEngineProperties} 的绑定，对应
 * {@code docs/交行直充业务系统详细设计.md} 的「6.4 Infrastructure 层」和「7.4 核心配置项」。
 * D4 阶段开始声明 Redis 接单所需的 Key 构造、Lua 脚本和适配器；真实 Redis 客户端仍由 Spring Boot
 * 自动配置和 Nacos 环境提供，local profile 禁用 Redis 时不会强制创建接单端口。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TradeEngineProperties.class)
public class TradeEngineInfrastructureConfiguration {

    @Bean
    public RechargeRedisKeyBuilder rechargeRedisKeyBuilder(TradeEngineProperties properties) {
        return new RechargeRedisKeyBuilder(
                properties.getRedis().getKeyPrefix(),
                properties.getRedis().getPendingOrgBucketCount());
    }

    @Bean
    public RechargeRedisScriptLoader rechargeRedisScriptLoader() {
        return new RechargeRedisScriptLoader();
    }

    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    public RechargeRedisScriptExecutor rechargeRedisScriptExecutor(ReactiveStringRedisTemplate redisTemplate) {
        return new SpringDataRechargeRedisScriptExecutor(redisTemplate);
    }

    @Bean
    @ConditionalOnBean(LettuceConnectionFactory.class)
    public RechargeRedisSessionFactory rechargeRedisSessionFactory(LettuceConnectionFactory connectionFactory) {
        return new LettuceRechargeRedisSessionFactory(connectionFactory);
    }

    @Bean
    @ConditionalOnBean(RechargeRedisSessionFactory.class)
    public RedisSessionAcceptReliabilityAdapter redisSessionAcceptReliabilityAdapter(
            RechargeRedisSessionFactory sessionFactory,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            TradeEngineProperties properties) {
        return new RedisSessionAcceptReliabilityAdapter(sessionFactory, keyBuilder, scriptLoader, properties);
    }

    @Bean
    @ConditionalOnBean(RechargeRedisScriptExecutor.class)
    @ConditionalOnMissingBean(AcceptRechargeOrderPort.class)
    public AcceptRechargeOrderPort acceptRechargeOrderPort(
            RechargeRedisScriptExecutor scriptExecutor,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            TradeEngineProperties properties) {
        return new RedisAcceptRechargeOrderAdapter(scriptExecutor, keyBuilder, scriptLoader, properties);
    }

    @Bean
    @ConditionalOnBean(RabbitTemplate.class)
    public PersistRechargeOrderMessagePublisher persistRechargeOrderMessagePublisher(
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper,
            TradeEngineProperties properties) {
        return new RabbitPersistRechargeOrderMessagePublisher(
                new RabbitTemplatePersistMessageOperations(rabbitTemplate),
                objectMapper,
                properties,
                Clock.systemUTC());
    }

    /**
     * 装配接单异常登记端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
     * Mapper 存在才说明 D2 的 anomaly 表访问能力已就绪；否则 API 层会使用 noop 端口保守跳过登记。
     */
    @Bean
    @ConditionalOnBean(RechargeAcceptAnomalyMapper.class)
    public AcceptAnomalyRecordPort acceptAnomalyRecordPort(
            RechargeAcceptAnomalyMapper mapper,
            ObjectMapper objectMapper) {
        return new MybatisAcceptAnomalyRecordAdapter(mapper, objectMapper, Clock.systemUTC());
    }

    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    public V1TokenStorePort v1TokenStorePort(
            ReactiveStringRedisTemplate redisTemplate,
            TradeEngineProperties properties) {
        return new RedisV1TokenStoreAdapter(redisTemplate, properties);
    }

    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    public RechargeOrderQueryPort rechargeOrderQueryPort(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties) {
        int timeoutMs = Math.max(100, properties.getRedis().getWaitTimeoutMs() * 5);
        return new RedisRechargeOrderQueryAdapter(
                redisTemplate,
                keyBuilder,
                java.time.Duration.ofMillis(timeoutMs));
    }
}
