package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecordPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshotPort;
import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.V1TokenStorePort;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.infrastructure.mq.RabbitTemplatePersistMessageOperations;
import com.cardshop.trade.engine.infrastructure.persistence.FallbackRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisAcceptAnomalyCheckRepositoryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisAcceptAnomalyRecordAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.cardshop.trade.engine.infrastructure.redis.LettuceRechargeRedisSessionFactory;
import com.cardshop.trade.engine.infrastructure.redis.InMemoryRedisHaGuard;
import com.cardshop.trade.engine.infrastructure.redis.RedisPersistOrderSnapshotAdapter;
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
import org.springframework.beans.factory.ObjectProvider;

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
    @ConditionalOnMissingBean(RedisHaGuardPort.class)
    public RedisHaGuardPort redisHaGuardPort(TradeEngineProperties properties) {
        return new InMemoryRedisHaGuard(properties, Clock.systemUTC());
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
            TradeEngineProperties properties,
            RedisHaGuardPort redisHaGuardPort) {
        return new RedisSessionAcceptReliabilityAdapter(sessionFactory, keyBuilder, scriptLoader, properties, redisHaGuardPort);
    }

    @Bean
    @ConditionalOnBean(RechargeRedisScriptExecutor.class)
    @ConditionalOnMissingBean(AcceptRechargeOrderPort.class)
    public AcceptRechargeOrderPort acceptRechargeOrderPort(
            RechargeRedisScriptExecutor scriptExecutor,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            TradeEngineProperties properties,
            RedisHaGuardPort redisHaGuardPort) {
        return new RedisAcceptRechargeOrderAdapter(scriptExecutor, keyBuilder, scriptLoader, properties, redisHaGuardPort);
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

    /**
     * 装配接单异常自动核查仓储端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 Redis HA Guard」和
     * 「20.13.6 Redis Lua 下单结果」。该 Bean 只提供异常表读取与状态推进，自动核查服务由 bootstrap 层组装。
     */
    @Bean
    @ConditionalOnBean(RechargeAcceptAnomalyMapper.class)
    public AcceptAnomalyCheckRepositoryPort acceptAnomalyCheckRepositoryPort(
            RechargeAcceptAnomalyMapper mapper,
            ObjectMapper objectMapper) {
        return new MybatisAcceptAnomalyCheckRepositoryAdapter(mapper, objectMapper, Clock.systemUTC());
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
            TradeEngineProperties properties,
            ObjectProvider<McOrderMapper> orderMapperProvider,
            ObjectProvider<RechargeOrderExtMapper> extMapperProvider) {
        int timeoutMs = Math.max(100, properties.getRedis().getWaitTimeoutMs() * 5);
        RechargeOrderQueryPort redisQueryPort = new RedisRechargeOrderQueryAdapter(
                redisTemplate,
                keyBuilder,
                java.time.Duration.ofMillis(timeoutMs));
        McOrderMapper orderMapper = orderMapperProvider.getIfAvailable();
        RechargeOrderExtMapper extMapper = extMapperProvider.getIfAvailable();
        if (orderMapper == null || extMapper == null) {
            return redisQueryPort;
        }
        return new FallbackRechargeOrderQueryAdapter(
                redisQueryPort,
                new MybatisRechargeOrderQueryAdapter(orderMapper, extMapper));
    }

    /**
     * 装配 Persist Consumer 使用的 Redis 完整订单快照端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。该端口读取完整
     * Redis hash，不复用查单端口，避免查单 DTO 字段不足导致 MySQL 兜底落库信息缺失。
     */
    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    @ConditionalOnMissingBean(PersistOrderSnapshotPort.class)
    public PersistOrderSnapshotPort persistOrderSnapshotPort(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties) {
        int timeoutMs = Math.max(100, properties.getRedis().getWaitTimeoutMs() * 5);
        return new RedisPersistOrderSnapshotAdapter(
                redisTemplate,
                keyBuilder,
                java.time.Duration.ofMillis(timeoutMs));
    }

    /**
     * 装配 MySQL 异步落库端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。
     * 三个 Mapper 同时存在时才启用，确保事务内能完整写 `mc_order`、`recharge_order_ext` 和 outbox。
     */
    @Bean
    @ConditionalOnBean({McOrderMapper.class, RechargeOrderExtMapper.class, RechargeOutboxMapper.class})
    @ConditionalOnMissingBean(RechargeOrderPersistencePort.class)
    public RechargeOrderPersistencePort rechargeOrderPersistencePort(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            RechargeOutboxMapper outboxMapper,
            ObjectMapper objectMapper) {
        return new MybatisRechargeOrderPersistenceAdapter(orderMapper, extMapper, outboxMapper, objectMapper);
    }
}
