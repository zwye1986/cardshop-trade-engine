package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecordPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshotPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.V1TokenStorePort;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessageListener;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.infrastructure.mq.RabbitTemplatePersistMessageOperations;
import com.cardshop.trade.engine.infrastructure.persistence.FallbackRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisAcceptAnomalyCheckRepositoryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisAcceptAnomalyRecordAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
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
import java.util.concurrent.Executor;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
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
@EnableRabbit
public class TradeEngineInfrastructureConfiguration {

    /**
     * 声明异步落库 RabbitMQ 拓扑。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.9 RabbitMQ 声明契约」。
     * 主队列、retry queue 和 DLQ 均使用 Quorum Queue；主队列死信始终进入最终 DLQ，
     * 未耗尽重试次数的业务失败由 listener 发布到 retry exchange，retry queue 到期后通过 DLX 投回主 exchange。
     * 该方法只创建 Spring AMQP 的声明对象，真正向 broker declare 由 RabbitAdmin 在具备 RabbitMQ 自动配置的环境中完成。
     */
    @Bean
    public Declarables rechargePersistDeclarables(TradeEngineProperties properties) {
        TradeEngineProperties.Mq mq = properties.getMq();
        DirectExchange exchange = ExchangeBuilder
                .directExchange(mq.getPersistExchange())
                .durable(true)
                .build();
        DirectExchange retryExchange = ExchangeBuilder
                .directExchange(mq.getPersistRetryExchange())
                .durable(true)
                .build();
        DirectExchange deadLetterExchange = ExchangeBuilder
                .directExchange(mq.getPersistDeadLetterExchange())
                .durable(true)
                .build();
        Queue queue = QueueBuilder
                .durable(mq.getPersistQueue())
                .quorum()
                .deadLetterExchange(mq.getPersistDeadLetterExchange())
                .deadLetterRoutingKey(mq.getPersistDeadLetterRoutingKey())
                .build();
        Queue retryQueue = QueueBuilder
                .durable(mq.getPersistRetryQueue())
                .quorum()
                .deadLetterExchange(mq.getPersistExchange())
                .deadLetterRoutingKey(mq.getPersistRoutingKey())
                .ttl(Math.max(1000, mq.getConsumerRetryDelayMs()))
                .build();
        Queue deadLetterQueue = QueueBuilder
                .durable(mq.getPersistDeadLetterQueue())
                .quorum()
                .build();

        // 参见详细设计「20.9 RabbitMQ 声明契约」：声明对象只描述 broker 拓扑，
        // 真实创建/校验由 RabbitAdmin 在具备 RabbitMQ 连接的 profile 中执行。
        return new Declarables(
                exchange,
                retryExchange,
                deadLetterExchange,
                queue,
                retryQueue,
                deadLetterQueue,
                BindingBuilder.bind(queue).to(exchange).with(mq.getPersistRoutingKey()),
                BindingBuilder.bind(retryQueue).to(retryExchange).with(mq.getPersistRetryRoutingKey()),
                BindingBuilder.bind(deadLetterQueue).to(deadLetterExchange).with(mq.getPersistDeadLetterRoutingKey()));
    }

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
            TradeEngineProperties properties,
            @Qualifier("tradeMqConfirmTaskExecutor") Executor mqConfirmExecutor) {
        return new RabbitPersistRechargeOrderMessagePublisher(
                new RabbitTemplatePersistMessageOperations(rabbitTemplate),
                objectMapper,
                properties,
                Clock.systemUTC(),
                mqConfirmExecutor);
    }

    /**
     * 装配 RabbitMQ 持久化消息 Listener。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.9 RabbitMQ 声明契约」和
     * 「20.13.8 异步落库 Consumer」。该 Bean 只在 RabbitMQ 客户端和 application 层 Consumer 核心服务都存在时创建：
     * local profile 排除 Rabbit 自动配置时不会启动监听容器；RabbitMQ 就绪但 MySQL/Redis 落库端口未就绪时也不会消费消息。
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
     * 四个 Mapper 同时存在时才启用，确保事务内能完整写 `mc_order`、`recharge_order_ext`、
     * `mc_depart_record(PRE_DEDUCT)` 和 outbox。
     */
    @Bean
    @ConditionalOnBean({McOrderMapper.class, RechargeOrderExtMapper.class, RechargeOutboxMapper.class, McDepartRecordMapper.class})
    @ConditionalOnMissingBean(RechargeOrderPersistencePort.class)
    public RechargeOrderPersistencePort rechargeOrderPersistencePort(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            RechargeOutboxMapper outboxMapper,
            McDepartRecordMapper departRecordMapper,
            ObjectMapper objectMapper) {
        return new MybatisRechargeOrderPersistenceAdapter(orderMapper, extMapper, outboxMapper, departRecordMapper, objectMapper);
    }
}
