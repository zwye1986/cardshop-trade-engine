package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecordPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.BalanceDeltaFlushOutboxPort;
import com.cardshop.trade.engine.application.recharge.BalanceReservationReleasePort;
import com.cardshop.trade.engine.application.recharge.CancelUnconfirmedAcceptResult;
import com.cardshop.trade.engine.application.recharge.DirectRechargeFrequencyPort;
import com.cardshop.trade.engine.application.recharge.MarkPersistQueuedResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistStateResult;
import com.cardshop.trade.engine.application.recharge.MerchantNotifyClientPort;
import com.cardshop.trade.engine.application.recharge.MerchantNotifyPayloadSigner;
import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.application.recharge.PersistedOrderStatePort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerSettings;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerSettings;
import com.cardshop.trade.engine.application.recharge.RechargeSupplierGatewayPort;
import com.cardshop.trade.engine.application.recharge.SupplierQueryOutboxPort;
import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.recharge.ReplicationWaitResult;
import com.cardshop.trade.engine.application.recharge.V1MerchantNotifyPayloadSigner;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.CachedV1MerchantProfilePort;
import com.cardshop.trade.engine.application.v1.CachedV1ProductCandidateSelector;
import com.cardshop.trade.engine.application.v1.V1CreateOrderStageObserver;
import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfileCacheObserver;
import com.cardshop.trade.engine.application.v1.V1MerchantProfileCacheSettings;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.application.v1.V1ProductCacheObserver;
import com.cardshop.trade.engine.application.v1.V1ProductCacheSettings;
import com.cardshop.trade.engine.application.v1.V1ProductCandidateSelector;
import com.cardshop.trade.engine.application.v1.V1ProductConfigPreheatPort;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.application.v1.V1ProductSelectionCachePort;
import com.cardshop.trade.engine.application.v1.V1TokenStorePort;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessageListener;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.infrastructure.mq.RabbitTemplatePersistMessageOperations;
import com.cardshop.trade.engine.infrastructure.notify.HttpMerchantNotifyClientAdapter;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.persistence.FallbackRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisAcceptAnomalyCheckRepositoryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisAcceptAnomalyRecordAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeFulfillmentOrderAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeNotifyTaskAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeBalanceDeltaFlushAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOutboxAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisPersistedOrderStateAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisV1MerchantProfileAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisV1ProductCandidateSelector;
import com.cardshop.trade.engine.infrastructure.persistence.V1DepartProductReader;
import com.cardshop.trade.engine.infrastructure.persistence.V1MerchantProfileReader;
import com.cardshop.trade.engine.infrastructure.persistence.V1ProductReader;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeNotifyTaskMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.cardshop.trade.engine.infrastructure.redis.LettuceRechargeRedisSessionFactory;
import com.cardshop.trade.engine.infrastructure.redis.InMemoryRedisHaGuard;
import com.cardshop.trade.engine.infrastructure.redis.RedisRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisSessionAcceptReliabilityAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisV1TokenStoreAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisV1ProductConfigPreheatAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisV1ProductSelectionCacheAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisDirectRechargeFrequencyAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptLoader;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptExecutor;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisSessionFactory;
import com.cardshop.trade.engine.infrastructure.redis.RedisBalanceReservationReleaseAdapter;
import com.cardshop.trade.engine.infrastructure.redis.SpringDataRechargeRedisScriptExecutor;
import com.cardshop.trade.engine.infrastructure.supplier.HttpRechargeSupplierGatewayAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import reactor.core.publisher.Mono;

/**
 * 基础设施层自动配置入口。
 *
 * <p>本配置类只启用 {@link TradeEngineProperties} 的绑定，对应
 * {@code docs/交行直充业务系统详细设计.md} 的「6.4 Infrastructure 层」和「11. 配置中心、部署与健康检查」。
 * D4 阶段开始声明 Redis 接单所需的 Key 构造、Lua 脚本和适配器；真实 Redis 客户端仍由 Spring Boot
 * 自动配置和 Nacos 环境提供，local profile 禁用 Redis 时不会强制创建接单端口。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TradeEngineProperties.class)
@EnableRabbit
public class TradeEngineInfrastructureConfiguration {

    @Bean
    @ConditionalOnMissingBean(ObjectMapper.class)
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    @ConditionalOnMissingBean(Clock.class)
    public Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * 校验并暴露 V1 商品选品缓存时间窗口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。构造器会校验
     * {@code refreshAfter < maxStaleAge < physicalTtl}，非法 Nacos 配置必须阻止应用启动。
     */
    @Bean
    @ConditionalOnMissingBean(V1ProductCacheSettings.class)
    public V1ProductCacheSettings v1ProductCacheSettings(TradeEngineProperties properties) {
        TradeEngineProperties.V1.ProductCache cache = properties.getV1().getProductCache();
        return new V1ProductCacheSettings(
                cache.getRefreshAfter(),
                cache.getMaxStaleAge(),
                cache.getPhysicalTtl(),
                cache.getRefreshLockTtl(),
                cache.getRefreshWaitTimeout(),
                cache.getNegativeCacheTtl());
    }

    /**
     * 声明异步落库 RabbitMQ 拓扑。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「7. RabbitMQ 持久化消息层」。
     * 主队列和 DLQ 使用 Quorum Queue；主队列死信始终进入最终 DLQ。retry queue 使用 classic durable queue，
     * 未耗尽重试次数的业务失败由 listener 发布到 retry exchange，retry queue 到期后通过 DLX 投回主 exchange。
     * 当前 dev/test RabbitMQ 3.8.8 对 quorum queue 不接受 {@code x-message-ttl} 参数；若 retry queue 声明为
     * quorum，会导致 RabbitAdmin 启动声明失败，进而出现应用健康但 RabbitMQ 无 consumer 的联调现象。
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
                .deadLetterExchange(mq.getPersistExchange())
                .deadLetterRoutingKey(mq.getPersistRoutingKey())
                .ttl(Math.max(1000, mq.getConsumerRetryDelayMs()))
                .build();
        Queue deadLetterQueue = QueueBuilder
                .durable(mq.getPersistDeadLetterQueue())
                .quorum()
                .build();

        // 参见详细设计「7. RabbitMQ 持久化消息层」：声明对象只描述 broker 拓扑，
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
    @ConditionalOnMissingBean(RechargeRedisScriptExecutor.class)
    public RechargeRedisScriptExecutor rechargeRedisScriptExecutor(
            ObjectProvider<ReactiveStringRedisTemplate> redisTemplateProvider) {
        ReactiveStringRedisTemplate redisTemplate = redisTemplateProvider.getIfAvailable();
        if (redisTemplate == null) {
            return (script, keys, args) -> Mono.error(new IllegalStateException("ReactiveStringRedisTemplate 未装配"));
        }
        return new SpringDataRechargeRedisScriptExecutor(redisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean(RechargeRedisSessionFactory.class)
    public RechargeRedisSessionFactory rechargeRedisSessionFactory(
            ObjectProvider<LettuceConnectionFactory> connectionFactoryProvider,
            ObjectProvider<V1CreateOrderStageObserver> createOrderStageObserverProvider) {
        LettuceConnectionFactory connectionFactory = connectionFactoryProvider.getIfAvailable();
        if (connectionFactory == null) {
            return () -> {
                throw new IllegalStateException("LettuceConnectionFactory 未装配");
            };
        }
        return new LettuceRechargeRedisSessionFactory(
                connectionFactory,
                () -> createOrderStageObserverProvider.getIfAvailable(V1CreateOrderStageObserver::noop));
    }

    @Bean
    @ConditionalOnMissingBean({AcceptRechargeOrderPort.class, RechargeAcceptReliabilityPort.class})
    public RedisSessionAcceptReliabilityAdapter redisSessionAcceptReliabilityAdapter(
            RechargeRedisSessionFactory sessionFactory,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            TradeEngineProperties properties,
            RedisHaGuardPort redisHaGuardPort,
            ObjectProvider<AcceptAnomalyRecordPort> acceptAnomalyRecordPortProvider,
            @Qualifier("tradeDbTaskExecutor") ObjectProvider<Executor> acceptAnomalyRecordExecutorProvider,
            ObjectProvider<TradeEngineObservabilityMetrics> observabilityMetricsProvider) {
        /*
         * accept Lua timeout/BUSY 属于“Redis 可能已写入但 Java 未拿到返回”的状态未知窗口。
         * 这里把 anomaly 写库端口和 DB executor 一起注入 Redis adapter：adapter 负责发现 Redis 协议边界异常，
         * 但真实写库必须切到 tradeDbTaskExecutor，避免阻塞 Lettuce/Reactor 回调线程。
         */
        return new RedisSessionAcceptReliabilityAdapter(
                sessionFactory,
                keyBuilder,
                scriptLoader,
                properties,
                redisHaGuardPort,
                acceptAnomalyRecordPortProvider.getIfAvailable(AcceptAnomalyRecordPort::noop),
                acceptAnomalyRecordExecutorProvider.getIfAvailable(() -> Runnable::run),
                observabilityMetricsProvider.getIfAvailable(TradeEngineObservabilityMetrics::noop));
    }

    @Bean
    @ConditionalOnMissingBean(BalanceReservationReleasePort.class)
    public BalanceReservationReleasePort balanceReservationReleasePort(
            RechargeRedisSessionFactory sessionFactory,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            Clock clock,
            TradeEngineProperties properties) {
        /*
         * 余额释放由 outbox worker 同步调用，不能让 Redis Lua 无界阻塞占死补偿线程。
         * 这里沿用 pending 补投 adapter 的 Redis 普通操作超时策略：以 WAIT 超时为基准放大 5 倍，
         * 既避免误伤短抖动，也保证异常窗口能回到 outbox 重试。参见 docs/交行直充业务系统高可用方案设计.md「14.6」。
         */
        Duration redisOperationTimeout = Duration.ofMillis(Math.max(100, properties.getRedis().getWaitTimeoutMs() * 5L));
        return new RedisBalanceReservationReleaseAdapter(
                sessionFactory, keyBuilder, scriptLoader, clock, redisOperationTimeout);
    }

    @Bean
    @ConditionalOnMissingBean(PersistRechargeOrderMessagePublisher.class)
    public PersistRechargeOrderMessagePublisher persistRechargeOrderMessagePublisher(
            ObjectProvider<RabbitTemplate> rabbitTemplateProvider,
            ObjectMapper objectMapper,
            TradeEngineProperties properties,
            @Qualifier("tradeMqConfirmTaskExecutor") ObjectProvider<Executor> mqConfirmExecutorProvider,
            ObjectProvider<TradeEngineObservabilityMetrics> observabilityMetricsProvider) {
        RabbitTemplate rabbitTemplate = rabbitTemplateProvider.getIfAvailable();
        Executor mqConfirmExecutor = mqConfirmExecutorProvider.getIfAvailable();
        if (rabbitTemplate == null || mqConfirmExecutor == null) {
            return unavailablePersistRechargeOrderMessagePublisher();
        }
        return new RabbitPersistRechargeOrderMessagePublisher(
                new RabbitTemplatePersistMessageOperations(rabbitTemplate),
                objectMapper,
                properties,
                Clock.systemUTC(),
                mqConfirmExecutor,
                observabilityMetricsProvider.getIfAvailable(TradeEngineObservabilityMetrics::noop));
    }

    /**
     * 装配 RabbitMQ 持久化消息 Listener。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「7. RabbitMQ 持久化消息层」和
     * 「8.3 异步落库 Consumer」。该 Bean 只在 RabbitMQ 客户端和 application 层 Consumer 核心服务都存在时创建：
     * local profile 排除 Rabbit 自动配置时不会启动监听容器；RabbitMQ 就绪但 MySQL/Redis 落库端口未就绪时也不会消费消息。
     */
    @Bean
    @ConditionalOnBean({RabbitTemplate.class, PersistRechargeOrderConsumerService.class})
    @ConditionalOnMissingBean(RabbitPersistRechargeOrderMessageListener.class)
    @ConditionalOnProperty(
            name = "trade.engine.mq.persist-batch-enabled",
            havingValue = "false",
            matchIfMissing = true)
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
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
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
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」和
     * 「6. Redis 受理与缓存层」。该 Bean 只提供异常表读取与状态推进，自动核查服务由 bootstrap 层组装。
     */
    @Bean
    @ConditionalOnBean(RechargeAcceptAnomalyMapper.class)
    public AcceptAnomalyCheckRepositoryPort acceptAnomalyCheckRepositoryPort(
            RechargeAcceptAnomalyMapper mapper,
            ObjectMapper objectMapper) {
        return new MybatisAcceptAnomalyCheckRepositoryAdapter(mapper, objectMapper, Clock.systemUTC());
    }

    /**
     * 装配 V1 商户资料端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。
     * 这里使用配置类显式装配，而不是在适配器类上做条件组件扫描，避免 MyBatis Mapper
     * 注册顺序导致条件过早判定为不满足，使 `/auth/getToken` 和新链路下单回落到未启用实现。
     */
    @Bean
    @ConditionalOnMissingBean(V1MerchantProfilePort.class)
    public V1MerchantProfilePort v1MerchantProfilePort(
            ObjectProvider<V1MerchantProfileReader> readerProvider,
            TradeEngineProperties properties,
            Clock clock,
            ObjectProvider<V1MerchantProfileCacheObserver> observerProvider) {
        V1MerchantProfileReader reader = readerProvider.getIfAvailable();
        if (reader == null) {
            return unavailableV1MerchantProfilePort();
        }
        V1MerchantProfilePort mybatisPort = new MybatisV1MerchantProfileAdapter(reader);
        TradeEngineProperties.V1.MerchantProfileCache cache = properties.getV1().getMerchantProfileCache();
        if (!cache.isEnabled()) {
            return mybatisPort;
        }
        /*
         * 参见详细设计「3. API/V1 接入层」：短 TTL 本地缓存降低 prepare 每单打 mc_ext_depart，
         * 避免与异步落库争抢 Hikari；aesKey 仅存本进程内存，不写入 Redis。
         */
        return new CachedV1MerchantProfilePort(
                mybatisPort,
                new V1MerchantProfileCacheSettings(
                        cache.isEnabled(),
                        cache.getTtl(),
                        cache.getNegativeTtl(),
                        cache.getMaximumSize()),
                clock,
                () -> observerProvider.getIfAvailable(V1MerchantProfileCacheObserver::noop));
    }

    /**
     * 装配 V1 直充商品候选选择端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
     * 该端口依赖两个旧卡管单表 Reader，和商户资料端口一样放在配置类中按依赖显式启用。
     */
    @Bean
    @ConditionalOnMissingBean(V1ProductCandidateSelector.class)
    public V1ProductCandidateSelector v1ProductCandidateSelector(
            ObjectProvider<V1DepartProductReader> departProductReaderProvider,
            ObjectProvider<V1ProductReader> productReaderProvider,
            ObjectProvider<V1ProductSelectionCachePort> cachePortProvider,
            V1ProductCacheSettings cacheSettings,
            TradeEngineProperties properties,
            Clock clock,
            ObjectProvider<V1ProductCacheObserver> observerProvider) {
        V1DepartProductReader departProductReader = departProductReaderProvider.getIfAvailable();
        V1ProductReader productReader = productReaderProvider.getIfAvailable();
        V1ProductCandidateSelector sourceSelector;
        if (departProductReader == null || productReader == null) {
            sourceSelector = (orgCode, productId, account) -> V1ProductSelection.failed(
                    productId,
                    "PRODUCT_READER_UNAVAILABLE",
                    "商品资料读取端口未装配",
                    1L);
        } else {
            sourceSelector = new MybatisV1ProductCandidateSelector(departProductReader, productReader);
        }
        V1ProductSelectionCachePort cachePort = cachePortProvider.getIfAvailable();
        if (cachePort == null || !cachePort.isAvailable()) {
            /*
             * 参见详细设计「6. Redis 受理与缓存层」：只有 local 且 Redis 写关闭的轻量自测允许无 Redis
             * 时直读 MySQL。local 开启 Redis 写，或 dev/test/prod 因配置错误缺少 Redis 时，仍保留缓存
             * 包装器并在请求时 fail closed，防止父快照缺失或秒杀流量静默退化为每单访问共享旧卡管数据库。
             */
            if ("local".equalsIgnoreCase(properties.getEnvironment())
                    && !properties.getRedis().isWriteEnabled()) {
                return sourceSelector;
            }
            cachePort = V1ProductSelectionCachePort.unavailable();
        }
        return new CachedV1ProductCandidateSelector(
                cachePort,
                sourceSelector,
                cacheSettings,
                clock,
                observerProvider.getIfAvailable(V1ProductCacheObserver::noop));
    }

    /**
     * 装配 V1 父商品选品快照 Redis 端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
     * TODO 后续接入旧卡管商品保存 outbox、CDC 或增量扫描主动同步；请求级单飞刷新长期保留为漏事件兜底，
     * 参见 {@code docs/交行直充业务系统高可用方案设计.md}「24.2 高风险点」。
     */
    @Bean
    @ConditionalOnMissingBean(V1ProductSelectionCachePort.class)
    public V1ProductSelectionCachePort v1ProductSelectionCachePort(
            ObjectProvider<ReactiveStringRedisTemplate> redisTemplateProvider,
            RechargeRedisKeyBuilder keyBuilder,
            V1ProductCacheSettings cacheSettings,
            TradeEngineProperties properties) {
        ReactiveStringRedisTemplate redisTemplate = redisTemplateProvider.getIfAvailable();
        if (redisTemplate == null) {
            return V1ProductSelectionCachePort.unavailable();
        }
        int timeoutMs = Math.max(100, properties.getRedis().getWaitTimeoutMs() * 5);
        return new RedisV1ProductSelectionCacheAdapter(
                redisTemplate,
                keyBuilder,
                cacheSettings,
                Duration.ofMillis(timeoutMs));
    }

    /**
     * 装配 V1 请求级商品配置补热端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。商品选择器已经从
     * MySQL 形成实际子商品快照；该端口在接单 Lua 前原子补写缺失 key，已有 Redis 配置仍由 Lua 按版本裁决。
     */
    @Bean
    @ConditionalOnMissingBean(V1ProductConfigPreheatPort.class)
    public V1ProductConfigPreheatPort v1ProductConfigPreheatPort(
            ObjectProvider<ReactiveStringRedisTemplate> redisTemplateProvider,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties) {
        ReactiveStringRedisTemplate redisTemplate = redisTemplateProvider.getIfAvailable();
        if (redisTemplate == null) {
            return V1ProductConfigPreheatPort.noop();
        }
        int timeoutMs = Math.max(100, properties.getRedis().getWaitTimeoutMs() * 5);
        return new RedisV1ProductConfigPreheatAdapter(
                redisTemplate,
                keyBuilder,
                properties.getV1().getProductCache().getPhysicalTtl(),
                Duration.ofMillis(timeoutMs));
    }

    @Bean
    @ConditionalOnMissingBean(V1TokenStorePort.class)
    public V1TokenStorePort v1TokenStorePort(
            ObjectProvider<ReactiveStringRedisTemplate> redisTemplateProvider,
            TradeEngineProperties properties) {
        ReactiveStringRedisTemplate redisTemplate = redisTemplateProvider.getIfAvailable();
        if (redisTemplate == null) {
            return unavailableV1TokenStorePort();
        }
        return new RedisV1TokenStoreAdapter(redisTemplate, properties);
    }

    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    @ConditionalOnMissingBean(RechargeOrderQueryPort.class)
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

    @Bean
    @ConditionalOnBean(RechargeOrderExtMapper.class)
    @ConditionalOnMissingBean(PersistedOrderStatePort.class)
    public PersistedOrderStatePort persistedOrderStatePort(RechargeOrderExtMapper extMapper) {
        return new MybatisPersistedOrderStateAdapter(extMapper);
    }

    /**
     * 装配 MySQL 异步落库端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
     * 四个 Mapper 同时存在时才启用，确保事务内能完整写 `mc_order`、`recharge_order_ext`、
     * `mc_depart_record(PRE_DEDUCT)` 和 outbox。该 Bean 主要覆盖显式提供 Mapper 的 slice test 或局部上下文；
     * 真实 dev/test/prod 中 Mapper 来自 MyBatis 自动配置，最终兜底由
     * {@link TradeEngineMybatisPersistenceAutoConfiguration} 在 MyBatis mapper scanner 完成后执行。
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

    @Bean
    @ConditionalOnBean({RechargeOutboxMapper.class, McExtDepartMapper.class, PlatformTransactionManager.class})
    @ConditionalOnMissingBean(RechargeBalanceDeltaFlushPort.class)
    public RechargeBalanceDeltaFlushPort rechargeBalanceDeltaFlushPort(
            RechargeOutboxMapper outboxMapper,
            McExtDepartMapper extDepartMapper,
            ObjectMapper objectMapper,
            Clock clock,
            TradeEngineProperties properties,
            PlatformTransactionManager transactionManager) {
        return new MybatisRechargeBalanceDeltaFlushAdapter(
                outboxMapper,
                extDepartMapper,
                objectMapper,
                clock,
                Duration.ofSeconds(properties.getFulfillment().getOutboxClaimLeaseSeconds()),
                Duration.ofSeconds(properties.getFulfillment().getOutboxRetryBackoffSeconds()),
                transactionManager);
    }

    @Bean
    @ConditionalOnBean(RechargeOutboxMapper.class)
    @ConditionalOnMissingBean(RechargeOutboxPort.class)
    public MybatisRechargeOutboxAdapter rechargeOutboxPort(
            RechargeOutboxMapper outboxMapper,
            ObjectMapper objectMapper,
            Clock clock) {
        return new MybatisRechargeOutboxAdapter(outboxMapper, objectMapper, clock);
    }

    @Bean
    @ConditionalOnBean({McOrderMapper.class, RechargeOrderExtMapper.class, McOrderSupplierFlowMapper.class})
    @ConditionalOnMissingBean(RechargeFulfillmentOrderPort.class)
    public RechargeFulfillmentOrderPort rechargeFulfillmentOrderPort(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            McOrderSupplierFlowMapper flowMapper,
            ObjectMapper objectMapper,
            Clock clock) {
        /*
         * 参见详细设计「9.1 派单 Outbox」。派单阶段只使用订单受理时从请求父产品候选中冻结的
         * `produt_code/url` 子商品快照，不再读取 mc_depart_product/mc_product 做候选切换。
         */
        return new MybatisRechargeFulfillmentOrderAdapter(
                orderMapper,
                extMapper,
                flowMapper,
                objectMapper,
                clock);
    }

    @Bean
    @ConditionalOnBean(RechargeNotifyTaskMapper.class)
    @ConditionalOnMissingBean(RechargeNotifyTaskPort.class)
    public MybatisRechargeNotifyTaskAdapter rechargeNotifyTaskPort(
            RechargeNotifyTaskMapper notifyTaskMapper,
            ObjectMapper objectMapper,
            Clock clock) {
        return new MybatisRechargeNotifyTaskAdapter(notifyTaskMapper, objectMapper, clock);
    }

    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    @ConditionalOnMissingBean(DirectRechargeFrequencyPort.class)
    public DirectRechargeFrequencyPort directRechargeFrequencyPort(
            ReactiveStringRedisTemplate redisTemplate,
            TradeEngineProperties properties) {
        int timeoutMs = Math.max(100, properties.getRedis().getWaitTimeoutMs() * 3);
        return new RedisDirectRechargeFrequencyAdapter(redisTemplate, Duration.ofMillis(timeoutMs));
    }

    @Bean
    @ConditionalOnMissingBean(HttpClient.class)
    public HttpClient tradeEngineHttpClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(MerchantNotifyClientPort.class)
    public MerchantNotifyClientPort merchantNotifyClientPort(
            HttpClient httpClient,
            ObjectMapper objectMapper,
            TradeEngineProperties properties) {
        return new HttpMerchantNotifyClientAdapter(
                httpClient,
                objectMapper,
                Duration.ofMillis(properties.getNotify().getHttpTimeoutMs()));
    }

    @Bean
    @ConditionalOnProperty(prefix = "trade.engine.supplier", name = "http-enabled", havingValue = "true")
    @ConditionalOnMissingBean(RechargeSupplierGatewayPort.class)
    public RechargeSupplierGatewayPort rechargeSupplierGatewayPort(
            HttpClient httpClient,
            ObjectMapper objectMapper,
            TradeEngineProperties properties) {
        Map<String, HttpRechargeSupplierGatewayAdapter.SupplierChannelEndpoint> channels = new LinkedHashMap<>();
        int defaultTimeoutMs = Math.max(100, properties.getSupplier().getDefaultTimeoutMs());
        properties.getSupplier().getChannels().forEach((channel, endpoint) -> {
            int timeoutMs = endpoint.getTimeoutMs() == null ? defaultTimeoutMs : Math.max(100, endpoint.getTimeoutMs());
            channels.put(channel, new HttpRechargeSupplierGatewayAdapter.SupplierChannelEndpoint(
                    endpoint.getDispatchUrl(),
                    endpoint.getQueryUrl(),
                    endpoint.getInvalidUrl(),
                    Duration.ofMillis(timeoutMs)));
        });
        return new HttpRechargeSupplierGatewayAdapter(channels, objectMapper, httpClient);
    }

    @Bean
    @ConditionalOnBean({
            RechargeOutboxPort.class,
            RechargeFulfillmentOrderPort.class,
            RechargeSupplierGatewayPort.class,
            SupplierQueryOutboxPort.class,
            DirectRechargeFrequencyPort.class,
            BalanceDeltaFlushOutboxPort.class,
            BalanceReservationReleasePort.class,
            RechargeNotifyTaskPort.class,
            V1MerchantProfilePort.class
    })
    @ConditionalOnMissingBean(RechargeOutboxWorkerService.class)
    public RechargeOutboxWorkerService rechargeOutboxWorkerService(
            RechargeOutboxPort outboxPort,
            RechargeFulfillmentOrderPort orderPort,
            RechargeSupplierGatewayPort supplierGatewayPort,
            SupplierQueryOutboxPort supplierQueryOutboxPort,
            DirectRechargeFrequencyPort frequencyPort,
            BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort,
            BalanceReservationReleasePort balanceReservationReleasePort,
            RechargeNotifyTaskPort notifyTaskPort,
            ObjectProvider<MerchantNotifyPayloadSigner> payloadSignerProvider,
            V1MerchantProfilePort merchantProfilePort,
            Clock clock,
            TradeEngineProperties properties) {
        var fulfillment = properties.getFulfillment();
        MerchantNotifyPayloadSigner payloadSigner = payloadSignerProvider.getIfAvailable(
                () -> new V1MerchantNotifyPayloadSigner(merchantProfilePort));
        return new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGatewayPort,
                supplierQueryOutboxPort,
                frequencyPort,
                balanceDeltaFlushOutboxPort,
                balanceReservationReleasePort,
                notifyTaskPort,
                payloadSigner,
                clock,
                new RechargeOutboxWorkerSettings(
                        Duration.ofSeconds(fulfillment.getOutboxClaimLeaseSeconds()),
                        Duration.ofSeconds(fulfillment.getOutboxRetryBackoffSeconds()),
                        Duration.ofSeconds(fulfillment.getFrequencyRetryDelaySeconds()),
                        Duration.ofMinutes(fulfillment.getFrequencyLockTtlMinutes()),
                        Duration.ofSeconds(fulfillment.getFrequencySuccessMinimumHoldSeconds())));
    }

    @Bean
    @ConditionalOnBean({RechargeNotifyWorkerTaskPort.class, MerchantNotifyClientPort.class})
    @ConditionalOnMissingBean(RechargeNotifyWorkerService.class)
    public RechargeNotifyWorkerService rechargeNotifyWorkerService(
            RechargeNotifyWorkerTaskPort taskPort,
            MerchantNotifyClientPort notifyClientPort,
            Clock clock,
            TradeEngineProperties properties) {
        return new RechargeNotifyWorkerService(
                taskPort,
                notifyClientPort,
                clock,
                new RechargeNotifyWorkerSettings(
                        Duration.ofSeconds(properties.getNotify().getClaimLeaseSeconds()),
                        Duration.ofSeconds(properties.getNotify().getRetryBackoffSeconds())));
    }

    /**
     * RabbitMQ 依赖缺失时的不可用发布端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「4. Application 用例编排层」。
     * 该兜底端口让应用保持可启动，但 {@link PersistRechargeOrderMessagePublisher#isAvailable()}
     * 始终返回 false，使下单链路在写 Redis 前 fail closed，避免产生无法异步落库的订单事实。
     */
    private static PersistRechargeOrderMessagePublisher unavailablePersistRechargeOrderMessagePublisher() {
        return new PersistRechargeOrderMessagePublisher() {
            @Override
            public boolean isAvailable() {
                return false;
            }

            @Override
            public CompletionStage<PersistMessagePublishResult> publish(PersistTradeOrderMessage message) {
                String messageId = message == null ? "" : message.messageId();
                return CompletableFuture.completedFuture(PersistMessagePublishResult.failed(
                        messageId,
                        1,
                        "MQ_PUBLISHER_UNAVAILABLE",
                        "RabbitMQ 发布端口未装配"));
            }
        };
    }

    /**
     * 旧卡管商户资料 Reader 缺失时的不可用端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。
     * 这里返回空资料而不是抛异常，保证启动诊断可继续完成；Token、下单和回调签名会按商户资料缺失处理。
     */
    private static V1MerchantProfilePort unavailableV1MerchantProfilePort() {
        return new V1MerchantProfilePort() {
            @Override
            public Optional<V1MerchantProfile> findByAppIdAndOrgCode(String appId, String orgCode) {
                return Optional.empty();
            }

            @Override
            public Optional<V1MerchantProfile> findByOrgCode(String orgCode) {
                return Optional.empty();
            }

            @Override
            public Optional<V1MerchantProfile> findByAppIdAndAppSecret(String appId, String appSecret) {
                return Optional.empty();
            }
        };
    }

    /**
     * Redis Token 存储缺失时的不可用端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。
     * 保存返回 false、读取返回空，让 `/auth/getToken` 在 Redis 未就绪时明确失败。
     */
    private static V1TokenStorePort unavailableV1TokenStorePort() {
        return new V1TokenStorePort() {
            @Override
            public boolean storeIssuedToken(String appId, String token, Duration ttl) {
                return false;
            }

            @Override
            public Optional<String> findIssuedToken(String appId) {
                return Optional.empty();
            }
        };
    }
}
