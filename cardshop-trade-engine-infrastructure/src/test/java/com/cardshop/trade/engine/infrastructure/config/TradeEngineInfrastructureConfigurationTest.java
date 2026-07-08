package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.DirectRechargeFrequencyPort;
import com.cardshop.trade.engine.application.recharge.MerchantNotifyPayloadSigner;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshotPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.MarkPersistStateResult;
import com.cardshop.trade.engine.application.recharge.RechargeSupplierGatewayPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessageListener;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.infrastructure.persistence.FallbackRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.cardshop.trade.engine.infrastructure.redis.InMemoryRedisHaGuard;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptLoader;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptExecutor;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisSessionFactory;
import com.cardshop.trade.engine.infrastructure.redis.RedisPersistOrderSnapshotAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisRechargeOrderQueryAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListenerAnnotationBeanPostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;

/**
 * 基础设施自动配置测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.14 实施任务拆分」任务 5 和任务 6。
 * Redis key builder 与脚本加载器是纯本地组件，应始终可装配；Redis 接单端口依赖真实 Redis 客户端，
 * local profile 排除 Redis 自动配置时不能因为缺少客户端导致应用启动失败。
 */
class TradeEngineInfrastructureConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TradeEngineInfrastructureConfiguration.class);

    @Test
    void createsKeyBuilderAndScriptLoaderWithoutRedisClient() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(TradeEngineProperties.class);
            assertThat(context).hasSingleBean(RechargeRedisKeyBuilder.class);
            assertThat(context).hasSingleBean(RechargeRedisScriptLoader.class);
            assertThat(context).hasSingleBean(RedisHaGuardPort.class);
            assertThat(context.getBean(RedisHaGuardPort.class)).isInstanceOf(InMemoryRedisHaGuard.class);
            assertThat(context).doesNotHaveBean(AcceptRechargeOrderPort.class);
        });
    }

    @Test
    void createsAcceptPortOnlyWhenRedisScriptExecutorExists() {
        contextRunner
                .withBean(RechargeRedisScriptExecutor.class, () -> (script, keys, args) -> Mono.empty())
                .run(context -> assertThat(context).hasSingleBean(AcceptRechargeOrderPort.class));
    }

    @Test
    void createsSessionAdapterAsAcceptAndReliabilityPortsWhenRedisSessionFactoryExists() {
        contextRunner
                .withBean(RechargeRedisSessionFactory.class, () -> () -> {
                    throw new AssertionError("configuration test should not open redis session");
                })
                .run(context -> {
                    assertThat(context).hasSingleBean(AcceptRechargeOrderPort.class);
                    assertThat(context).hasSingleBean(RechargeAcceptReliabilityPort.class);
                    assertThat(context.getBean(AcceptRechargeOrderPort.class))
                            .isSameAs(context.getBean(RechargeAcceptReliabilityPort.class));
                });
    }

    @Test
    void createsRedisOnlyOrderQueryPortWhenMysqlMappersAreMissing() {
        contextRunner
                .withBean(ReactiveStringRedisTemplate.class, TradeEngineInfrastructureConfigurationTest::redisTemplate)
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargeOrderQueryPort.class);
                    assertThat(context.getBean(RechargeOrderQueryPort.class))
                            .isInstanceOf(RedisRechargeOrderQueryAdapter.class);
                });
    }

    @Test
    void wrapsRedisOrderQueryPortWithMysqlFallbackWhenMappersExist() {
        contextRunner
                .withBean(ReactiveStringRedisTemplate.class, TradeEngineInfrastructureConfigurationTest::redisTemplate)
                .withBean(McOrderMapper.class, () -> mapperProxy(McOrderMapper.class))
                .withBean(RechargeOrderExtMapper.class, () -> mapperProxy(RechargeOrderExtMapper.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargeOrderQueryPort.class);
                    assertThat(context.getBean(RechargeOrderQueryPort.class))
                            .isInstanceOf(FallbackRechargeOrderQueryAdapter.class);
                });
    }

    @Test
    void createsPersistOrderSnapshotPortWhenRedisTemplateExists() {
        contextRunner
                .withBean(ReactiveStringRedisTemplate.class, TradeEngineInfrastructureConfigurationTest::redisTemplate)
                .run(context -> {
                    assertThat(context).hasSingleBean(PersistOrderSnapshotPort.class);
                    assertThat(context.getBean(PersistOrderSnapshotPort.class))
                            .isInstanceOf(RedisPersistOrderSnapshotAdapter.class);
                });
    }

    @Test
    void createsRechargeOrderPersistencePortWhenMappersExist() {
        contextRunner
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(McOrderMapper.class, () -> mapperProxy(McOrderMapper.class))
                .withBean(RechargeOrderExtMapper.class, () -> mapperProxy(RechargeOrderExtMapper.class))
                .withBean(RechargeOutboxMapper.class, () -> mapperProxy(RechargeOutboxMapper.class))
                .withBean(McDepartRecordMapper.class, () -> mapperProxy(McDepartRecordMapper.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargeOrderPersistencePort.class);
                    assertThat(context.getBean(RechargeOrderPersistencePort.class))
                            .isInstanceOf(MybatisRechargeOrderPersistenceAdapter.class);
                });
    }

    @Test
    void doesNotCreateRechargeOrderPersistencePortWhenDepartRecordMapperIsMissing() {
        contextRunner
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(McOrderMapper.class, () -> mapperProxy(McOrderMapper.class))
                .withBean(RechargeOrderExtMapper.class, () -> mapperProxy(RechargeOrderExtMapper.class))
                .withBean(RechargeOutboxMapper.class, () -> mapperProxy(RechargeOutboxMapper.class))
                .run(context -> assertThat(context).doesNotHaveBean(RechargeOrderPersistencePort.class));
    }

    @Test
    void createsPersistPublisherWhenRabbitTemplateAndMqConfirmExecutorExist() {
        contextRunner
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(RabbitTemplate.class, TestRabbitTemplate::new)
                .withBean("tradeMqConfirmTaskExecutor", Executor.class, () -> Runnable::run)
                .run(context -> {
                    assertThat(context).hasSingleBean(PersistRechargeOrderMessagePublisher.class);
                    assertThat(context.getBean(PersistRechargeOrderMessagePublisher.class))
                            .isInstanceOf(RabbitPersistRechargeOrderMessagePublisher.class);
                });
    }

    @Test
    void doesNotCreatePersistPublisherWhenMqConfirmExecutorIsMissing() {
        contextRunner
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(RabbitTemplate.class, TestRabbitTemplate::new)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void enablesRabbitListenerAnnotationProcessing() {
        contextRunner.run(context ->
                assertThat(context).hasSingleBean(RabbitListenerAnnotationBeanPostProcessor.class));
    }

    @Test
    void createsPersistQueueDeclarablesWithQuorumAndDeadLetterContract() {
        contextRunner.run(context -> {
            Declarables declarables = context.getBean("rechargePersistDeclarables", Declarables.class);

            DirectExchange exchange = singleDeclarable(
                    declarables,
                    DirectExchange.class,
                    item -> "recharge.persist.exchange".equals(item.getName()));
            DirectExchange retryExchange = singleDeclarable(
                    declarables,
                    DirectExchange.class,
                    item -> "recharge.persist.retry.exchange".equals(item.getName()));
            DirectExchange dlx = singleDeclarable(
                    declarables,
                    DirectExchange.class,
                    item -> "recharge.persist.dlx".equals(item.getName()));
            Queue queue = singleDeclarable(
                    declarables,
                    Queue.class,
                    item -> "recharge.persist.mysql.queue".equals(item.getName()));
            Queue retryQueue = singleDeclarable(
                    declarables,
                    Queue.class,
                    item -> "recharge.persist.mysql.retry.queue".equals(item.getName()));
            Queue dlq = singleDeclarable(
                    declarables,
                    Queue.class,
                    item -> "recharge.persist.mysql.dlq".equals(item.getName()));
            Binding binding = singleDeclarable(
                    declarables,
                    Binding.class,
                    item -> "recharge.persist.mysql.queue".equals(item.getDestination()));
            Binding retryBinding = singleDeclarable(
                    declarables,
                    Binding.class,
                    item -> "recharge.persist.mysql.retry.queue".equals(item.getDestination()));
            Binding dlqBinding = singleDeclarable(
                    declarables,
                    Binding.class,
                    item -> "recharge.persist.mysql.dlq".equals(item.getDestination()));

            assertThat(exchange.isDurable()).isTrue();
            assertThat(retryExchange.isDurable()).isTrue();
            assertThat(dlx.isDurable()).isTrue();
            assertThat(queue.isDurable()).isTrue();
            assertThat(queue.getArguments()).containsEntry("x-queue-type", "quorum");
            assertThat(queue.getArguments()).containsEntry("x-dead-letter-exchange", "recharge.persist.dlx");
            assertThat(queue.getArguments()).containsEntry("x-dead-letter-routing-key", "recharge.persist.mysql.dlq");
            assertThat(retryQueue.isDurable()).isTrue();
            assertThat(retryQueue.getArguments()).containsEntry("x-queue-type", "quorum");
            assertThat(retryQueue.getArguments()).containsEntry("x-dead-letter-exchange", "recharge.persist.exchange");
            assertThat(retryQueue.getArguments()).containsEntry("x-dead-letter-routing-key", "recharge.persist.mysql");
            assertThat(retryQueue.getArguments()).containsEntry("x-message-ttl", 30000);
            assertThat(dlq.isDurable()).isTrue();
            assertThat(dlq.getArguments()).containsEntry("x-queue-type", "quorum");
            assertThat(binding.getExchange()).isEqualTo("recharge.persist.exchange");
            assertThat(binding.getRoutingKey()).isEqualTo("recharge.persist.mysql");
            assertThat(retryBinding.getExchange()).isEqualTo("recharge.persist.retry.exchange");
            assertThat(retryBinding.getRoutingKey()).isEqualTo("recharge.persist.mysql.retry");
            assertThat(dlqBinding.getExchange()).isEqualTo("recharge.persist.dlx");
            assertThat(dlqBinding.getRoutingKey()).isEqualTo("recharge.persist.mysql.dlq");
        });
    }

    @Test
    void createsPersistMessageListenerWhenRabbitAndConsumerExist() {
        contextRunner
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(RabbitTemplate.class, TestRabbitTemplate::new)
                .withBean("tradeMqConfirmTaskExecutor", Executor.class, () -> Runnable::run)
                .withBean(PersistRechargeOrderConsumerService.class, TradeEngineInfrastructureConfigurationTest::consumerService)
                .withBean("rabbitListenerContainerFactory", RabbitListenerContainerFactory.class,
                        () -> ignored -> interfaceProxy(MessageListenerContainer.class))
                .run(context -> assertThat(context).hasSingleBean(RabbitPersistRechargeOrderMessageListener.class));
    }

    @Test
    void doesNotCreatePersistMessageListenerWhenRabbitTemplateIsMissing() {
        contextRunner
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(PersistRechargeOrderConsumerService.class, TradeEngineInfrastructureConfigurationTest::consumerService)
                .run(context -> assertThat(context).doesNotHaveBean(RabbitPersistRechargeOrderMessageListener.class));
    }

    @Test
    void createsAcceptAnomalyCheckRepositoryWhenMapperExists() {
        contextRunner
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(RechargeAcceptAnomalyMapper.class, () -> mapperProxy(RechargeAcceptAnomalyMapper.class))
                .run(context -> assertThat(context).hasSingleBean(AcceptAnomalyCheckRepositoryPort.class));
    }

    @Test
    void doesNotCreateOutboxWorkerWhenMerchantProfilePortIsMissing() {
        contextRunner
                .withBean(RechargeOutboxPort.class, () -> interfaceProxy(RechargeOutboxPort.class))
                .withBean(RechargeFulfillmentOrderPort.class, () -> interfaceProxy(RechargeFulfillmentOrderPort.class))
                .withBean(RechargeSupplierGatewayPort.class, () -> interfaceProxy(RechargeSupplierGatewayPort.class))
                .withBean(DirectRechargeFrequencyPort.class, () -> interfaceProxy(DirectRechargeFrequencyPort.class))
                .withBean(RechargeNotifyTaskPort.class, () -> interfaceProxy(RechargeNotifyTaskPort.class))
                .withBean(MerchantNotifyPayloadSigner.class, () -> interfaceProxy(MerchantNotifyPayloadSigner.class))
                .run(context -> assertThat(context).doesNotHaveBean(RechargeOutboxWorkerService.class));
    }

    @Test
    void createsOutboxWorkerWhenMerchantProfilePortAndFulfillmentPortsExist() {
        contextRunner
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(RechargeOutboxPort.class, () -> interfaceProxy(RechargeOutboxPort.class))
                .withBean(RechargeFulfillmentOrderPort.class, () -> interfaceProxy(RechargeFulfillmentOrderPort.class))
                .withBean(RechargeSupplierGatewayPort.class, () -> interfaceProxy(RechargeSupplierGatewayPort.class))
                .withBean(DirectRechargeFrequencyPort.class, () -> interfaceProxy(DirectRechargeFrequencyPort.class))
                .withBean(RechargeNotifyTaskPort.class, () -> interfaceProxy(RechargeNotifyTaskPort.class))
                .withBean(V1MerchantProfilePort.class, () -> interfaceProxy(V1MerchantProfilePort.class))
                .run(context -> assertThat(context).hasSingleBean(RechargeOutboxWorkerService.class));
    }

    private static <T> T mapperProxy(Class<T> mapperType) {
        return interfaceProxy(mapperType);
    }

    private static ReactiveStringRedisTemplate redisTemplate() {
        ReactiveRedisConnection connection = interfaceProxy(ReactiveRedisConnection.class);
        ReactiveRedisConnectionFactory connectionFactory = (ReactiveRedisConnectionFactory) Proxy.newProxyInstance(
                ReactiveRedisConnectionFactory.class.getClassLoader(),
                new Class<?>[]{ReactiveRedisConnectionFactory.class},
                (ignored, method, args) -> {
                    if ("getReactiveConnection".equals(method.getName())) {
                        return connection;
                    }
                    return defaultValue(method.getReturnType());
                });
        return new ReactiveStringRedisTemplate(connectionFactory);
    }

    private static PersistRechargeOrderConsumerService consumerService() {
        RechargeAcceptReliabilityPort reliabilityPort = new RechargeAcceptReliabilityPort() {
            @Override
            public java.util.concurrent.CompletionStage<com.cardshop.trade.engine.application.recharge.ReplicationWaitResult> waitForReplication(
                    com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand command,
                    com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult acceptResult) {
                throw new AssertionError("configuration test should not wait for redis replication");
            }

            @Override
            public java.util.concurrent.CompletionStage<com.cardshop.trade.engine.application.recharge.CancelUnconfirmedAcceptResult> cancelUnconfirmedAccept(
                    com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand command,
                    com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult acceptResult,
                    String reason) {
                throw new AssertionError("configuration test should not cancel redis accept");
            }

            @Override
            public java.util.concurrent.CompletionStage<com.cardshop.trade.engine.application.recharge.MarkPersistQueuedResult> markPersistQueued(
                    com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand command,
                    com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult acceptResult,
                    String persistMessageId,
                    int publishCount) {
                throw new AssertionError("configuration test should not mark redis persist queued");
            }

            @Override
            public java.util.concurrent.CompletionStage<MarkPersistStateResult> markDbPersisted(
                    com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand command,
                    com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult acceptResult,
                    long persistedVersion,
                    long nowEpochMillis) {
                return CompletableFuture.completedFuture(MarkPersistStateResult.success());
            }

            @Override
            public java.util.concurrent.CompletionStage<MarkPersistStateResult> markPersistFailed(
                    com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand command,
                    com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult acceptResult,
                    String failCode,
                    String failReason,
                    int retryCount,
                    long nextRetryEpochMillis,
                    long nowEpochMillis) {
                return CompletableFuture.completedFuture(MarkPersistStateResult.success());
            }
        };
        return new PersistRechargeOrderConsumerService(
                PersistOrderSnapshotPort.noop(),
                aggregate -> {
                },
                reliabilityPort,
                Clock.systemUTC());
    }

    private static <T> T interfaceProxy(Class<T> type) {
        Object proxy = Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[]{type},
                (ignored, method, args) -> defaultValue(method.getReturnType()));
        return type.cast(proxy);
    }

    private static Object defaultValue(Class<?> returnType) {
        if (returnType == void.class) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == byte.class) {
            return (byte) 0;
        }
        if (returnType == short.class) {
            return (short) 0;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0F;
        }
        if (returnType == double.class) {
            return 0D;
        }
        if (returnType == char.class) {
            return '\0';
        }
        if (returnType == Mono.class) {
            return Mono.empty();
        }
        return null;
    }

    private static final class TestRabbitTemplate extends RabbitTemplate {

        @Override
        public void afterPropertiesSet() {
            // 装配测试只需要 RabbitTemplate bean 存在，不需要真实 RabbitMQ ConnectionFactory。
        }
    }

    private static <T extends Declarable> T singleDeclarable(
            Declarables declarables,
            Class<T> declarableType,
            Predicate<T> predicate) {
        return declarables.getDeclarables().stream()
                .filter(declarableType::isInstance)
                .map(declarableType::cast)
                .filter(predicate)
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "missing declarable type=" + declarableType.getSimpleName()));
    }
}
