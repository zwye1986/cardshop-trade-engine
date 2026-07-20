package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.RechargeOrderBatchPersistencePort;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderBatchMessageListener;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessageListener;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.context.SmartLifecycle;
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

    private static final Logger log = LoggerFactory.getLogger(TradeEngineRabbitListenerAutoConfiguration.class);
    private static final String DEFAULT_RABBIT_LISTENER_CONTAINER_FACTORY = "rabbitListenerContainerFactory";
    private static final String REWRITE_BATCHED_STATEMENTS = "rewritebatchedstatements";

    /**
     * 按业务批消费开关修正 Spring AMQP 默认 simple listener factory。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。Spring Boot 3.3.x
     * 会映射 {@code spring.rabbitmq.listener.simple.consumer-batch-enabled} 和 {@code batch-size}，但没有暴露
     * {@link SimpleRabbitListenerContainerFactory#setBatchReceiveTimeout(Long)} 对应的配置项。这里在业务批开关打开时
     * 显式设置 {@code consumerBatchEnabled}、{@code batchSize}、{@code receiveTimeout=persist-batch-poll-timeout-ms}
     * 与 {@code batchReceiveTimeout=persist-batch-receive-timeout-ms}，保证 Nacos 中的
     * {@code trade.engine.mq.persist-batch-*} 配置真实驱动容器攒批行为。
     */
    @Bean
    @ConditionalOnClass(SimpleRabbitListenerContainerFactory.class)
    @ConditionalOnProperty(name = "trade.engine.mq.persist-batch-enabled", havingValue = "true")
    public static BeanPostProcessor tradeEnginePersistBatchRabbitListenerContainerFactoryPostProcessor() {
        return new PersistBatchRabbitListenerContainerFactoryPostProcessor();
    }

    /**
     * 批消费端口缺失时启动失败。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。批 listener 调用
     * {@link PersistRechargeOrderConsumerService#persistBatch(java.util.List)} 时必须依赖
     * {@link RechargeOrderBatchPersistencePort}；如果批开关已打开但批写端口没有装配，应在启动期暴露配置/依赖问题，
     * 不能等 RabbitMQ 消费到第一批消息后才整批 retry 或进入 DLQ。
     */
    @Bean
    @ConditionalOnProperty(name = "trade.engine.mq.persist-batch-enabled", havingValue = "true")
    @ConditionalOnMissingBean(RechargeOrderBatchPersistencePort.class)
    public SmartLifecycle persistBatchRequiresBatchPersistencePort() {
        return new MissingBatchPersistencePortLifecycle();
    }

    /**
     * 批消费开启时校验 JDBC {@code rewriteBatchedStatements=true}。
     *
     * <p>参见 {@code docs/superpowers/specs/2026-07-17-persist-batch-consumer-design.md}「6. JDBC / MyBatis 底座」。
     * 缺少该参数时 MyBatis BATCH 仍会逐条发往 MySQL，批模式收益几乎落空。启动期 fail-fast，避免灰度打开后才发现
     * 吞吐未提升。
     */
    @Bean
    @ConditionalOnProperty(name = "trade.engine.mq.persist-batch-enabled", havingValue = "true")
    public SmartLifecycle persistBatchRequiresRewriteBatchedStatements(Environment environment) {
        return new RewriteBatchedStatementsLifecycle(environment);
    }

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
     * 创建 Redis/MQ 到 MySQL 异步落库的 Rabbit 批 listener。
     *
     * <p>批 listener 与单条 listener 通过 {@code trade.engine.mq.persist-batch-enabled} 互斥注册，防止两个
     * consumer 同时订阅同一队列。默认 Spring Rabbit simple listener factory 会由本自动配置同步打开
     * consumer batch 模式；自定义 factory 需要保持同等设置。
     */
    @Bean
    @ConditionalOnBean({
            RabbitTemplate.class,
            PersistRechargeOrderConsumerService.class,
            RechargeOrderBatchPersistencePort.class
    })
    @ConditionalOnMissingBean(RabbitPersistRechargeOrderBatchMessageListener.class)
    @ConditionalOnProperty(name = "trade.engine.mq.persist-batch-enabled", havingValue = "true")
    public RabbitPersistRechargeOrderBatchMessageListener rabbitPersistRechargeOrderBatchMessageListener(
            ObjectMapper objectMapper,
            PersistRechargeOrderConsumerService consumerService,
            RabbitTemplate rabbitTemplate,
            TradeEngineProperties properties,
            ObjectProvider<TradeEngineObservabilityMetrics> observabilityMetricsProvider) {
        return new RabbitPersistRechargeOrderBatchMessageListener(
                objectMapper,
                consumerService,
                rabbitTemplate,
                properties.getMq().getPersistRetryExchange(),
                properties.getMq().getPersistRetryRoutingKey(),
                properties.getMq().getConsumerMaxRetry(),
                observabilityMetricsProvider.getIfAvailable(TradeEngineObservabilityMetrics::noop));
    }

    /**
     * 批消费 Rabbit listener factory 后置处理器。
     *
     * <p>该处理器只触碰 Spring Boot 默认的 {@link SimpleRabbitListenerContainerFactory}。如果部署侧替换了
     * {@code rabbitListenerContainerFactory}，仍需要在自定义 factory 中保持同样的批开关、批大小和等待时间约束。
     */
    private static final class PersistBatchRabbitListenerContainerFactoryPostProcessor
            implements BeanPostProcessor, BeanFactoryAware {

        private BeanFactory beanFactory;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
            this.beanFactory = beanFactory;
        }

        @Override
        public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
            if (!DEFAULT_RABBIT_LISTENER_CONTAINER_FACTORY.equals(beanName)) {
                return bean;
            }
            /*
             * 批 listener 签名是 List<Message>，必须由 Simple 容器的 consumer-batch 模式投递。
             * 部署侧若把同名 factory 换成 Direct 或其他非 Simple 实现，批开关打开后会 silently 无法匹配，
             * 因此在启动期直接失败，而不是等消费时才发现没有批消费者。
             */
            if (!(bean instanceof SimpleRabbitListenerContainerFactory factory)) {
                throw new IllegalStateException(
                        "trade.engine.mq.persist-batch-enabled=true requires bean 'rabbitListenerContainerFactory' "
                                + "to be a SimpleRabbitListenerContainerFactory with consumer batch mode");
            }
            TradeEngineProperties properties = beanFactory.getBean(TradeEngineProperties.class);
            TradeEngineProperties.Mq mq = properties.getMq();
            int batchSize = positive(
                    mq.getPersistBatchSize(),
                    "trade.engine.mq.persist-batch-size");
            int batchReceiveTimeoutMs = positive(
                    mq.getPersistBatchReceiveTimeoutMs(),
                    "trade.engine.mq.persist-batch-receive-timeout-ms");
            int pollTimeoutMs = positive(
                    mq.getPersistBatchPollTimeoutMs(),
                    "trade.engine.mq.persist-batch-poll-timeout-ms");
            validateRabbitListenerProperties(batchSize);

            factory.setConsumerBatchEnabled(true);
            factory.setBatchSize(batchSize);
            /*
             * Spring AMQP 3.1.x：每轮 nextMessage(receiveTimeout)，batchReceiveTimeout 在两次 receive 之间检查。
             * 二者语义不同，必须拆配置；不要把 poll timeout 盲目放大到秒级，否则低流量 partial batch 也会被拖长。
             */
            factory.setReceiveTimeout((long) pollTimeoutMs);
            factory.setBatchReceiveTimeout((long) batchReceiveTimeoutMs);
            return bean;
        }

        private void validateRabbitListenerProperties(int batchSize) {
            RabbitProperties rabbitProperties = beanFactory.getBeanProvider(RabbitProperties.class).getIfAvailable();
            if (rabbitProperties == null) {
                return;
            }
            RabbitProperties.Listener listener = rabbitProperties.getListener();
            if (listener.getType() != RabbitProperties.ContainerType.SIMPLE) {
                throw new IllegalStateException(
                        "trade.engine.mq.persist-batch-enabled=true requires spring.rabbitmq.listener.type=simple");
            }
            Integer prefetch = listener.getSimple().getPrefetch();
            if (prefetch != null && prefetch < batchSize) {
                throw new IllegalStateException(
                        "spring.rabbitmq.listener.simple.prefetch must be >= trade.engine.mq.persist-batch-size");
            }
        }

        private static int positive(int value, String propertyName) {
            if (value <= 0) {
                throw new IllegalStateException(propertyName + " must be > 0 when batch persist consumer is enabled");
            }
            return value;
        }
    }

    private static final class MissingBatchPersistencePortLifecycle implements SmartLifecycle {

        @Override
        public void start() {
            throw new IllegalStateException(
                    "trade.engine.mq.persist-batch-enabled=true requires RechargeOrderBatchPersistencePort");
        }

        @Override
        public void stop() {
        }

        @Override
        public boolean isRunning() {
            return false;
        }
    }

    /**
     * 批开关打开时校验数据源 URL 含 {@code rewriteBatchedStatements=true}。
     */
    private static final class RewriteBatchedStatementsLifecycle implements SmartLifecycle {

        private final Environment environment;

        private RewriteBatchedStatementsLifecycle(Environment environment) {
            this.environment = environment;
        }

        @Override
        public void start() {
            String jdbcUrl = firstNonBlank(
                    environment.getProperty("spring.datasource.url"),
                    environment.getProperty("SPRING_DATASOURCE_URL"),
                    environment.getProperty("MYSQL_URL"));
            if (jdbcUrl == null || jdbcUrl.isBlank()) {
                throw new IllegalStateException(
                        "trade.engine.mq.persist-batch-enabled=true requires spring.datasource.url "
                                + "with rewriteBatchedStatements=true");
            }
            if (!hasRewriteBatchedStatementsEnabled(jdbcUrl)) {
                log.error(
                        "批消费已开启但 JDBC URL 未启用 rewriteBatchedStatements=true，MyBatis BATCH 将退化为逐条发送。"
                                + "请在 Nacos/部署 MYSQL_URL 中追加该参数。jdbcUrlHost={}",
                        sanitizeJdbcUrlForLog(jdbcUrl));
                throw new IllegalStateException(
                        "trade.engine.mq.persist-batch-enabled=true requires "
                                + "spring.datasource.url to contain rewriteBatchedStatements=true");
            }
        }

        @Override
        public void stop() {
        }

        @Override
        public boolean isRunning() {
            return false;
        }
    }

    static boolean hasRewriteBatchedStatementsEnabled(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            return false;
        }
        int queryIndex = jdbcUrl.indexOf('?');
        if (queryIndex < 0 || queryIndex >= jdbcUrl.length() - 1) {
            return false;
        }
        String query = jdbcUrl.substring(queryIndex + 1);
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String key = pair.substring(0, eq).trim().toLowerCase(Locale.ROOT);
            String value = pair.substring(eq + 1).trim().toLowerCase(Locale.ROOT);
            if (REWRITE_BATCHED_STATEMENTS.equals(key) && "true".equals(value)) {
                return true;
            }
        }
        return false;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static String sanitizeJdbcUrlForLog(String jdbcUrl) {
        int queryIndex = jdbcUrl.indexOf('?');
        return queryIndex < 0 ? jdbcUrl : jdbcUrl.substring(0, queryIndex);
    }
}
