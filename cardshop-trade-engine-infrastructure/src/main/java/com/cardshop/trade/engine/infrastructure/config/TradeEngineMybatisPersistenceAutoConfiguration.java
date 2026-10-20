package com.cardshop.trade.engine.infrastructure.config;

import com.baomidou.mybatisplus.autoconfigure.MybatisPlusAutoConfiguration;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.config.GlobalConfig;
import com.baomidou.mybatisplus.core.injector.DefaultSqlInjector;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.BalanceDeltaFlushOutboxPort;
import com.cardshop.trade.engine.application.recharge.PersistedOrderStatePort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushPort;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderBatchPersistencePort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.application.recharge.RechargePersistOutboxAsyncWriter;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.persistence.MinisoReceiptStore;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisAcceptAnomalyCheckRepositoryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisBalanceDeltaOutboxWriteAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisBatchRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisPersistedOrderStateAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeBalanceDeltaFlushAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeDispatchOutboxAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeFulfillmentOrderAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeNotifyTaskAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargePersistOutboxAsyncWriter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeBalanceDeltaOutboxMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeNotifyTaskMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * MyBatis 落库端口的后置自动配置。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。真实 dev/test/prod
 * 环境中的 Mapper Bean 由 MyBatis-Plus 自动配置和 mapper scanner 注册；普通业务 {@code @Configuration} 早于
 * {@link MybatisPlusAutoConfiguration} 解析时，按 Mapper 做 {@code @ConditionalOnBean} 可能被过早判定为不满足，
 * 从而跳过 {@link RechargeOrderPersistencePort}。该端口缺失会继续导致
 * {@code PersistRechargeOrderConsumerService} 和 Rabbit listener 缺失，最终表现为应用健康但 RabbitMQ 没有
 * connection/consumer。
 *
 * <p>因此 MySQL 异步落库端口在 MyBatis 自动配置之后再兜底创建。D12 余额刷账端口还依赖事务管理器，
 * 必须同时排在 Spring Boot 事务管理器自动配置之后；否则 test188 dev 会出现应用已有
 * {@link PlatformTransactionManager} 实例、但 {@link RechargeBalanceDeltaFlushPort} 条件判断过早失败，
 * 人工刷账一直 scanned=0。基础设施普通配置类中的同名 Bean 仍保留给 slice test 或显式提供 Mapper 的局部上下文；
     * 本自动配置只要求 {@link SqlSessionFactory} 已经存在，然后把落库必需 Mapper 作为构造参数直接注入
     *（含拆表后的 {@link RechargeDispatchOutboxMapper} 与 {@link RechargeBalanceDeltaOutboxMapper}）。
     * 若 mapper scan 漏掉任一 Mapper，dev/test/prod 应在启动期 fail-fast，不能静默缺失 Consumer 后继续对外显示健康。
     */
@AutoConfiguration(after = {
        MybatisPlusAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        TransactionAutoConfiguration.class
})
@ConditionalOnClass({MybatisPlusAutoConfiguration.class, MybatisRechargeOrderPersistenceAdapter.class})
public class TradeEngineMybatisPersistenceAutoConfiguration {

    /** 密钥持久化依赖MySQL；未提供独立主密钥时装配不可用端口，入口拒绝签发，参见详细设计3.1。 */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort.class)
    public com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort v12CredentialStore(
            org.springframework.beans.factory.ObjectProvider<com.cardshop.trade.engine.infrastructure.persistence.mapper.V12SigningKeyMapper> mapper,
            org.springframework.beans.factory.ObjectProvider<org.springframework.transaction.PlatformTransactionManager> transactionManager,
            TradeEngineProperties properties) {
        if (properties.getV1().getSigningKeyEncryptionKey().isBlank()) {
            return com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort.unavailable();
        }
        return new com.cardshop.trade.engine.infrastructure.persistence.MybatisV12SigningCredentialStore(mapper.getObject(),
                new com.cardshop.trade.engine.infrastructure.security.V12KeyEnvelope(
                        properties.getV1().getSigningKeyEncryptionKey()), transactionManager.getObject());
    }


    /**
     * 创建 Redis/MQ 到 MySQL 异步落库端口。
     *
     * <p>只以 {@link SqlSessionFactory} 作为 MyBatis 就绪条件，不再以 Mapper Bean 做条件判断。Mapper 作为方法参数
     * 注入，能够在所有 BeanDefinition 注册完成后的实例化阶段解析；若缺失则启动失败，避免半闭环消费 MQ 消息。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」：落库事务写
     * {@code recharge_dispatch_outbox}（「9.1 派单 Outbox」）与
     * {@code recharge_balance_delta_outbox}（「9.2 余额 Delta 刷账 Outbox」），不再写旧混合表。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(RechargeOrderPersistencePort.class)
    public RechargeOrderPersistencePort rechargeOrderPersistencePort(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            RechargeDispatchOutboxMapper dispatchOutboxMapper,
            RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper,
            McDepartRecordMapper departRecordMapper,
            ObjectMapper objectMapper,
            TradeEngineProperties properties) {
        return new MybatisRechargeOrderPersistenceAdapter(
                orderMapper,
                extMapper,
                dispatchOutboxMapper,
                balanceDeltaOutboxMapper,
                departRecordMapper,
                objectMapper,
                properties.getFulfillment().dispatchInitialDelay());
    }

    /**
     * 创建 Redis/MQ 到 MySQL 异步落库批写端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。批端口独立于单条端口，
     * 使用 mapper 显式 multi-row SQL 写入拆表后的派单/余额 outbox，并分别上报 phase，避免批执行器的延迟
     * row count 影响单条路径中已有的控制流。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(RechargeOrderBatchPersistencePort.class)
    @ConditionalOnProperty(name = "trade.engine.mq.persist-batch-enabled", havingValue = "true")
    public RechargeOrderBatchPersistencePort rechargeOrderBatchPersistencePort(
            @Qualifier("sqlSessionFactory") SqlSessionFactory sqlSessionFactory,
            ObjectMapper objectMapper,
            ObjectProvider<TradeEngineObservabilityMetrics> observabilityMetricsProvider,
            TradeEngineProperties properties) {
        return new MybatisBatchRechargeOrderPersistenceAdapter(
                sqlSessionFactory,
                objectMapper,
                observabilityMetricsProvider.getIfAvailable(TradeEngineObservabilityMetrics::noop),
                properties.getFulfillment().dispatchInitialDelay());
    }

    /**
     * 创建 persist outbox 异步 writer 的独立连接池。
     *
     * <p>参见详细设计「8.3 异步落库 Consumer · Outbox 异步落库模式」。该连接池复用主数据源连接参数，
     * 但必须是独立 {@link HikariDataSource} 实例，避免 outbox INSERT/ODKU 与 timer claim 的锁等待回压到
     * 订单事实事务的连接获取。池大小由 {@code trade.engine.persist.outbox-writer-hikari-max} 独立控制；
     * 连接超时/生命周期调优继承主池（例如 Nacos 中 {@code spring.datasource.hikari.connection-timeout=2000}），
     * 防止 writer 池静默回退 Hikari 默认值，导致故障窗口行为与事实事务主池不一致。
     */
    @Bean(name = "persistOutboxWriterDataSource")
    @ConditionalOnBean(DataSourceProperties.class)
    @ConditionalOnMissingBean(name = "persistOutboxWriterDataSource")
    @ConditionalOnProperty(
            name = {
                    "trade.engine.mq.persist-batch-enabled",
                    "trade.engine.persist.outbox-async-enabled"
            },
            havingValue = "true")
    public HikariDataSource persistOutboxWriterDataSource(
            DataSourceProperties dataSourceProperties,
            @Qualifier("dataSource") DataSource dataSource,
            TradeEngineProperties properties) {
        int maximumPoolSize = properties.getPersist().getOutboxWriterHikariMax();
        if (maximumPoolSize <= 0) {
            throw new IllegalStateException("trade.engine.persist.outbox-writer-hikari-max must be > 0");
        }
        HikariDataSource writerDataSource = dataSourceProperties
                .initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
        writerDataSource.setPoolName("persist-outbox-writer");
        writerDataSource.setMaximumPoolSize(maximumPoolSize);
        if (dataSource instanceof HikariDataSource primaryHikari) {
            writerDataSource.setConnectionTimeout(primaryHikari.getConnectionTimeout());
            writerDataSource.setValidationTimeout(primaryHikari.getValidationTimeout());
            writerDataSource.setIdleTimeout(primaryHikari.getIdleTimeout());
            writerDataSource.setMaxLifetime(primaryHikari.getMaxLifetime());
            writerDataSource.setKeepaliveTime(primaryHikari.getKeepaliveTime());
            if (primaryHikari.getConnectionTestQuery() != null) {
                writerDataSource.setConnectionTestQuery(primaryHikari.getConnectionTestQuery());
            }
        }
        if (writerDataSource == dataSource) {
            throw new IllegalStateException("persist outbox writer DataSource must be independent from primary DataSource");
        }
        return writerDataSource;
    }

    /**
     * 创建 writer 专用 SqlSessionFactory，只注册两张 outbox Mapper。
     */
    @Bean(name = "persistOutboxWriterSqlSessionFactory", autowireCandidate = false)
    @ConditionalOnBean(name = "persistOutboxWriterDataSource")
    @ConditionalOnMissingBean(name = "persistOutboxWriterSqlSessionFactory")
    public SqlSessionFactory persistOutboxWriterSqlSessionFactory(
            ApplicationContext applicationContext) throws Exception {
        DataSource dataSource = applicationContext.getBean("persistOutboxWriterDataSource", DataSource.class);
        MybatisConfiguration configuration = new MybatisConfiguration();
        configuration.addMapper(RechargeDispatchOutboxMapper.class);
        configuration.addMapper(RechargeBalanceDeltaOutboxMapper.class);
        GlobalConfig globalConfig = new GlobalConfig();
        globalConfig.setSqlInjector(new DefaultSqlInjector());

        MybatisSqlSessionFactoryBean factoryBean = new MybatisSqlSessionFactoryBean();
        factoryBean.setDataSource(dataSource);
        factoryBean.setConfiguration(configuration);
        factoryBean.setGlobalConfig(globalConfig);
        return factoryBean.getObject();
    }

    /**
     * 创建 outbox 异步 writer。
     */
    @Bean
    @ConditionalOnBean(name = {
            "persistOutboxWriterDataSource",
            "persistOutboxWriterSqlSessionFactory"
    })
    @ConditionalOnMissingBean(RechargePersistOutboxAsyncWriter.class)
    public RechargePersistOutboxAsyncWriter rechargePersistOutboxAsyncWriter(
            ApplicationContext applicationContext,
            ObjectMapper objectMapper,
            ObjectProvider<TradeEngineObservabilityMetrics> observabilityMetricsProvider,
            TradeEngineProperties properties) {
        SqlSessionFactory sqlSessionFactory = applicationContext.getBean(
                "persistOutboxWriterSqlSessionFactory",
                SqlSessionFactory.class);
        DataSource dataSource = applicationContext.getBean("persistOutboxWriterDataSource", DataSource.class);
        PlatformTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        return new MybatisRechargePersistOutboxAsyncWriter(
                sqlSessionFactory,
                objectMapper,
                observabilityMetricsProvider.getIfAvailable(TradeEngineObservabilityMetrics::noop),
                properties.getFulfillment().dispatchInitialDelay(),
                properties.getPersist().isOutboxWriterBatchEnabled(),
                transactionManager);
    }

    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(PersistedOrderStatePort.class)
    public PersistedOrderStatePort persistedOrderStatePort(RechargeOrderExtMapper extMapper) {
        return new MybatisPersistedOrderStateAdapter(extMapper);
    }

    /**
     * 创建派单履约 outbox 端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。真实 dev/test/prod
     * 中 Mapper BeanDefinition 由 MyBatis-Plus 后置注册；这里以 {@link SqlSessionFactory} 作为 MyBatis
     * 就绪条件，并把 Mapper 放到方法参数上解析，避免普通配置类过早按 Mapper 条件判断导致 outbox worker
     * 缺少 {@link RechargeOutboxPort}。余额入队见 {@link #balanceDeltaFlushOutboxPort}（「9.2」）。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(RechargeOutboxPort.class)
    public MybatisRechargeDispatchOutboxAdapter rechargeOutboxPort(
            RechargeDispatchOutboxMapper dispatchOutboxMapper,
            ObjectMapper objectMapper,
            Clock clock) {
        return new MybatisRechargeDispatchOutboxAdapter(dispatchOutboxMapper, objectMapper, clock);
    }

    /**
     * 创建余额 Delta 刷账 outbox 写入端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」。
     * 履约 Worker 释放预扣后通过本端口写入 {@code recharge_balance_delta_outbox}；与刷账扫描端口分开，
     * 避免派单适配器再混写余额事件。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(BalanceDeltaFlushOutboxPort.class)
    public BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort(
            RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper,
            Clock clock) {
        return new MybatisBalanceDeltaOutboxWriteAdapter(balanceDeltaOutboxMapper, clock);
    }

    /**
     * 创建供应商履约订单读写端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和「9.3 作废设计」。
     * SUP/INV 联调必须能读取旧订单、写供应商流水并推进 `recharge_order_ext.dispatch_status`；若任一 Mapper
     * 缺失，应在启动期暴露依赖错误，而不是静默跳过 worker。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(RechargeFulfillmentOrderPort.class)
    public RechargeFulfillmentOrderPort rechargeFulfillmentOrderPort(
            ObjectProvider<MinisoReceiptStore> minisoReceipts,
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            McOrderSupplierFlowMapper flowMapper,
            ObjectMapper objectMapper,
            Clock clock) {
        // 参见详细设计「名创 MINISO 专属协议实施约束」：后置端口也必须接入凭据事务，不能遗漏普通配置的依赖。
        return new MybatisRechargeFulfillmentOrderAdapter(
                orderMapper,
                extMapper,
                flowMapper,
                objectMapper,
                clock,
                minisoReceipts.getIfAvailable());
    }

    /**
     * 创建商户回调任务端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。同一个 MyBatis adapter 同时承担
     * outbox 生成回调任务的写端和 notify worker 抢占/重试的读写端，因此返回具体类型，确保 Spring 能同时按
     * {@link RechargeNotifyTaskPort} 和
     * {@link com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerTaskPort} 匹配到同一个 Bean。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(RechargeNotifyTaskPort.class)
    public MybatisRechargeNotifyTaskAdapter rechargeNotifyTaskPort(
            RechargeNotifyTaskMapper notifyTaskMapper,
            ObjectMapper objectMapper,
            Clock clock) {
        return new MybatisRechargeNotifyTaskAdapter(notifyTaskMapper, objectMapper, clock);
    }

    /**
     * 创建接单异常自动核查仓储端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」和
     * 「14.3 MySQL 表字段」。2026-07-11 test188 的 XXL-Job 冒烟暴露过
     * {@code acceptAnomalyAutoCheckJob} handler 已注册但 job facade 缺失的问题；根因之一是普通
     * {@code TradeEngineInfrastructureConfiguration} 早于 MyBatis mapper scanner 条件判断，导致
     * {@link AcceptAnomalyCheckRepositoryPort} 没有注册，进而无法创建异常核查 service/job。
     *
     * <p>这里沿用本类其它 MyBatis 端口的模式：只以 {@link SqlSessionFactory} 作为 MyBatis 就绪信号，
     * {@link RechargeAcceptAnomalyMapper} 作为方法参数在实例化阶段解析。若 mapper scan 漏配，应启动失败，
     * 不能让调度入口在运行时才表现为 “job is not available”。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(AcceptAnomalyCheckRepositoryPort.class)
    public AcceptAnomalyCheckRepositoryPort acceptAnomalyCheckRepositoryPort(
            RechargeAcceptAnomalyMapper mapper,
            ObjectMapper objectMapper,
            Clock clock) {
        return new MybatisAcceptAnomalyCheckRepositoryAdapter(mapper, objectMapper, clock);
    }

    @Bean
    @ConditionalOnBean({SqlSessionFactory.class, PlatformTransactionManager.class})
    @ConditionalOnMissingBean(RechargeBalanceDeltaFlushPort.class)
    public RechargeBalanceDeltaFlushPort rechargeBalanceDeltaFlushPort(
            RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper,
            McExtDepartMapper extDepartMapper,
            Clock clock,
            TradeEngineProperties properties,
            @Qualifier("transactionManager") PlatformTransactionManager transactionManager) {
        // 余额刷账租约/退避/批大小读 trade.engine.outbox.*；参见详细设计「9.2 余额 Delta 刷账 Outbox」。
        var outbox = properties.getOutbox();
        return new MybatisRechargeBalanceDeltaFlushAdapter(
                balanceDeltaOutboxMapper,
                extDepartMapper,
                clock,
                Duration.ofSeconds(outbox.getBalanceDeltaClaimLeaseSeconds()),
                outbox.balanceDeltaRetryBackoffDurations(),
                outbox.getBalanceDeltaFlushBatchSize(),
                transactionManager);
    }
}
