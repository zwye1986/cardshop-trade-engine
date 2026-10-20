package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.baomidou.mybatisplus.autoconfigure.DdlApplicationRunner;
import com.baomidou.mybatisplus.autoconfigure.MybatisPlusAutoConfiguration;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushPort;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderBatchPersistencePort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.application.recharge.RechargePersistOutboxAsyncWriter;
import com.cardshop.trade.engine.application.recharge.FulfillmentDispatchStatus;
import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.application.recharge.OrderFulfillmentStatusUpdate;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisAcceptAnomalyCheckRepositoryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisBatchRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeBalanceDeltaFlushAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeDispatchOutboxAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeFulfillmentOrderAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeNotifyTaskAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargePersistOutboxAsyncWriter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartProductMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McProductMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeBalanceDeltaOutboxMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeNotifyTaskMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Clock;
import javax.sql.DataSource;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;

/**
 * MyBatis 落库端口后置自动配置测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。真实运行时
 * Mapper Bean 来自 MyBatis 自动配置和 mapper scanner，普通业务 {@code @Configuration} 过早使用
 * {@code @ConditionalOnBean(McOrderMapper.class)} 一类条件时，可能在 Mapper BeanDefinition 注册前被判定为不满足，
 * 继而跳过 {@link RechargeOrderPersistencePort}、Consumer 核心服务和 Rabbit listener。这里用后置自动配置模拟
 * MyBatis mapper 已就绪后的装配顺序，防止 MySQL 落库端口再次静默缺失。
 */
class TradeEngineMybatisPersistenceAutoConfigurationTest {

    @Test
    void registersPersistencePortWhenMappersComeFromLaterAutoConfiguration() {
        new ApplicationContextRunner()
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withConfiguration(AutoConfigurations.of(
                        TestMapperAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargeOrderPersistencePort.class);
                    assertThat(context.getBean(RechargeOrderPersistencePort.class))
                            .isInstanceOf(MybatisRechargeOrderPersistenceAdapter.class);
                });
    }

    @Test
    void skipsBatchPersistencePortByDefault() {
        new ApplicationContextRunner()
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withConfiguration(AutoConfigurations.of(
                        TestMapperAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class))
                .run(context -> assertThat(context).doesNotHaveBean(RechargeOrderBatchPersistencePort.class));
    }

    @Test
    void registersBatchPersistencePortOnlyWhenBatchSwitchIsEnabled() {
        new ApplicationContextRunner()
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withPropertyValues("trade.engine.mq.persist-batch-enabled=true")
                .withConfiguration(AutoConfigurations.of(
                        TestMapperAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargeOrderBatchPersistencePort.class);
                    assertThat(context.getBean(RechargeOrderBatchPersistencePort.class))
                            .isInstanceOf(MybatisBatchRechargeOrderPersistenceAdapter.class);
                });
    }

    @Test
    void registersOutboxAsyncWriterWithIndependentDataSourceWhenBothSwitchesAreEnabled() {
        new ApplicationContextRunner()
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withPropertyValues(
                        "spring.datasource.url=jdbc:mysql://127.0.0.1:3306/cardshop_trade_engine_test",
                        "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
                        "trade.engine.mq.persist-batch-enabled=true",
                        "trade.engine.persist.outbox-async-enabled=true",
                        "trade.engine.persist.outbox-writer-hikari-max=3")
                .withConfiguration(AutoConfigurations.of(
                        DataSourceAutoConfiguration.class,
                        DataSourceTransactionManagerAutoConfiguration.class,
                        TestMapperAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(RechargePersistOutboxAsyncWriter.class);
                    assertThat(context.getBean(RechargePersistOutboxAsyncWriter.class))
                            .isInstanceOf(MybatisRechargePersistOutboxAsyncWriter.class);
                    HikariDataSource writerDataSource =
                            context.getBean("persistOutboxWriterDataSource", HikariDataSource.class);
                    assertThat(writerDataSource).isNotSameAs(context.getBean("dataSource", DataSource.class));
                    assertThat(writerDataSource.getMaximumPoolSize()).isEqualTo(3);
                    assertThat(writerDataSource.getPoolName()).isEqualTo("persist-outbox-writer");
                });
    }

    /**
     * writer 独立连接池必须继承主池 {@code spring.datasource.hikari.*} 的超时/生命周期调优，
     * 池大小仍由 {@code outbox-writer-hikari-max} 独立控制，防止 writer 静默回退 Hikari 默认值。
     */
    @Test
    void outboxWriterDataSourceInheritsPrimaryHikariTimeoutTuning() {
        new ApplicationContextRunner()
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withPropertyValues(
                        "spring.datasource.url=jdbc:mysql://127.0.0.1:3306/cardshop_trade_engine_test",
                        "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
                        "spring.datasource.hikari.connection-timeout=2500",
                        "spring.datasource.hikari.max-lifetime=600000",
                        "trade.engine.mq.persist-batch-enabled=true",
                        "trade.engine.persist.outbox-async-enabled=true",
                        "trade.engine.persist.outbox-writer-hikari-max=3")
                .withConfiguration(AutoConfigurations.of(
                        DataSourceAutoConfiguration.class,
                        DataSourceTransactionManagerAutoConfiguration.class,
                        TestMapperAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    HikariDataSource primaryDataSource = context.getBean("dataSource", HikariDataSource.class);
                    HikariDataSource writerDataSource =
                            context.getBean("persistOutboxWriterDataSource", HikariDataSource.class);
                    assertThat(primaryDataSource.getConnectionTimeout()).isEqualTo(2500);
                    assertThat(writerDataSource.getConnectionTimeout()).isEqualTo(2500);
                    assertThat(writerDataSource.getMaxLifetime()).isEqualTo(600000);
                    assertThat(writerDataSource.getMaximumPoolSize()).isEqualTo(3);
                    assertThat(writerDataSource.getMaximumPoolSize())
                            .isNotEqualTo(primaryDataSource.getMaximumPoolSize());
                });
    }

    @Test
    void failsFastWhenOutboxAsyncWriterHikariMaxIsNotPositive() {
        new ApplicationContextRunner()
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withPropertyValues(
                        "spring.datasource.url=jdbc:mysql://127.0.0.1:3306/cardshop_trade_engine_test",
                        "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
                        "trade.engine.mq.persist-batch-enabled=true",
                        "trade.engine.persist.outbox-async-enabled=true",
                        "trade.engine.persist.outbox-writer-hikari-max=0")
                .withConfiguration(AutoConfigurations.of(
                        DataSourceAutoConfiguration.class,
                        DataSourceTransactionManagerAutoConfiguration.class,
                        TestMapperAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class))
                .run(context -> assertThat(context)
                        .hasFailed()
                        .getFailure()
                        .hasMessageContaining("trade.engine.persist.outbox-writer-hikari-max must be > 0"));
    }

    @Test
    void registersOutboxWorkerMysqlPortsWhenMappersComeFromLaterAutoConfiguration() {
        new ApplicationContextRunner()
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withConfiguration(AutoConfigurations.of(
                        TestMapperAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargeOutboxPort.class);
                    assertThat(context.getBean(RechargeOutboxPort.class))
                            .isInstanceOf(MybatisRechargeDispatchOutboxAdapter.class);
                    assertThat(context).hasSingleBean(RechargeFulfillmentOrderPort.class);
                    assertThat(context.getBean(RechargeFulfillmentOrderPort.class))
                            .isInstanceOf(MybatisRechargeFulfillmentOrderAdapter.class);
                    assertThat(context).hasSingleBean(RechargeNotifyTaskPort.class);
                    assertThat(context).hasSingleBean(RechargeNotifyWorkerTaskPort.class);
                    assertThat(context.getBean(RechargeNotifyTaskPort.class))
                            .isSameAs(context.getBean(RechargeNotifyWorkerTaskPort.class))
                            .isInstanceOf(MybatisRechargeNotifyTaskAdapter.class);
                });
    }

    /** 晚扫描必须验证实际终态写入，避免端口存在但凭据依赖丢失；见详细设计名创实施约束。 */
    @Test
    void lateConfiguredFulfillmentPortCommitsMinisoReceiptAndSuccessTogether() {
        new ApplicationContextRunner()
                .withInitializer(context -> AutoConfigurationPackages.register(
                        (BeanDefinitionRegistry) context.getBeanFactory(),
                        "com.cardshop.trade.engine.infrastructure.persistence.mapper"))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withPropertyValues(
                        "spring.datasource.url=jdbc:h2:mem:" + java.util.UUID.randomUUID()
                                + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE",
                        "spring.datasource.driver-class-name=org.h2.Driver",
                        "trade.engine.supplier.miniso.callback.processing-enabled=false")
                .withConfiguration(AutoConfigurations.of(
                        DataSourceAutoConfiguration.class,
                        MybatisPlusDdlRunnerCompatibilityAutoConfiguration.class,
                        MybatisPlusAutoConfiguration.class,
                        DataSourceTransactionManagerAutoConfiguration.class,
                        TransactionAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class,
                        MinisoWriteOffAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var jdbc = new JdbcTemplate(context.getBean(DataSource.class));
                    jdbc.execute("""
                            create table mc_order(order_Id varchar(100) primary key, org_code varchar(64),
                                out_orderId varchar(100), account varchar(64), num int, supplier_channel varchar(32),
                                order_status int, data_status varchar(32), flow_id bigint, fail_code varchar(100),
                                fail_reason varchar(1000), remark varchar(1000), edit_time timestamp)
                            """);
                    jdbc.execute("""
                            create table recharge_order_ext(order_id varchar(100) primary key, org_code varchar(64),
                                out_order_sn varchar(100), supplier_batch_id varchar(64), persist_status varchar(32),
                                dispatch_status varchar(32), last_error varchar(1000), updated_at timestamp,
                                voucher_use_state varchar(8))
                            """);
                    jdbc.execute("""
                            create table recharge_miniso_receipt(order_id varchar(100) primary key,
                                org_code varchar(64), batch_id int, coupon_code varchar(128) unique, phone varchar(64),
                                begin_time varchar(19), end_time varchar(19), coupon_status int,
                                created_at timestamp default current_timestamp,
                                updated_at timestamp default current_timestamp)
                            """);
                    jdbc.update("""
                            insert into mc_order(order_Id, org_code, out_orderId, account, num, supplier_channel,
                                order_status) values('O-LATE', 'A39', 'OUT-LATE', '13800138000', 1, 'MINISO', 13)
                            """);
                    jdbc.update("""
                            insert into recharge_order_ext(order_id, org_code, out_order_sn, supplier_batch_id,
                                persist_status) values('O-LATE', 'A39', 'OUT-LATE', '100023', 'DB_PERSISTED')
                            """);
                    var receipt = new MinisoFulfillmentReceipt("O-LATE", "A39", 100023, "TEST-LATE-COUPON",
                            "13800138000", "2026-10-01 00:00:00", "2026-12-01 00:00:00", 0);
                    var update = new OrderFulfillmentStatusUpdate("O-LATE", FulfillmentDispatchStatus.SUCCESS,
                            12, "PROCESSED", null, null, null, "confirmed").withMinisoReceipt(receipt);
                    assertThatCode(() -> context.getBean(RechargeFulfillmentOrderPort.class)
                            .updateFulfillmentStatus(update)).doesNotThrowAnyException();
                    assertThat(jdbc.queryForObject(
                            "select coupon_code from recharge_miniso_receipt where order_id='O-LATE'", String.class))
                            .isEqualTo("TEST-LATE-COUPON");
                    assertThat(jdbc.queryForObject(
                            "select order_status from mc_order where order_Id='O-LATE'", Integer.class)).isEqualTo(12);
                    assertThat(jdbc.queryForObject(
                            "select dispatch_status from recharge_order_ext where order_id='O-LATE'", String.class))
                            .isEqualTo("SUCCESS");
                });
    }

    @Test
    void registersAcceptAnomalyCheckRepositoryWhenMapperComesFromLaterAutoConfiguration() {
        new ApplicationContextRunner()
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withConfiguration(AutoConfigurations.of(
                        TestMapperAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class))
                .run(context -> {
                    /*
                     * 参见详细设计「6.3 Redis HA Guard 与状态未知窗口」。test188 的 XXL-Job 冒烟已暴露：
                     * handler 能注册，但异常核查 job facade 因 repository 端口没有在后置配置阶段创建而缺失。
                     * 这里要求 MyBatis mapper scan 完成后兜底创建真实核查仓储，避免调度入口只剩空壳。
                     */
                    assertThat(context).hasSingleBean(AcceptAnomalyCheckRepositoryPort.class);
                    assertThat(context.getBean(AcceptAnomalyCheckRepositoryPort.class))
                            .isInstanceOf(MybatisAcceptAnomalyCheckRepositoryAdapter.class);
                });
    }

    @Test
    void registersBalanceDeltaFlushPortAfterTransactionManagerAutoConfiguration() {
        new ApplicationContextRunner()
                .withBean(DataSource.class, TradeEngineMybatisPersistenceAutoConfigurationTest::dataSource)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withConfiguration(AutoConfigurations.of(
                        TestMapperAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class,
                        DataSourceTransactionManagerAutoConfiguration.class))
                .run(context -> {
                    /*
                     * 参见高可用设计「17.2 分片规则」。BAL-001 在 test188 dev 暴露过
                     * `PlatformTransactionManager` 已实例化、但余额刷账端口因自动配置顺序过早而没有注册，
                     * 人工 job 固定返回 scanned=0。这里故意把本自动配置写在事务管理器自动配置之前，
                     * 要求 @AutoConfiguration(after=...) 能把真实端口延后到事务管理器之后创建。
                     */
                    assertThat(context).hasSingleBean(RechargeBalanceDeltaFlushPort.class);
                    assertThat(context.getBean(RechargeBalanceDeltaFlushPort.class))
                            .isInstanceOf(MybatisRechargeBalanceDeltaFlushAdapter.class);
                });
    }

    @Nested
    class MybatisPlusDdlRunnerCompatibility {

        @Test
        void providesRealDdlRunnerWhenNoMybatisPlusDdlScriptsAreConfigured() {
            new ApplicationContextRunner()
                    .withBean(DataSource.class, TradeEngineMybatisPersistenceAutoConfigurationTest::dataSource)
                    .withConfiguration(AutoConfigurations.of(
                            MybatisPlusDdlRunnerCompatibilityAutoConfiguration.class,
                            MybatisPlusAutoConfiguration.class))
                    .run(context -> {
                        assertThat(context).hasNotFailed();

                        Object runner = context.getBean("ddlApplicationRunner");
                        assertThat(runner)
                                .isInstanceOf(ApplicationRunner.class)
                                .isInstanceOf(DdlApplicationRunner.class);
                        assertThat(runner.getClass().getName())
                                .isNotEqualTo("org.springframework.beans.factory.support.NullBean");
                        ((ApplicationRunner) runner).run(new DefaultApplicationArguments());
                    });
        }
    }

    @AutoConfiguration(before = TradeEngineMybatisPersistenceAutoConfiguration.class)
    static class TestMapperAutoConfiguration {

        @Bean
        SqlSessionFactory sqlSessionFactory() {
            return configuredSqlSessionFactory();
        }

        @Bean
        McOrderMapper mcOrderMapper() {
            return mapperProxy(McOrderMapper.class);
        }

        @Bean
        RechargeOrderExtMapper rechargeOrderExtMapper() {
            return mapperProxy(RechargeOrderExtMapper.class);
        }

        @Bean
        RechargeDispatchOutboxMapper rechargeDispatchOutboxMapper() {
            return mapperProxy(RechargeDispatchOutboxMapper.class);
        }

        @Bean
        RechargeBalanceDeltaOutboxMapper rechargeBalanceDeltaOutboxMapper() {
            return mapperProxy(RechargeBalanceDeltaOutboxMapper.class);
        }

        @Bean
        McDepartRecordMapper mcDepartRecordMapper() {
            return mapperProxy(McDepartRecordMapper.class);
        }

        @Bean
        McExtDepartMapper mcExtDepartMapper() {
            return mapperProxy(McExtDepartMapper.class);
        }

        @Bean
        McOrderSupplierFlowMapper mcOrderSupplierFlowMapper() {
            return mapperProxy(McOrderSupplierFlowMapper.class);
        }

        @Bean
        McDepartProductMapper mcDepartProductMapper() {
            return mapperProxy(McDepartProductMapper.class);
        }

        @Bean
        McProductMapper mcProductMapper() {
            return mapperProxy(McProductMapper.class);
        }

        @Bean
        RechargeNotifyTaskMapper rechargeNotifyTaskMapper() {
            return mapperProxy(RechargeNotifyTaskMapper.class);
        }

        @Bean
        RechargeAcceptAnomalyMapper rechargeAcceptAnomalyMapper() {
            return mapperProxy(RechargeAcceptAnomalyMapper.class);
        }
    }

    private static SqlSessionFactory configuredSqlSessionFactory() {
        Configuration configuration = new Configuration();
        configuration.setEnvironment(new Environment(
                "configuration-test",
                new JdbcTransactionFactory(),
                dataSource()));
        configuration.addMapper(McOrderMapper.class);
        configuration.addMapper(RechargeOrderExtMapper.class);
        configuration.addMapper(RechargeDispatchOutboxMapper.class);
        configuration.addMapper(RechargeBalanceDeltaOutboxMapper.class);
        configuration.addMapper(McDepartRecordMapper.class);
        Object proxy = Proxy.newProxyInstance(
                SqlSessionFactory.class.getClassLoader(),
                new Class<?>[]{SqlSessionFactory.class},
                (ignored, method, args) -> {
                    if ("getConfiguration".equals(method.getName())) {
                        return configuration;
                    }
                    throw new AssertionError(
                            "configuration test should not invoke SqlSessionFactory method " + method.getName());
                });
        return SqlSessionFactory.class.cast(proxy);
    }

    private static <T> T mapperProxy(Class<T> mapperType) {
        Object proxy = Proxy.newProxyInstance(
                mapperType.getClassLoader(),
                new Class<?>[]{mapperType},
                (ignored, method, args) -> {
                    throw new AssertionError("configuration test should not invoke mapper method " + method.getName());
                });
        return mapperType.cast(proxy);
    }

    private static DataSource dataSource() {
        return new AbstractDataSource() {
            @Override
            public Connection getConnection() {
                throw new AssertionError("configuration test should not open a database connection");
            }

            @Override
            public Connection getConnection(String username, String password) {
                throw new AssertionError("configuration test should not open a database connection");
            }
        };
    }
}
