package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeBalanceDeltaFlushAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisAcceptAnomalyCheckRepositoryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeFulfillmentOrderAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeNotifyTaskAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOutboxAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeNotifyTaskMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.baomidou.mybatisplus.autoconfigure.DdlApplicationRunner;
import com.baomidou.mybatisplus.autoconfigure.MybatisPlusAutoConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Clock;
import javax.sql.DataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
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
    void registersOutboxWorkerMysqlPortsWhenMappersComeFromLaterAutoConfiguration() {
        new ApplicationContextRunner()
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withConfiguration(AutoConfigurations.of(
                        TestMapperAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargeOutboxPort.class);
                    assertThat(context.getBean(RechargeOutboxPort.class))
                            .isInstanceOf(MybatisRechargeOutboxAdapter.class);
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

    @Test
    void registersAcceptAnomalyCheckRepositoryWhenMapperComesFromLaterAutoConfiguration() {
        new ApplicationContextRunner()
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(Clock.class, Clock::systemUTC)
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
            return mapperProxy(SqlSessionFactory.class);
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
        RechargeOutboxMapper rechargeOutboxMapper() {
            return mapperProxy(RechargeOutboxMapper.class);
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
        RechargeNotifyTaskMapper rechargeNotifyTaskMapper() {
            return mapperProxy(RechargeNotifyTaskMapper.class);
        }

        @Bean
        RechargeAcceptAnomalyMapper rechargeAcceptAnomalyMapper() {
            return mapperProxy(RechargeAcceptAnomalyMapper.class);
        }
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
