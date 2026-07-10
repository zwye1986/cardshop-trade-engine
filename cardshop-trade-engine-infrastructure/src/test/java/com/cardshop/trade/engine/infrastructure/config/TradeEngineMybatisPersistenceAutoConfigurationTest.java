package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeFulfillmentOrderAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeNotifyTaskAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOutboxAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper;
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
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.AbstractDataSource;

/**
 * MyBatis 落库端口后置自动配置测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。真实运行时
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
        McOrderSupplierFlowMapper mcOrderSupplierFlowMapper() {
            return mapperProxy(McOrderSupplierFlowMapper.class);
        }

        @Bean
        RechargeNotifyTaskMapper rechargeNotifyTaskMapper() {
            return mapperProxy(RechargeNotifyTaskMapper.class);
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
