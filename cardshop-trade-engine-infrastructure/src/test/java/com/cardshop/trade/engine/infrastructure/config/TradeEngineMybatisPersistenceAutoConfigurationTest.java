package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Proxy;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

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
                .withConfiguration(AutoConfigurations.of(
                        TestMapperAutoConfiguration.class,
                        TradeEngineMybatisPersistenceAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargeOrderPersistencePort.class);
                    assertThat(context.getBean(RechargeOrderPersistencePort.class))
                            .isInstanceOf(MybatisRechargeOrderPersistenceAdapter.class);
                });
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
}
