package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcilePort;
import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeBalanceReconcileAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import com.cardshop.trade.engine.infrastructure.redis.RedisRechargeCacheRepairAdapter;
import java.lang.reflect.Proxy;
import java.time.Clock;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;

/**
 * Redis 后置自动配置回归测试。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.10 查单设计」和「20.13.11 作废设计」。
 * test188 dev 曾出现普通基础设施配置早于 RedisTemplate/Mapper 条件判断，导致
 * {@link RechargeOrderQueryPort} 缺失，V1 查询/作废入口只能拿到 bootstrap fallback，表现为
 * `/v1/cardapi/order/invalid` 返回“新交易引擎作废暂未启用”。这些端口必须放在 Redis 后置自动配置中兜底。
 */
class TradeEngineRedisPendingAutoConfigurationTest {

    @Test
    void registersRechargeOrderQueryPortAfterRedisTemplateIsAvailable() {
        new ApplicationContextRunner()
                .withBean(ReactiveStringRedisTemplate.class, TradeEngineRedisPendingAutoConfigurationTest::redisTemplate)
                .withBean(RechargeRedisKeyBuilder.class, () -> new RechargeRedisKeyBuilder("cardshop:trade", 128))
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withConfiguration(AutoConfigurations.of(TradeEngineRedisPendingAutoConfiguration.class))
                .run(context -> assertThat(context).hasSingleBean(RechargeOrderQueryPort.class));
    }

    @Test
    void registersBalanceReconcileAndCacheRepairPortsWhenRedisAndMappersAreAvailable() {
        new ApplicationContextRunner()
                .withBean(ReactiveStringRedisTemplate.class, TradeEngineRedisPendingAutoConfigurationTest::redisTemplate)
                .withBean(RechargeRedisKeyBuilder.class, () -> new RechargeRedisKeyBuilder("cardshop:trade", 128))
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withUserConfiguration(BalanceMapperBeans.class)
                .withConfiguration(AutoConfigurations.of(TradeEngineRedisPendingAutoConfiguration.class))
                .run(context -> {
                    /*
                     * 参见高可用设计「17.4 缓存修复任务」和「17.5 余额对账任务」。BAL-002/CACHE-001
                     * 同时依赖 RedisTemplate 与 MyBatis Mapper；若 Redis 后置配置早于 Mapper 条件判断执行，
                     * job facade 会长期退回 noop。本测试锁定 Redis+Mapper 条件齐备时必须创建真实 D12 端口。
                     */
                    assertThat(context).hasSingleBean(RechargeBalanceReconcilePort.class);
                    assertThat(context.getBean(RechargeBalanceReconcilePort.class))
                            .isInstanceOf(MybatisRechargeBalanceReconcileAdapter.class);
                    assertThat(context).hasSingleBean(RechargeCacheRepairPort.class);
                    assertThat(context.getBean(RechargeCacheRepairPort.class))
                            .isInstanceOf(RedisRechargeCacheRepairAdapter.class);
                });
    }

    @Test
    void registersRechargeOrderQueryPortWithMysqlFallbackWhenMappersAreAvailable() {
        new ApplicationContextRunner()
                .withBean(ReactiveStringRedisTemplate.class, TradeEngineRedisPendingAutoConfigurationTest::redisTemplate)
                .withBean(RechargeRedisKeyBuilder.class, () -> new RechargeRedisKeyBuilder("cardshop:trade", 128))
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withUserConfiguration(QueryMapperBeans.class)
                .withConfiguration(AutoConfigurations.of(TradeEngineRedisPendingAutoConfiguration.class))
                .run(context -> assertThat(context).hasSingleBean(RechargeOrderQueryPort.class));
    }

    private static <T> T proxy(Class<T> type) {
        Object proxy = Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[]{type},
                (ignored, method, args) -> {
                    throw new AssertionError("configuration test should not invoke " + method.getName());
                });
        return type.cast(proxy);
    }

    private static ReactiveStringRedisTemplate redisTemplate() {
        return new ReactiveStringRedisTemplate(
                proxy(ReactiveRedisConnectionFactory.class),
                RedisSerializationContext.string());
    }

    static class QueryMapperBeans {

        @Bean
        McOrderMapper mcOrderMapper() {
            return proxy(McOrderMapper.class);
        }

        @Bean
        RechargeOrderExtMapper rechargeOrderExtMapper() {
            return proxy(RechargeOrderExtMapper.class);
        }
    }

    static class BalanceMapperBeans {

        @Bean
        McExtDepartMapper mcExtDepartMapper() {
            return proxy(McExtDepartMapper.class);
        }

        @Bean
        RechargeOutboxMapper rechargeOutboxMapper() {
            return proxy(RechargeOutboxMapper.class);
        }
    }
}
