package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.infrastructure.persistence.FallbackRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.redis.InMemoryRedisHaGuard;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptLoader;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptExecutor;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisSessionFactory;
import com.cardshop.trade.engine.infrastructure.redis.RedisRechargeOrderQueryAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
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
                .withBean(ReactiveStringRedisTemplate.class, () -> null)
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargeOrderQueryPort.class);
                    assertThat(context.getBean(RechargeOrderQueryPort.class))
                            .isInstanceOf(RedisRechargeOrderQueryAdapter.class);
                });
    }

    @Test
    void wrapsRedisOrderQueryPortWithMysqlFallbackWhenMappersExist() {
        contextRunner
                .withBean(ReactiveStringRedisTemplate.class, () -> null)
                .withBean(McOrderMapper.class, () -> mapperProxy(McOrderMapper.class))
                .withBean(RechargeOrderExtMapper.class, () -> mapperProxy(RechargeOrderExtMapper.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargeOrderQueryPort.class);
                    assertThat(context.getBean(RechargeOrderQueryPort.class))
                            .isInstanceOf(FallbackRechargeOrderQueryAdapter.class);
                });
    }

    @Test
    void createsAcceptAnomalyCheckRepositoryWhenMapperExists() {
        contextRunner
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(RechargeAcceptAnomalyMapper.class, () -> mapperProxy(RechargeAcceptAnomalyMapper.class))
                .run(context -> assertThat(context).hasSingleBean(AcceptAnomalyCheckRepositoryPort.class));
    }

    private static <T> T mapperProxy(Class<T> mapperType) {
        Object proxy = Proxy.newProxyInstance(
                mapperType.getClassLoader(),
                new Class<?>[]{mapperType},
                (ignored, method, args) -> {
                    if (method.getReturnType().isPrimitive()) {
                        return 0;
                    }
                    return null;
                });
        return mapperType.cast(proxy);
    }
}
