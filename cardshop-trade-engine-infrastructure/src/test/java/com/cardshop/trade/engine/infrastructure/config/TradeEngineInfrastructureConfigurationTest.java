package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptLoader;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptExecutor;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisSessionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
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
}
