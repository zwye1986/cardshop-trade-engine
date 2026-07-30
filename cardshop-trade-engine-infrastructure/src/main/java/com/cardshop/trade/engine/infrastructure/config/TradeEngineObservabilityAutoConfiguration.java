package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.RechargeDispatchTimerPort;
import com.cardshop.trade.engine.infrastructure.observability.RedisMysqlTradeEngineObservabilitySnapshotProvider;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilitySnapshotProvider;
import com.cardshop.trade.engine.application.v1.V1CreateOrderStageObserver;
import com.cardshop.trade.engine.application.v1.V1MerchantProfileCacheObserver;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeBalanceDeltaOutboxMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import com.cardshop.trade.engine.infrastructure.timer.ScheduledExecutorRechargeDispatchTimer;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 交易引擎业务监控自动配置。
 *
 * <p>参见 {@code docs/开发进度.md}「5.1 监控告警最小闭环执行边界」。本配置只在 Micrometer
 * 可用时注册业务指标；Redis/MySQL 快照 Provider 依赖真实 RedisTemplate 与 Mapper，条件不满足时退回空快照，
 * 确保 local/slice 测试不会因为外部中间件缺失而启动失败。
 */
@AutoConfiguration(after = {
        TradeEngineInfrastructureConfiguration.class,
        TradeEngineMybatisPersistenceAutoConfiguration.class,
        TradeEngineRedisPendingAutoConfiguration.class,
        TradeEngineFulfillmentAutoConfiguration.class
})
@ConditionalOnClass(MeterRegistry.class)
@EnableConfigurationProperties(TradeEngineProperties.class)
public class TradeEngineObservabilityAutoConfiguration {

    @Bean
    @ConditionalOnBean({
            ReactiveStringRedisTemplate.class,
            RechargeRedisKeyBuilder.class,
            RechargeDispatchOutboxMapper.class,
            RechargeBalanceDeltaOutboxMapper.class,
            RechargeAcceptAnomalyMapper.class,
            McExtDepartMapper.class
    })
    @ConditionalOnMissingBean(TradeEngineObservabilitySnapshotProvider.class)
    public TradeEngineObservabilitySnapshotProvider tradeEngineObservabilitySnapshotProvider(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties,
            RechargeDispatchOutboxMapper dispatchOutboxMapper,
            RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper,
            RechargeAcceptAnomalyMapper acceptAnomalyMapper,
            McExtDepartMapper extDepartMapper,
            Clock clock) {
        return new RedisMysqlTradeEngineObservabilitySnapshotProvider(
                redisTemplate,
                keyBuilder,
                properties,
                dispatchOutboxMapper,
                balanceDeltaOutboxMapper,
                acceptAnomalyMapper,
                extDepartMapper,
                clock);
    }

    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean(TradeEngineObservabilityMetrics.class)
    public TradeEngineObservabilityMetrics tradeEngineObservabilityMetrics(
            MeterRegistry meterRegistry,
            TradeEngineProperties properties,
            ObjectProvider<TradeEngineObservabilitySnapshotProvider> snapshotProviderProvider) {
        TradeEngineObservabilityMetrics metrics = new TradeEngineObservabilityMetrics(
                snapshotProviderProvider.getIfAvailable(TradeEngineObservabilitySnapshotProvider::empty),
                properties.getEnvironment(),
                Duration.ofMillis(Math.max(1, properties.getRedis().getLuaWarnThresholdMs())));
        metrics.bindTo(meterRegistry);
        return metrics;
    }

    /**
     * 注册进程内供应商履约 timer 指标。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。timer 是热路径唤醒器，
     * 不承载持久业务事实；这里仅暴露低基数本地队列指标，用于压测时判断 timer 是否因为 pending 上限或拒绝
     * 调度成为新瓶颈。订单号、eventId 和供应商请求号均不得进入指标标签。
     */
    @Bean
    @ConditionalOnBean({MeterRegistry.class, RechargeDispatchTimerPort.class})
    @ConditionalOnMissingBean(RechargeDispatchTimerMetricsRegistration.class)
    public RechargeDispatchTimerMetricsRegistration rechargeDispatchTimerMetricsRegistration(
            MeterRegistry meterRegistry,
            TradeEngineProperties properties,
            RechargeDispatchTimerPort dispatchTimerPort) {
        if (!(dispatchTimerPort instanceof ScheduledExecutorRechargeDispatchTimer timer)) {
            return RechargeDispatchTimerMetricsRegistration.noop();
        }
        String environment = metricEnvironment(properties);
        Gauge.builder(
                        "trade.engine.dispatch.timer.pending",
                        timer,
                        ScheduledExecutorRechargeDispatchTimer::pendingCount)
                .description("进程内供应商履约 timer 当前待触发事件数")
                .tag("environment", environment)
                .register(meterRegistry);
        Gauge.builder(
                        "trade.engine.dispatch.timer.max.pending",
                        timer,
                        ScheduledExecutorRechargeDispatchTimer::maxPendingTasks)
                .description("进程内供应商履约 timer 待触发事件上限")
                .tag("environment", environment)
                .register(meterRegistry);
        FunctionCounter.builder(
                        "trade.engine.dispatch.timer.rejected.total",
                        timer,
                        ScheduledExecutorRechargeDispatchTimer::rejectedScheduleCount)
                .description("进程内供应商履约 timer 拒绝新增热唤醒的累计次数")
                .tag("environment", environment)
                .register(meterRegistry);
        FunctionCounter.builder(
                        "trade.engine.dispatch.timer.worker.rejected.total",
                        timer,
                        ScheduledExecutorRechargeDispatchTimer::rejectedWorkerExecutionCount)
                .description("进程内供应商履约 worker executor 拒绝执行的累计次数")
                .tag("environment", environment)
                .register(meterRegistry);
        return RechargeDispatchTimerMetricsRegistration.ofRegistered();
    }

    /**
     * 将业务指标 Bean 显式暴露为商户资料缓存观测端口，供
     * {@code V1MerchantProfilePort} 装配时注入 hit/miss 计数。
     */
    @Bean
    @ConditionalOnBean(TradeEngineObservabilityMetrics.class)
    @ConditionalOnMissingBean(V1MerchantProfileCacheObserver.class)
    public V1MerchantProfileCacheObserver v1MerchantProfileCacheObserver(
            TradeEngineObservabilityMetrics metrics) {
        return metrics;
    }

    /**
     * 将业务指标 Bean 显式暴露为下单同步子阶段观测端口。
     */
    @Bean
    @ConditionalOnBean(TradeEngineObservabilityMetrics.class)
    @ConditionalOnMissingBean(V1CreateOrderStageObserver.class)
    public V1CreateOrderStageObserver v1CreateOrderStageObserver(
            TradeEngineObservabilityMetrics metrics) {
        return metrics;
    }

    private static String metricEnvironment(TradeEngineProperties properties) {
        String environment = properties == null ? null : properties.getEnvironment();
        return environment == null || environment.isBlank() ? "unknown" : environment;
    }

    /**
     * timer 指标注册哨兵 Bean，避免同一上下文重复注册 Micrometer meter。
     */
    public record RechargeDispatchTimerMetricsRegistration(boolean registered) {

        private static RechargeDispatchTimerMetricsRegistration ofRegistered() {
            return new RechargeDispatchTimerMetricsRegistration(true);
        }

        private static RechargeDispatchTimerMetricsRegistration noop() {
            return new RechargeDispatchTimerMetricsRegistration(false);
        }
    }
}
