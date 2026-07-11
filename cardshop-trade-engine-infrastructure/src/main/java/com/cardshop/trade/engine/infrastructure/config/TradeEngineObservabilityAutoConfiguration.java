package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.infrastructure.observability.RedisMysqlTradeEngineObservabilitySnapshotProvider;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilitySnapshotProvider;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
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
        TradeEngineRedisPendingAutoConfiguration.class
})
@ConditionalOnClass(MeterRegistry.class)
@EnableConfigurationProperties(TradeEngineProperties.class)
public class TradeEngineObservabilityAutoConfiguration {

    @Bean
    @ConditionalOnBean({
            ReactiveStringRedisTemplate.class,
            RechargeRedisKeyBuilder.class,
            RechargeOutboxMapper.class,
            RechargeAcceptAnomalyMapper.class,
            McExtDepartMapper.class
    })
    @ConditionalOnMissingBean(TradeEngineObservabilitySnapshotProvider.class)
    public TradeEngineObservabilitySnapshotProvider tradeEngineObservabilitySnapshotProvider(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties,
            RechargeOutboxMapper outboxMapper,
            RechargeAcceptAnomalyMapper acceptAnomalyMapper,
            McExtDepartMapper extDepartMapper,
            Clock clock) {
        return new RedisMysqlTradeEngineObservabilitySnapshotProvider(
                redisTemplate,
                keyBuilder,
                properties,
                outboxMapper,
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
}
