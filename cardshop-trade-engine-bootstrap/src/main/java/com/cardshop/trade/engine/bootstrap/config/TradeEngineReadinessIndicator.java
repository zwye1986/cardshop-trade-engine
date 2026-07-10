package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 交易引擎 readiness 探针。
 *
 * <p>探针字段对应 {@code docs/交行直充业务系统详细设计.md} 的「11. 配置中心、部署与健康检查」和
 * {@code docs/交行直充业务系统详细设计.md} 的「11. 配置中心、部署与健康检查」。当前 D1 阶段只暴露配置态，
 * 后续接入 Redis、RabbitMQ、MySQL 后，再按设计文档扩展外部依赖检查。
 */
@Component("tradeEngineReadiness")
public class TradeEngineReadinessIndicator implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(TradeEngineReadinessIndicator.class);

    private final TradeEngineProperties properties;
    private final AtomicBoolean loggedLegacyBaseUrlMissing = new AtomicBoolean();
    private final AtomicBoolean loggedProdRedisWriteEnabled = new AtomicBoolean();

    public TradeEngineReadinessIndicator(TradeEngineProperties properties) {
        this.properties = properties;
    }

    /**
     * 返回当前服务可接流状态。
     *
     * <p>这里不暴露任何密钥、连接串或旧系统真实地址，只暴露排查所需的开关状态。
     */
    @Override
    public Health health() {
        var snapshot = ReadinessSnapshot.from(properties);
        logReadinessRisks(snapshot);

        // TODO(D8): 当 externalCheckEnabled=true 时接入 Redis、RabbitMQ、MySQL 真实连通性检查；参见 docs/交行直充业务系统详细设计.md「11. 配置中心、部署与健康检查」。
        return Health.up()
                .withDetail("environment", snapshot.environment())
                .withDetail("redisWriteEnabled", snapshot.redisWriteEnabled())
                .withDetail("redisFailClosed", snapshot.redisFailClosed())
                .withDetail("mqBreakerEnabled", snapshot.mqBreakerEnabled())
                .withDetail("routingEnabled", snapshot.routingEnabled())
                .withDetail("legacyBaseUrlConfigured", snapshot.legacyBaseUrlConfigured())
                .withDetail("externalCheckEnabled", snapshot.externalCheckEnabled())
                .build();
    }

    private void logReadinessRisks(ReadinessSnapshot snapshot) {
        log.debug(
                "交易引擎 readiness 配置快照 environment={}, redisWriteEnabled={}, redisFailClosed={}, mqBreakerEnabled={}, routingEnabled={}, legacyBaseUrlConfigured={}, externalCheckEnabled={}",
                snapshot.environment(),
                snapshot.redisWriteEnabled(),
                snapshot.redisFailClosed(),
                snapshot.mqBreakerEnabled(),
                snapshot.routingEnabled(),
                snapshot.legacyBaseUrlConfigured(),
                snapshot.externalCheckEnabled());

        if (snapshot.routingEnabled()
                && !snapshot.legacyBaseUrlConfigured()
                && loggedLegacyBaseUrlMissing.compareAndSet(false, true)) {
            log.warn("交易引擎路由已开启但旧系统地址未配置，灰度切流或代理回退可能不可用 environment={}", snapshot.environment());
        }

        if ("prod".equalsIgnoreCase(snapshot.environment())
                && snapshot.redisWriteEnabled()
                && loggedProdRedisWriteEnabled.compareAndSet(false, true)) {
            log.warn("生产环境 Redis 写入开关已开启，请确认 Redis、MQ、MySQL、Liquibase 和灰度路由均已通过上线前验证");
        }
    }

    /**
     * readiness 输出快照。
     *
     * <p>使用 Java record 明确探针只携带可观测状态，不携带敏感配置值。
     */
    private record ReadinessSnapshot(
            String environment,
            boolean redisWriteEnabled,
            boolean redisFailClosed,
            boolean mqBreakerEnabled,
            boolean routingEnabled,
            boolean legacyBaseUrlConfigured,
            boolean externalCheckEnabled) {

        private static ReadinessSnapshot from(TradeEngineProperties properties) {
            return new ReadinessSnapshot(
                    properties.getEnvironment(),
                    properties.getRedis().isWriteEnabled(),
                    properties.getRedis().isUnavailableFailClosed(),
                    properties.getMq().getBreaker().isEnabled(),
                    properties.getRouting().isEnabled(),
                    StringUtils.hasText(properties.getRouting().getLegacyBaseUrl()),
                    properties.getHealth().isExternalCheckEnabled());
        }
    }
}
