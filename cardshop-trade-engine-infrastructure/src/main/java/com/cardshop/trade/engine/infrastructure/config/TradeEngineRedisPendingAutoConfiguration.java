package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.PendingPersistOrderPort;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshotPort;
import com.cardshop.trade.engine.application.recharge.PersistedOrderStatePort;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import com.cardshop.trade.engine.infrastructure.redis.RedisPendingPersistOrderAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisPersistOrderSnapshotAdapter;
import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * Redis pending 补投相关端口的后置自动配置。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.11 XXL-Job Handler 契约」和
 * 「20.13.8 异步落库 Consumer」。真实 dev/test 启动中，{@link ReactiveStringRedisTemplate} 来自
 * Spring Boot Redis reactive auto-configuration，可能晚于普通 {@link TradeEngineInfrastructureConfiguration}
 * 的条件判断出现。若只在普通配置类中按 RedisTemplate 做 {@code @ConditionalOnBean}，应用会有 Redis
 * 客户端却缺少 {@link RedisPendingPersistOrderAdapter}，人工补投入口表现为一直 {@code scanned=0}。
 *
 * <p>该自动配置在 Redis reactive auto-configuration 后兜底创建快照读取端口和 pending 扫描端口。两个端口都只在
 * Redis 客户端真实存在时注册；没有 Redis 的 local/slice test 仍由 bootstrap 层构造参数级 noop 兜底，避免把
 * noop 注册成全局 Bean 后抢占真实端口。
 */
@AutoConfiguration(after = RedisReactiveAutoConfiguration.class)
public class TradeEngineRedisPendingAutoConfiguration {

    /**
     * 创建 Consumer 落库前读取 Redis 完整订单快照的端口。
     *
     * <p>MQ 消息虽然携带 initial snapshot 兜底，但真实 Redis 仍是最新订单版本来源。后置创建该 Bean 可以覆盖
     * RedisTemplate 晚到的启动顺序，避免 Consumer 总是退回 {@code MQ_SNAPSHOT_FALLBACK}。
     */
    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    @ConditionalOnMissingBean(PersistOrderSnapshotPort.class)
    public PersistOrderSnapshotPort persistOrderSnapshotPort(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties) {
        int timeoutMs = Math.max(100, properties.getRedis().getWaitTimeoutMs() * 5);
        return new RedisPersistOrderSnapshotAdapter(
                redisTemplate,
                keyBuilder,
                Duration.ofMillis(timeoutMs));
    }

    /**
     * 创建 D11/D12 Redis pending MQ 补投端口。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.3 MQ 补投任务」。MySQL 二次确认端口允许
     * 构造参数级 noop，是为了在局部测试没有 Mapper 时仍能实例化；真实 dev/test 中
     * {@link PersistedOrderStatePort} 会由 MyBatis 后置自动配置提供，从而避免已落库 pending 被重复补投。
     */
    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    @ConditionalOnMissingBean(PendingPersistOrderPort.class)
    public PendingPersistOrderPort pendingPersistOrderPort(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties,
            ObjectProvider<PersistedOrderStatePort> persistedOrderStatePortProvider,
            Clock clock) {
        return new RedisPendingPersistOrderAdapter(
                redisTemplate,
                keyBuilder,
                properties,
                persistedOrderStatePortProvider.getIfAvailable(PersistedOrderStatePort::noop),
                clock);
    }
}
