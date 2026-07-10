package com.cardshop.trade.engine.infrastructure.config;

import com.baomidou.mybatisplus.autoconfigure.MybatisPlusAutoConfiguration;
import com.cardshop.trade.engine.application.recharge.DirectRechargeFrequencyPort;
import com.cardshop.trade.engine.application.recharge.PendingPersistOrderPort;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshotPort;
import com.cardshop.trade.engine.application.recharge.PersistedOrderStatePort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcilePort;
import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.infrastructure.persistence.FallbackRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeBalanceReconcileAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderQueryAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import com.cardshop.trade.engine.infrastructure.redis.RedisDirectRechargeFrequencyAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisRechargeCacheRepairAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RedisRechargeOrderQueryAdapter;
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
 * <p>该自动配置在 Redis reactive auto-configuration 后兜底创建快照读取端口和 pending 扫描端口。D12
 * 余额对账/缓存修复同时依赖 Redis 与 MyBatis Mapper，因此还必须排在 MyBatis-Plus 自动配置之后，避免
 * test188 dev 出现 RedisTemplate 已有、Mapper 条件过早失败、`balance-reconcile/cache-repair` 永远走 noop。
 * 没有 Redis 的 local/slice test 仍由 bootstrap 层构造参数级 noop 兜底，避免把 noop 注册成全局 Bean 后抢占真实端口。
 */
@AutoConfiguration(after = {RedisReactiveAutoConfiguration.class, MybatisPlusAutoConfiguration.class})
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

    /**
     * 创建同账号直充限频端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」。派单 worker 在调用供应商前
     * 必须先抢同账号限频锁，并在明确终态或可重试失败窗口按 owner 释放/保留。该端口依赖
     * {@link ReactiveStringRedisTemplate}，需要和 pending 端口一样放到 Redis 后置自动配置中，避免普通配置类
     * 早于 RedisTemplate 条件判断而跳过，最终让 `RechargeOutboxWorkerService` 无法创建。
     */
    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    @ConditionalOnMissingBean(DirectRechargeFrequencyPort.class)
    public DirectRechargeFrequencyPort directRechargeFrequencyPort(
            ReactiveStringRedisTemplate redisTemplate,
            TradeEngineProperties properties) {
        int timeoutMs = Math.max(100, properties.getRedis().getWaitTimeoutMs() * 3);
        return new RedisDirectRechargeFrequencyAdapter(redisTemplate, Duration.ofMillis(timeoutMs));
    }

    /**
     * 创建 V1 查单/作废共用的订单快照查询端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.10 查单设计」和「20.13.11 作废设计」。
     * test188 dev 曾出现普通 {@link TradeEngineInfrastructureConfiguration} 早于 RedisTemplate 条件判断执行，
     * 导致 {@link RechargeOrderQueryPort} 没有注册，V1 查询/作废 use case 只能拿到 bootstrap fallback，
     * `/v1/cardapi/order/invalid` 即使已灰度且验签通过也返回“新交易引擎作废暂未启用”。这里放到 Redis reactive
     * 后置自动配置中兜底创建真实端口：Redis 始终作为受理期短期事实来源；MySQL Mapper 已存在时再叠加最终事实兜底。
     */
    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    @ConditionalOnMissingBean(RechargeOrderQueryPort.class)
    public RechargeOrderQueryPort rechargeOrderQueryPort(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties,
            ObjectProvider<McOrderMapper> orderMapperProvider,
            ObjectProvider<RechargeOrderExtMapper> extMapperProvider) {
        int timeoutMs = Math.max(100, properties.getRedis().getWaitTimeoutMs() * 5);
        RechargeOrderQueryPort redisQueryPort = new RedisRechargeOrderQueryAdapter(
                redisTemplate,
                keyBuilder,
                Duration.ofMillis(timeoutMs));
        McOrderMapper orderMapper = orderMapperProvider.getIfAvailable();
        RechargeOrderExtMapper extMapper = extMapperProvider.getIfAvailable();
        if (orderMapper == null || extMapper == null) {
            return redisQueryPort;
        }
        return new FallbackRechargeOrderQueryAdapter(
                redisQueryPort,
                new MybatisRechargeOrderQueryAdapter(orderMapper, extMapper));
    }

    /**
     * 创建余额对账端口。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.5 余额对账任务」。该端口同时依赖
     * MySQL 旧余额、未完成余额刷账 outbox 和 Redis 实时余额。test188 dev 曾出现 Redis 后置配置早于
     * Mapper 条件判断执行，导致真实 RedisTemplate 存在但对账端口缺失。这里不再用 Mapper 做 Bean 条件：
     * 如果 Bean 创建时 Mapper 已就绪，直接返回真实 adapter；否则返回执行期晚解析端口，真正执行时仍缺 Mapper
     * 就 fail-fast，让人工入口返回 FAILED，而不是静默 scanned=0。
     */
    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    @ConditionalOnMissingBean(RechargeBalanceReconcilePort.class)
    public RechargeBalanceReconcilePort rechargeBalanceReconcilePort(
            ObjectProvider<McExtDepartMapper> extDepartMapperProvider,
            ObjectProvider<RechargeOutboxMapper> outboxMapperProvider,
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties) {
        int timeoutMs = Math.max(100, properties.getRedis().getWaitTimeoutMs() * 5);
        McExtDepartMapper extDepartMapper = extDepartMapperProvider.getIfAvailable();
        RechargeOutboxMapper outboxMapper = outboxMapperProvider.getIfAvailable();
        if (extDepartMapper != null && outboxMapper != null) {
            return balanceReconcilePort(extDepartMapper, outboxMapper, redisTemplate, keyBuilder, timeoutMs);
        }
        return (shardTotal, shardIndex, limit, workerId) -> {
            McExtDepartMapper lateExtDepartMapper = extDepartMapperProvider.getIfAvailable();
            RechargeOutboxMapper lateOutboxMapper = outboxMapperProvider.getIfAvailable();
            if (lateExtDepartMapper == null || lateOutboxMapper == null) {
                throw new IllegalStateException(
                        "RechargeBalanceReconcilePort requires McExtDepartMapper and RechargeOutboxMapper");
            }
            return balanceReconcilePort(
                    lateExtDepartMapper,
                    lateOutboxMapper,
                    redisTemplate,
                    keyBuilder,
                    timeoutMs)
                    .reconcileDue(shardTotal, shardIndex, limit, workerId);
        };
    }

    private static RechargeCacheRepairPort cacheRepairPort(
            ObjectProvider<McExtDepartMapper> extDepartMapperProvider,
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            int timeoutMs) {
        McExtDepartMapper extDepartMapper = extDepartMapperProvider.getIfAvailable();
        if (extDepartMapper != null) {
            return new RedisRechargeCacheRepairAdapter(
                    extDepartMapper,
                    redisTemplate,
                    keyBuilder,
                    Duration.ofMinutes(5),
                    Duration.ofMillis(timeoutMs));
        }
        return (shardTotal, shardIndex, limit, workerId) -> {
            McExtDepartMapper lateExtDepartMapper = extDepartMapperProvider.getIfAvailable();
            if (lateExtDepartMapper == null) {
                throw new IllegalStateException("RechargeCacheRepairPort requires McExtDepartMapper");
            }
            return new RedisRechargeCacheRepairAdapter(
                    lateExtDepartMapper,
                    redisTemplate,
                    keyBuilder,
                    Duration.ofMinutes(5),
                    Duration.ofMillis(timeoutMs))
                    .repairDue(shardTotal, shardIndex, limit, workerId);
        };
    }

    private static RechargeBalanceReconcilePort balanceReconcilePort(
            McExtDepartMapper extDepartMapper,
            RechargeOutboxMapper outboxMapper,
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            int timeoutMs) {
        return new MybatisRechargeBalanceReconcileAdapter(
                extDepartMapper,
                outboxMapper,
                redisTemplate,
                keyBuilder,
                Duration.ofMillis(timeoutMs));
    }

    /**
     * 创建 Redis 缓存修复端口。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.4 缓存修复任务」。当前首版只修复商户余额
     * 基线，且必须持有 repairing lock 后写 Redis；商品配置修复需要完整商品源字段，保留为后续独立任务。
     * 与对账端口一样，这里避免用 Mapper 做启动期条件，防止 Redis 自动配置早到时把 CACHE-001 入口永久降级。
     */
    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    @ConditionalOnMissingBean(RechargeCacheRepairPort.class)
    public RechargeCacheRepairPort rechargeCacheRepairPort(
            ObjectProvider<McExtDepartMapper> extDepartMapperProvider,
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties) {
        int timeoutMs = Math.max(100, properties.getRedis().getWaitTimeoutMs() * 5);
        return cacheRepairPort(extDepartMapperProvider, redisTemplate, keyBuilder, timeoutMs);
    }
}
