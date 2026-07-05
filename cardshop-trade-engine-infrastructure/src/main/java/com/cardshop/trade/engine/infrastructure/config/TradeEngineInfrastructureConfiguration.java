package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.infrastructure.redis.RedisAcceptRechargeOrderAdapter;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptLoader;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisScriptExecutor;
import com.cardshop.trade.engine.infrastructure.redis.SpringDataRechargeRedisScriptExecutor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 基础设施层自动配置入口。
 *
 * <p>本配置类只启用 {@link TradeEngineProperties} 的绑定，对应
 * {@code docs/交行直充业务系统详细设计.md} 的「6.4 Infrastructure 层」和「7.4 核心配置项」。
 * D4 阶段开始声明 Redis 接单所需的 Key 构造、Lua 脚本和适配器；真实 Redis 客户端仍由 Spring Boot
 * 自动配置和 Nacos 环境提供，local profile 禁用 Redis 时不会强制创建接单端口。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TradeEngineProperties.class)
public class TradeEngineInfrastructureConfiguration {

    @Bean
    public RechargeRedisKeyBuilder rechargeRedisKeyBuilder(TradeEngineProperties properties) {
        return new RechargeRedisKeyBuilder(
                properties.getRedis().getKeyPrefix(),
                properties.getRedis().getPendingOrgBucketCount());
    }

    @Bean
    public RechargeRedisScriptLoader rechargeRedisScriptLoader() {
        return new RechargeRedisScriptLoader();
    }

    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    public RechargeRedisScriptExecutor rechargeRedisScriptExecutor(ReactiveStringRedisTemplate redisTemplate) {
        return new SpringDataRechargeRedisScriptExecutor(redisTemplate);
    }

    @Bean
    @ConditionalOnBean(RechargeRedisScriptExecutor.class)
    public AcceptRechargeOrderPort acceptRechargeOrderPort(
            RechargeRedisScriptExecutor scriptExecutor,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            TradeEngineProperties properties) {
        return new RedisAcceptRechargeOrderAdapter(scriptExecutor, keyBuilder, scriptLoader, properties);
    }
}
