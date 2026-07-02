package com.cardshop.trade.engine.infrastructure.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 基础设施层自动配置入口。
 *
 * <p>本配置类只启用 {@link TradeEngineProperties} 的绑定，对应
 * {@code docs/交行直充业务系统详细设计.md} 的「6.4 Infrastructure 层」和「7.4 核心配置项」。
 * Redis、RabbitMQ、MySQL 等外部依赖的真实客户端会在后续适配器中按模块职责独立声明，避免 D1 骨架阶段
 * 因缺少外部环境而影响应用启动。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TradeEngineProperties.class)
public class TradeEngineInfrastructureConfiguration {
}
