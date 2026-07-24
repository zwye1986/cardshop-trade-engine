package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshotPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.PersistedOrderStatePort;
import com.cardshop.trade.engine.application.recharge.RechargeDispatchTimerPort;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderBatchPersistencePort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Redis/MQ 到 MySQL 异步落库 Consumer 核心服务的后置自动配置。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。真实 dev/test/prod
 * 中 {@link RechargeOrderPersistencePort} 可能由 MyBatis 后置自动配置创建，晚于普通 bootstrap configuration；
 * 如果只在普通配置类中按该端口做 {@code @ConditionalOnBean}，Consumer 核心服务会被提前跳过，进而使 Rabbit
 * listener 无法创建。该自动配置在依赖端口就绪后再兜底装配 Consumer，避免 Redis 已接单但 MQ/MySQL 链路没有
 * consumer 的半闭环状态。
 */
@AutoConfiguration(afterName = "com.cardshop.trade.engine.infrastructure.config.TradeEngineMybatisPersistenceAutoConfiguration")
public class TradeEngineConsumerAutoConfiguration {

    /**
     * 创建异步落库 Consumer 核心服务。
     *
     * <p>{@link PersistOrderSnapshotPort} 允许降级为 noop，是因为 MQ 消息本身携带 `initial_order_snapshot` 兜底快照；
     * 但 {@link RechargeOrderPersistencePort} 和 {@link RechargeAcceptReliabilityPort} 必须同时存在，否则不能消费
     * RabbitMQ 落库消息，避免 ack 掉无法真正持久化的订单。
     */
    @Bean
    @ConditionalOnBean({RechargeOrderPersistencePort.class, RechargeAcceptReliabilityPort.class})
    @ConditionalOnMissingBean(PersistRechargeOrderConsumerService.class)
    public PersistRechargeOrderConsumerService persistRechargeOrderConsumerService(
            ObjectProvider<PersistOrderSnapshotPort> snapshotPortProvider,
            ObjectProvider<RechargeOrderBatchPersistencePort> batchPersistencePortProvider,
            ObjectProvider<PersistedOrderStatePort> persistedOrderStatePortProvider,
            ObjectProvider<RechargeDispatchTimerPort> dispatchTimerPortProvider,
            RechargeOrderPersistencePort persistencePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            Clock clock,
            TradeEngineProperties properties) {
        return new PersistRechargeOrderConsumerService(
                snapshotPortProvider.getIfAvailable(PersistOrderSnapshotPort::noop),
                persistencePort,
                batchPersistencePortProvider.getIfAvailable(),
                persistedOrderStatePortProvider.getIfAvailable(PersistedOrderStatePort::noop),
                reliabilityPort,
                dispatchTimerPortProvider.getIfAvailable(RechargeDispatchTimerPort::noop),
                clock,
                Duration.ofSeconds(3),
                properties.getFulfillment().dispatchInitialDelay());
    }
}
