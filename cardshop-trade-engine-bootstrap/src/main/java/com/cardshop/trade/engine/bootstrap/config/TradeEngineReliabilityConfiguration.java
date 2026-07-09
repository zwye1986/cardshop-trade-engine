package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyAutoCheckService;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.PendingPersistOrderPort;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshotPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushPort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushService;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcilePort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcileService;
import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairPort;
import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairService;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.recharge.RechargePersistMqRepublishService;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessageListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.cardshop.trade.engine.job.recharge.AcceptAnomalyAutoCheckJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceDeltaFlushJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceReconcileJob;
import com.cardshop.trade.engine.job.recharge.RechargeCacheRepairJob;
import com.cardshop.trade.engine.job.recharge.RechargeOutboxPublishJob;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import java.time.Clock;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 交易可靠性与补偿类 application service 装配。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.3 Redis HA Guard」、
 * 「17. 分片补偿与对账任务」和「20.13.6 Redis Lua 下单结果」。bootstrap 层负责把 application 服务和
 * infrastructure 端口组装起来；具体 XXL-Job handler 后续只需要调用这些已装配好的服务。
 */
@Configuration(proxyBeanMethods = false)
public class TradeEngineReliabilityConfiguration {

    /**
     * 接单异常自动核查服务。
     *
     * <p>当前 D5B 服务覆盖 Redis/MySQL 读侧可判定的最小闭环；RabbitMQ confirm 轨迹和 Consumer 结果会在
     * D7/D8 接入后继续扩展到同一个服务，不改变 job 调用入口。
     */
    @Bean
    @ConditionalOnBean({AcceptAnomalyCheckRepositoryPort.class, RechargeOrderQueryPort.class})
    @ConditionalOnMissingBean(AcceptAnomalyAutoCheckService.class)
    public AcceptAnomalyAutoCheckService acceptAnomalyAutoCheckService(
            AcceptAnomalyCheckRepositoryPort repository,
            RechargeOrderQueryPort orderQueryPort) {
        return new AcceptAnomalyAutoCheckService(repository, orderQueryPort);
    }

    /**
     * 接单异常自动核查任务入口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「17. 分片补偿与对账任务」。D5B 先装配无框架依赖的
     * job facade，后续引入 XXL-Job handler 时只负责解析调度参数并调用该 Bean，避免重复实现核查批处理。
     */
    @Bean
    @ConditionalOnBean(AcceptAnomalyAutoCheckService.class)
    @ConditionalOnMissingBean(AcceptAnomalyAutoCheckJob.class)
    public AcceptAnomalyAutoCheckJob acceptAnomalyAutoCheckJob(
            AcceptAnomalyAutoCheckService autoCheckService) {
        return new AcceptAnomalyAutoCheckJob(autoCheckService);
    }

    @Bean
    @ConditionalOnBean(PersistRechargeOrderMessagePublisher.class)
    @ConditionalOnMissingBean(RechargePersistMqRepublishService.class)
    public RechargePersistMqRepublishService rechargePersistMqRepublishService(
            ObjectProvider<PendingPersistOrderPort> pendingPortProvider,
            PersistRechargeOrderMessagePublisher publisher) {
        /*
         * 参见高可用设计「17.3 MQ 补投任务」。不要把 PendingPersistOrderPort.noop() 注册成 Bean，
         * 否则普通配置类解析顺序可能让 Noop 先占位，挡住 infrastructure 在 Redis/MySQL 就绪后提供的真实端口。
         */
        return new RechargePersistMqRepublishService(
                pendingPortProvider.getIfAvailable(PendingPersistOrderPort::noop),
                publisher);
    }

    @Bean
    @ConditionalOnBean(RechargePersistMqRepublishService.class)
    @ConditionalOnMissingBean(RechargePersistMqRepublishJob.class)
    public RechargePersistMqRepublishJob rechargePersistMqRepublishJob(
            RechargePersistMqRepublishService service) {
        return new RechargePersistMqRepublishJob(service);
    }

    /**
     * Outbox 重试/补偿 Job 门面。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.11 XXL-Job Handler 契约」和
     * 「20.13.9 派单 Outbox」。D9 的实时 worker 与 D11/D12 的调度补偿共用同一个
     * {@link com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService}，确保 outbox CAS
     * 抢占、供应商派单、作废和回调任务生成只有一套业务语义。
     */
    @Bean
    @ConditionalOnBean(com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService.class)
    @ConditionalOnMissingBean(RechargeOutboxPublishJob.class)
    public RechargeOutboxPublishJob rechargeOutboxPublishJob(
            com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService service) {
        return new RechargeOutboxPublishJob(service);
    }

    @Bean
    @ConditionalOnMissingBean(RechargeBalanceDeltaFlushService.class)
    public RechargeBalanceDeltaFlushService rechargeBalanceDeltaFlushService(
            ObjectProvider<RechargeBalanceDeltaFlushPort> portProvider) {
        return new RechargeBalanceDeltaFlushService(
                portProvider.getIfAvailable(RechargeBalanceDeltaFlushPort::noop));
    }

    @Bean
    @ConditionalOnBean(RechargeBalanceDeltaFlushService.class)
    @ConditionalOnMissingBean(RechargeBalanceDeltaFlushJob.class)
    public RechargeBalanceDeltaFlushJob rechargeBalanceDeltaFlushJob(
            RechargeBalanceDeltaFlushService service) {
        return new RechargeBalanceDeltaFlushJob(service);
    }

    @Bean
    @ConditionalOnMissingBean(RechargeBalanceReconcileService.class)
    public RechargeBalanceReconcileService rechargeBalanceReconcileService(
            ObjectProvider<RechargeBalanceReconcilePort> portProvider) {
        return new RechargeBalanceReconcileService(
                portProvider.getIfAvailable(RechargeBalanceReconcilePort::noop));
    }

    @Bean
    @ConditionalOnBean(RechargeBalanceReconcileService.class)
    @ConditionalOnMissingBean(RechargeBalanceReconcileJob.class)
    public RechargeBalanceReconcileJob rechargeBalanceReconcileJob(
            RechargeBalanceReconcileService service) {
        return new RechargeBalanceReconcileJob(service);
    }

    @Bean
    @ConditionalOnMissingBean(RechargeCacheRepairService.class)
    public RechargeCacheRepairService rechargeCacheRepairService(
            ObjectProvider<RechargeCacheRepairPort> portProvider) {
        return new RechargeCacheRepairService(
                portProvider.getIfAvailable(RechargeCacheRepairPort::noop));
    }

    @Bean
    @ConditionalOnBean(RechargeCacheRepairService.class)
    @ConditionalOnMissingBean(RechargeCacheRepairJob.class)
    public RechargeCacheRepairJob rechargeCacheRepairJob(
            RechargeCacheRepairService service) {
        return new RechargeCacheRepairJob(service);
    }

    /**
     * Redis/MQ 到 MySQL 的异步落库 Consumer 核心服务。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。D7A 先装配
     * 不依赖 RabbitMQ listener 的核心服务，后续真实 `@RabbitListener` 只负责反序列化消息和按结果 ack/nack。
     * 该 Bean 覆盖显式提供 MySQL 落库端口的 slice test 或局部上下文；真实 dev/test/prod 中
     * {@link RechargeOrderPersistencePort} 可能由 MyBatis 后置自动配置创建，因此最终兜底由
     * {@link TradeEngineConsumerAutoConfiguration} 在依赖端口就绪后完成。
     */
    @Bean
    @ConditionalOnBean({RechargeOrderPersistencePort.class, RechargeAcceptReliabilityPort.class})
    @ConditionalOnMissingBean(PersistRechargeOrderConsumerService.class)
    public PersistRechargeOrderConsumerService persistRechargeOrderConsumerService(
            org.springframework.beans.factory.ObjectProvider<PersistOrderSnapshotPort> snapshotPortProvider,
            RechargeOrderPersistencePort persistencePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            Clock clock) {
        return new PersistRechargeOrderConsumerService(
                snapshotPortProvider.getIfAvailable(PersistOrderSnapshotPort::noop),
                persistencePort,
                reliabilityPort,
                clock);
    }

    /**
     * RabbitMQ 异步落库 listener 的 bootstrap 侧轻量装配。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.9 RabbitMQ 声明契约」和
     * 「20.13.8 异步落库 Consumer」。基础设施配置类中也有 listener 装配方法，但它依赖
     * {@link PersistRechargeOrderConsumerService}；当 infrastructure 配置先于本配置类解析时，
     * {@code @ConditionalOnBean(PersistRechargeOrderConsumerService.class)} 会在 Consumer Bean 注册前被判定为不满足，
     * 结果就是应用启动健康但 RabbitMQ 没有 consumer，也不会在管理端看到 AMQP 连接。
     *
     * <p>这个 Bean 只覆盖 slice test 或特殊上下文里显式提供 {@link RabbitTemplate} 的场景。真实 dev/test/prod 中
     * RabbitTemplate 来自 Spring Boot 的 Rabbit 自动配置，普通 {@code @Configuration} 仍可能早于 RabbitTemplate
     * BeanDefinition 被解析；因此最终兜底在 {@link TradeEngineRabbitListenerAutoConfiguration} 中完成。若基础设施层或
     * 后置自动配置已成功创建 listener，{@link ConditionalOnMissingBean} 会自动退让，保证全局只有一个消费入口。
     */
    @Bean
    @ConditionalOnBean({RabbitTemplate.class, PersistRechargeOrderConsumerService.class})
    @ConditionalOnMissingBean(RabbitPersistRechargeOrderMessageListener.class)
    public RabbitPersistRechargeOrderMessageListener rabbitPersistRechargeOrderMessageListener(
            ObjectMapper objectMapper,
            PersistRechargeOrderConsumerService consumerService,
            RabbitTemplate rabbitTemplate,
            TradeEngineProperties properties) {
        return new RabbitPersistRechargeOrderMessageListener(
                objectMapper,
                consumerService,
                rabbitTemplate,
                properties.getMq().getPersistRetryExchange(),
                properties.getMq().getPersistRetryRoutingKey(),
                properties.getMq().getConsumerMaxRetry());
    }
}
