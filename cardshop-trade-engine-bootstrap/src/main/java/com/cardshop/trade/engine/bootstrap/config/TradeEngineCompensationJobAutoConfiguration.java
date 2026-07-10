package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyAutoCheckService;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.PendingPersistOrderPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargePersistMqRepublishService;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.job.recharge.AcceptAnomalyAutoCheckJob;
import com.cardshop.trade.engine.job.recharge.RechargeOutboxPublishJob;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * 补偿任务和异常核查任务的后置自动配置。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」、
 * {@code docs/集成测试方案.md}「8.5 Redis pending 补投联调设计」。真实 dev/test/prod 启动时，
 * {@link PersistRechargeOrderMessagePublisher} 和 {@link PendingPersistOrderPort} 由基础设施层按 Redis/RabbitMQ
 * 客户端状态创建；这些 BeanDefinition 可能晚于普通 bootstrap {@code @Configuration} 的条件判断被看见。
 * 如果只依赖 {@link TradeEngineReliabilityConfiguration} 中的 {@code @ConditionalOnBean}，打开人工联调入口后，
 * {@link com.cardshop.trade.engine.bootstrap.admin.ManualRechargeJobController} 可能因缺少
 * {@link RechargePersistMqRepublishJob} 而阻断整个应用启动。
 *
 * <p>该自动配置运行在 Spring Boot auto-configuration 阶段，专门兜底装配 MQ 补投、outbox 推进和接单异常核查
 * 这些被人工入口/XXL-Job 共同调用的 service 与 job facade。它不注册 {@link PendingPersistOrderPort#noop()} 为
 * Bean，而是在构造 service 时按需降级，避免 noop 端口抢占 Redis 真实端口；同时仍通过 {@code @ConditionalOnBean}
 * 要求关键外部端口已经存在，确保没有实际执行能力时不会创建看似可用的补偿任务。
 */
@AutoConfiguration(afterName = {
        "com.cardshop.trade.engine.infrastructure.config.TradeEngineRedisPendingAutoConfiguration",
        "com.cardshop.trade.engine.infrastructure.config.TradeEngineMybatisPersistenceAutoConfiguration",
        "com.cardshop.trade.engine.infrastructure.config.TradeEngineFulfillmentAutoConfiguration"
})
public class TradeEngineCompensationJobAutoConfiguration {

    /**
     * 创建 Redis pending 到 RabbitMQ 的补投编排服务。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」和
     * {@code docs/交行直充业务系统高可用方案设计.md}「17.3 MQ 补投任务」。真实 Redis pending 端口存在时优先使用；
     * 局部测试或 Redis 未接入的上下文中只在构造参数层面退回 noop，保证应用能启动并通过执行结果暴露“本轮无待补投订单”，
     * 而不是把 noop 作为全局 Bean 注册出来影响后续真实端口装配。
     */
    @Bean
    @ConditionalOnBean(PersistRechargeOrderMessagePublisher.class)
    @ConditionalOnMissingBean(RechargePersistMqRepublishService.class)
    public RechargePersistMqRepublishService rechargePersistMqRepublishService(
            ObjectProvider<PendingPersistOrderPort> pendingPortProvider,
            PersistRechargeOrderMessagePublisher publisher) {
        return new RechargePersistMqRepublishService(
                pendingPortProvider.getIfAvailable(PendingPersistOrderPort::noop),
                publisher);
    }

    /**
     * 创建人工联调入口和后续 XXL-Job handler 共用的补投 job facade。
     *
     * <p>Controller、HTTP 参数校验和未来 XXL-Job handler 都只调用这个无框架依赖的 {@code runOnce} 门面，
     * 这样 PEND-001/PEND-002/PEND-003 的真实联调结果可以直接复用于后续调度验收，不需要再复制一套补投编排逻辑。
     */
    @Bean
    @ConditionalOnBean(RechargePersistMqRepublishService.class)
    @ConditionalOnMissingBean(RechargePersistMqRepublishJob.class)
    public RechargePersistMqRepublishJob rechargePersistMqRepublishJob(
            RechargePersistMqRepublishService service) {
        return new RechargePersistMqRepublishJob(service);
    }

    /**
     * 创建供应商派单/作废 outbox 的人工联调与调度共用 job facade。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」和
     * 「9.1 派单 Outbox」。真实 dev/test 中 {@link RechargeOutboxWorkerService} 由 infrastructure 的后置自动配置
     * 在 MyBatis/Redis/供应商 HTTP 端口都可见后创建；如果只依赖普通 bootstrap 配置类，可能因为条件判断过早而
     * 跳过 {@link RechargeOutboxPublishJob}，导致 `/internal/integration/recharge/jobs/outbox-publish/run-once`
     * 返回 “job is not available”。这里在 auto-configuration 阶段兜底创建 job facade，但仍要求真实 worker
     * 存在，避免没有履约能力时误报扫描成功。
     */
    @Bean
    @ConditionalOnBean(RechargeOutboxWorkerService.class)
    @ConditionalOnMissingBean(RechargeOutboxPublishJob.class)
    public RechargeOutboxPublishJob rechargeOutboxPublishJob(RechargeOutboxWorkerService service) {
        return new RechargeOutboxPublishJob(service);
    }

    /**
     * 创建 Redis 接单异常自动核查服务。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」和
     * 「10.1 XXL-Job Handler 契约」。2026-07-11 test188 已验证 {@code acceptAnomalyAutoCheckJob}
     * 的 handler 能注册，但普通 {@link TradeEngineReliabilityConfiguration} 可能早于 MyBatis/Redis
     * 查询端口完成条件判断，导致 handler 触发时只能报
     * {@code AcceptAnomalyAutoCheckJob is not available for XXL-Job trigger}。
     *
     * <p>这里只补齐装配兜底：真实核查决策仍在 {@link AcceptAnomalyAutoCheckService}，查询与状态推进仍通过
     * {@link AcceptAnomalyCheckRepositoryPort} 和 {@link RechargeOrderQueryPort} 两个端口完成，避免在调度层
     * 引入任何 Redis/MySQL 业务判断。
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
     * 创建异常核查的人工联调与 XXL-Job 共用 job facade。
     *
     * <p>该 Bean 是 {@code acceptAnomalyAutoCheckJob} handler 的真实执行目标。handler 只解析
     * {@code limit/workerId/batchId} 并委托这里，批量大小归一化和单条异常失败隔离仍复用
     * {@link AcceptAnomalyAutoCheckJob#runOnce(int, String)} 既有实现。
     */
    @Bean
    @ConditionalOnBean(AcceptAnomalyAutoCheckService.class)
    @ConditionalOnMissingBean(AcceptAnomalyAutoCheckJob.class)
    public AcceptAnomalyAutoCheckJob acceptAnomalyAutoCheckJob(
            AcceptAnomalyAutoCheckService service) {
        return new AcceptAnomalyAutoCheckJob(service);
    }
}
