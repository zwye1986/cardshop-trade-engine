package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyTaskPort;
import com.cardshop.trade.engine.application.recharge.PersistedOrderStatePort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisPersistedOrderStateAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeFulfillmentOrderAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeBalanceDeltaFlushAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeNotifyTaskAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOutboxAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeNotifyTaskMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.baomidou.mybatisplus.autoconfigure.MybatisPlusAutoConfiguration;
import java.time.Clock;
import java.time.Duration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * MyBatis 落库端口的后置自动配置。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。真实 dev/test/prod
 * 环境中的 Mapper Bean 由 MyBatis-Plus 自动配置和 mapper scanner 注册；普通业务 {@code @Configuration} 早于
 * {@link MybatisPlusAutoConfiguration} 解析时，按 Mapper 做 {@code @ConditionalOnBean} 可能被过早判定为不满足，
 * 从而跳过 {@link RechargeOrderPersistencePort}。该端口缺失会继续导致
 * {@code PersistRechargeOrderConsumerService} 和 Rabbit listener 缺失，最终表现为应用健康但 RabbitMQ 没有
 * connection/consumer。
 *
 * <p>因此 MySQL 异步落库端口在 MyBatis 自动配置之后再兜底创建。基础设施普通配置类中的同名 Bean 仍保留给
 * slice test 或显式提供 Mapper 的局部上下文；本自动配置只要求 {@link SqlSessionFactory} 已经存在，然后把四个
 * 必需 Mapper 作为构造参数直接注入。若 mapper scan 漏掉任一 Mapper，dev/test/prod 应在启动期 fail-fast，
 * 不能静默缺失 Consumer 后继续对外显示健康。
 */
@AutoConfiguration(after = MybatisPlusAutoConfiguration.class)
@ConditionalOnClass({MybatisPlusAutoConfiguration.class, MybatisRechargeOrderPersistenceAdapter.class})
public class TradeEngineMybatisPersistenceAutoConfiguration {

    /**
     * 创建 Redis/MQ 到 MySQL 异步落库端口。
     *
     * <p>只以 {@link SqlSessionFactory} 作为 MyBatis 就绪条件，不再以 Mapper Bean 做条件判断。Mapper 作为方法参数
     * 注入，能够在所有 BeanDefinition 注册完成后的实例化阶段解析；若缺失则启动失败，避免半闭环消费 MQ 消息。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(RechargeOrderPersistencePort.class)
    public RechargeOrderPersistencePort rechargeOrderPersistencePort(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            RechargeOutboxMapper outboxMapper,
            McDepartRecordMapper departRecordMapper,
            ObjectMapper objectMapper) {
        return new MybatisRechargeOrderPersistenceAdapter(
                orderMapper,
                extMapper,
                outboxMapper,
                departRecordMapper,
                objectMapper);
    }

    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(PersistedOrderStatePort.class)
    public PersistedOrderStatePort persistedOrderStatePort(RechargeOrderExtMapper extMapper) {
        return new MybatisPersistedOrderStateAdapter(extMapper);
    }

    /**
     * 创建派单/作废 outbox 端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」。真实 dev/test/prod
     * 中 Mapper BeanDefinition 由 MyBatis-Plus 后置注册；这里以 {@link SqlSessionFactory} 作为 MyBatis
     * 就绪条件，并把 Mapper 放到方法参数上解析，避免普通配置类过早按 Mapper 条件判断导致 outbox worker
     * 缺少 {@link RechargeOutboxPort} 和 {@link com.cardshop.trade.engine.application.recharge.BalanceDeltaFlushOutboxPort}。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(RechargeOutboxPort.class)
    public MybatisRechargeOutboxAdapter rechargeOutboxPort(
            RechargeOutboxMapper outboxMapper,
            ObjectMapper objectMapper,
            Clock clock) {
        return new MybatisRechargeOutboxAdapter(outboxMapper, objectMapper, clock);
    }

    /**
     * 创建供应商履约订单读写端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.10 供应商流水」和「20.13.11 作废流程」。
     * SUP/INV 联调必须能读取旧订单、写供应商流水并推进 `recharge_order_ext.dispatch_status`；若任一 Mapper
     * 缺失，应在启动期暴露依赖错误，而不是静默跳过 worker。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(RechargeFulfillmentOrderPort.class)
    public RechargeFulfillmentOrderPort rechargeFulfillmentOrderPort(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            McOrderSupplierFlowMapper flowMapper,
            ObjectMapper objectMapper,
            Clock clock) {
        return new MybatisRechargeFulfillmentOrderAdapter(orderMapper, extMapper, flowMapper, objectMapper, clock);
    }

    /**
     * 创建商户回调任务端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.12 回调设计」。同一个 MyBatis adapter 同时承担
     * outbox 生成回调任务的写端和 notify worker 抢占/重试的读写端，因此返回具体类型，确保 Spring 能同时按
     * {@link RechargeNotifyTaskPort} 和
     * {@link com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerTaskPort} 匹配到同一个 Bean。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(RechargeNotifyTaskPort.class)
    public MybatisRechargeNotifyTaskAdapter rechargeNotifyTaskPort(
            RechargeNotifyTaskMapper notifyTaskMapper,
            ObjectMapper objectMapper,
            Clock clock) {
        return new MybatisRechargeNotifyTaskAdapter(notifyTaskMapper, objectMapper, clock);
    }

    @Bean
    @ConditionalOnBean({SqlSessionFactory.class, PlatformTransactionManager.class})
    @ConditionalOnMissingBean(RechargeBalanceDeltaFlushPort.class)
    public RechargeBalanceDeltaFlushPort rechargeBalanceDeltaFlushPort(
            RechargeOutboxMapper outboxMapper,
            McExtDepartMapper extDepartMapper,
            ObjectMapper objectMapper,
            Clock clock,
            TradeEngineProperties properties,
            PlatformTransactionManager transactionManager) {
        return new MybatisRechargeBalanceDeltaFlushAdapter(
                outboxMapper,
                extDepartMapper,
                objectMapper,
                clock,
                Duration.ofSeconds(properties.getFulfillment().getOutboxClaimLeaseSeconds()),
                Duration.ofSeconds(properties.getFulfillment().getOutboxRetryBackoffSeconds()),
                transactionManager);
    }
}
