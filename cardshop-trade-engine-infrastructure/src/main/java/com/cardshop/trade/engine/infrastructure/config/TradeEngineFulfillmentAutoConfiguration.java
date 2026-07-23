package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.BalanceDeltaFlushOutboxPort;
import com.cardshop.trade.engine.application.recharge.BalanceReservationReleasePort;
import com.cardshop.trade.engine.application.recharge.DirectRechargeFrequencyPort;
import com.cardshop.trade.engine.application.recharge.MerchantNotifyClientPort;
import com.cardshop.trade.engine.application.recharge.MerchantNotifyPayloadSigner;
import com.cardshop.trade.engine.application.recharge.RechargeDispatchTimerPort;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerSettings;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeSupplierGatewayPort;
import com.cardshop.trade.engine.application.recharge.SupplierQueryOutboxPort;
import com.cardshop.trade.engine.application.recharge.V1MerchantNotifyPayloadSigner;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * 供应商履约、作废和商户回调 worker 的后置自动配置。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」、
 * 「9.3 作废设计」「9.4 回调设计」和「9.2 余额 Delta 刷账 Outbox」（余额释放入队）。
 * 这些 worker 同时依赖 MyBatis Mapper、RedisTemplate、
 * HTTP 供应商/回调客户端和商户资料端口；真实 dev/test/prod 中这些依赖常由 Spring Boot/MyBatis/Redis
 * 后置自动配置创建。若只放在普通 {@code @Configuration} 中用 {@code @ConditionalOnBean} 判断，条件可能在
 * 端口 BeanDefinition 出现前被判定为不满足，最终表现为应用健康但 `outbox-publish` 人工入口拿不到 job。
 *
 * <p>本类只在所有真实端口存在时创建 worker，不注册 noop worker。这样 SUP/INV/CB 联调缺任一真实依赖时会
 * 暴露为入口不可用或启动依赖错误，避免把“没有处理能力”伪装成“扫描为空”。
 */
@AutoConfiguration(afterName = {
        "com.cardshop.trade.engine.infrastructure.config.TradeEngineMybatisPersistenceAutoConfiguration",
        "com.cardshop.trade.engine.infrastructure.config.TradeEngineRedisPendingAutoConfiguration"
})
public class TradeEngineFulfillmentAutoConfiguration {

    /**
     * 创建供应商派单/作废 outbox worker。
     *
     * <p>该 worker 是 SUP-001/SUP-002/INV-001 的同一业务执行器，必须复用一套 CAS 抢占、供应商调用、
     * 余额释放、回调任务生成和状态回写语义。payload signer 允许测试显式注入；真实环境默认使用 V1 商户资料端口
     * 计算回调签名，保持和详细设计「9.4 回调设计」一致。
     * 失败/作废后的余额释放入队走 {@link BalanceDeltaFlushOutboxPort}（「9.2 余额 Delta 刷账 Outbox」）。
     */
    @Bean
    @ConditionalOnBean({
            RechargeOutboxPort.class,
            RechargeFulfillmentOrderPort.class,
            RechargeSupplierGatewayPort.class,
            SupplierQueryOutboxPort.class,
            DirectRechargeFrequencyPort.class,
            BalanceDeltaFlushOutboxPort.class,
            BalanceReservationReleasePort.class,
            RechargeNotifyTaskPort.class,
            V1MerchantProfilePort.class
    })
    @ConditionalOnMissingBean(RechargeOutboxWorkerService.class)
    public RechargeOutboxWorkerService rechargeOutboxWorkerService(
            RechargeOutboxPort outboxPort,
            RechargeFulfillmentOrderPort orderPort,
            RechargeSupplierGatewayPort supplierGatewayPort,
            SupplierQueryOutboxPort supplierQueryOutboxPort,
            DirectRechargeFrequencyPort frequencyPort,
            BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort,
            BalanceReservationReleasePort balanceReservationReleasePort,
            RechargeNotifyTaskPort notifyTaskPort,
            ObjectProvider<MerchantNotifyPayloadSigner> payloadSignerProvider,
            ObjectProvider<RechargeDispatchTimerPort> dispatchTimerPortProvider,
            V1MerchantProfilePort merchantProfilePort,
            Clock clock,
            TradeEngineProperties properties) {
        var fulfillment = properties.getFulfillment();
        MerchantNotifyPayloadSigner payloadSigner = payloadSignerProvider.getIfAvailable(
                () -> new V1MerchantNotifyPayloadSigner(merchantProfilePort));
        return new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGatewayPort,
                supplierQueryOutboxPort,
                frequencyPort,
                balanceDeltaFlushOutboxPort,
                balanceReservationReleasePort,
                notifyTaskPort,
                payloadSigner,
                RechargeFulfillmentConfigurationSupport.deferredRechargeDispatchTimerPort(dispatchTimerPortProvider),
                clock,
                RechargeFulfillmentConfigurationSupport.workerSettings(fulfillment));
    }

    /**
     * 创建供应商履约热路径 timer。
     *
     * <p>参见详细设计「9.1 派单 Outbox」。该 Bean 只在真实 outbox worker 存在后启用；timer 线程池大小复用
     * outbox 并行度，实际多节点互斥仍由 {@code recharge_dispatch_outbox} 的 CAS claim 完成。
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnBean(RechargeOutboxWorkerService.class)
    @ConditionalOnMissingBean(RechargeDispatchTimerPort.class)
    @ConditionalOnProperty(prefix = "trade.engine.fulfillment", name = "timer-enabled", havingValue = "true")
    public RechargeDispatchTimerPort rechargeDispatchTimer(
            RechargeOutboxWorkerService workerService,
            TradeEngineProperties properties) {
        return RechargeFulfillmentConfigurationSupport.rechargeDispatchTimer(workerService, properties);
    }

    /**
     * 创建商户回调重试 worker。
     *
     * <p>参见详细设计「9.3 回调设计」。notify worker 只在任务端口和 HTTP 回调客户端都存在时启用；
     * 回调失败会由 application service 写回 retry_count/next_retry_time，本配置只负责把真实端口接起来。
     */
    @Bean
    @ConditionalOnBean({RechargeNotifyWorkerTaskPort.class, MerchantNotifyClientPort.class})
    @ConditionalOnMissingBean(RechargeNotifyWorkerService.class)
    public RechargeNotifyWorkerService rechargeNotifyWorkerService(
            RechargeNotifyWorkerTaskPort taskPort,
            MerchantNotifyClientPort notifyClientPort,
            Clock clock,
            TradeEngineProperties properties) {
        return new RechargeNotifyWorkerService(
                taskPort,
                notifyClientPort,
                clock,
                new RechargeNotifyWorkerSettings(
                        Duration.ofSeconds(properties.getNotify().getClaimLeaseSeconds()),
                        Duration.ofSeconds(properties.getNotify().getRetryBackoffSeconds())));
    }

}
