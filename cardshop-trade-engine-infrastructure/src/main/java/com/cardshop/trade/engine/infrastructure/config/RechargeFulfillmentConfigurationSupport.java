package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.RechargeDispatchTimerPort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerSettings;
import com.cardshop.trade.engine.infrastructure.timer.ScheduledExecutorRechargeDispatchTimer;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import org.springframework.beans.factory.ObjectProvider;

/**
 * 供应商履约 worker/timer 自动配置的共享工厂。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。普通配置类和后置自动配置类都需要
 * 装配同一套 outbox worker 参数、timer workerId 和延迟注入端口；集中在这里避免灰度开关、timer 构造参数或
 * 查单退避默认值后续修改时出现两处语义漂移。
 */
final class RechargeFulfillmentConfigurationSupport {

    private RechargeFulfillmentConfigurationSupport() {}

    static RechargeOutboxWorkerSettings workerSettings(TradeEngineProperties.Fulfillment fulfillment) {
        return new RechargeOutboxWorkerSettings(
                Duration.ofSeconds(fulfillment.getOutboxClaimLeaseSeconds()),
                Duration.ofSeconds(fulfillment.getOutboxRetryBackoffSeconds()),
                Duration.ofSeconds(fulfillment.getFrequencyRetryDelaySeconds()),
                Duration.ofMinutes(fulfillment.getFrequencyLockTtlMinutes()),
                Duration.ofSeconds(fulfillment.getFrequencySuccessMinimumHoldSeconds()),
                fulfillment.getOutboxParallelism(),
                fulfillment.getOutboxScanOverfetchFactor(),
                fulfillment.supplierQueryRetryDelays());
    }

    static RechargeDispatchTimerPort rechargeDispatchTimer(
            RechargeOutboxWorkerService workerService,
            TradeEngineProperties properties) {
        return new ScheduledExecutorRechargeDispatchTimer(
                (eventId, eventType, workerId) -> workerService.processEvent(eventId, eventType, workerId),
                rechargeDispatchTimerWorkerId(),
                properties.getFulfillment().getOutboxParallelism(),
                properties.getFulfillment().getTimerMaxPendingTasks());
    }

    static RechargeDispatchTimerPort deferredRechargeDispatchTimerPort(
            ObjectProvider<RechargeDispatchTimerPort> provider) {
        return (eventId, eventType, delay) -> provider.getIfAvailable(RechargeDispatchTimerPort::noop)
                .schedule(eventId, eventType, delay);
    }

    private static String rechargeDispatchTimerWorkerId() {
        return "fulfillment-timer-" + ManagementFactory.getRuntimeMXBean().getName();
    }
}
