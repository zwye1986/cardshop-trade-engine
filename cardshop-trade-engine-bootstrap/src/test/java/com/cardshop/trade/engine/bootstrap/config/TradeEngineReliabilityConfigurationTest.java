package com.cardshop.trade.engine.bootstrap.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyAutoCheckService;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCase;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckEvidence;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.CancelUnconfirmedAcceptResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistQueuedResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistStateResult;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshotPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.application.recharge.PendingPersistOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushPort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushService;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcilePort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcileService;
import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairPort;
import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairService;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerSettings;
import com.cardshop.trade.engine.application.recharge.RechargePersistMqRepublishService;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.recharge.ReplicationWaitResult;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.job.recharge.AcceptAnomalyAutoCheckJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceDeltaFlushJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceReconcileJob;
import com.cardshop.trade.engine.job.recharge.RechargeCacheRepairJob;
import com.cardshop.trade.engine.job.recharge.RechargeOutboxPublishJob;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineInfrastructureConfiguration;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessageListener;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

/**
 * 交易可靠性补偿服务装配测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.3 Redis HA Guard」。只有异常核查仓储和订单查询端口
 * 同时存在时，才装配自动核查服务。
 */
class TradeEngineReliabilityConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TradeEngineReliabilityConfiguration.class);

    @Test
    void registersAcceptAnomalyAutoCheckServiceWhenPortsArePresent() {
        contextRunner
                .withBean(AcceptAnomalyCheckRepositoryPort.class, StubRepository::new)
                .withBean(RechargeOrderQueryPort.class, () -> (orgCode, outOrderSn) -> java.util.Optional.empty())
                .run(context -> {
                    assertThat(context).hasSingleBean(AcceptAnomalyAutoCheckService.class);
                    assertThat(context).hasSingleBean(AcceptAnomalyAutoCheckJob.class);
                });
    }

    @Test
    void doesNotRegisterAcceptAnomalyAutoCheckServiceWhenRepositoryIsMissing() {
        contextRunner
                .withBean(RechargeOrderQueryPort.class, () -> (orgCode, outOrderSn) -> java.util.Optional.empty())
                .run(context -> assertThat(context).doesNotHaveBean(AcceptAnomalyAutoCheckService.class));
    }

    @Test
    void registersPersistRechargeOrderConsumerServiceWhenPortsArePresent() {
        contextRunner
                .withBean(PersistOrderSnapshotPort.class, PersistOrderSnapshotPort::noop)
                .withBean(RechargeOrderPersistencePort.class, () -> aggregate -> {
                })
                .withBean(RechargeAcceptReliabilityPort.class, StubReliabilityPort::new)
                .withBean(Clock.class, Clock::systemUTC)
                .run(context -> assertThat(context).hasSingleBean(PersistRechargeOrderConsumerService.class));
    }

    @Test
    void registersD11D12CompensationServicesAndJobFacadesWithNoopPorts() {
        contextRunner
                .withBean(PersistRechargeOrderMessagePublisher.class, StubPersistPublisher::new)
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargePersistMqRepublishService.class);
                    assertThat(context).hasSingleBean(RechargePersistMqRepublishJob.class);
                    assertThat(context).hasSingleBean(RechargeBalanceDeltaFlushService.class);
                    assertThat(context).hasSingleBean(RechargeBalanceDeltaFlushJob.class);
                    assertThat(context).hasSingleBean(RechargeBalanceReconcileService.class);
                    assertThat(context).hasSingleBean(RechargeBalanceReconcileJob.class);
                    assertThat(context).hasSingleBean(RechargeCacheRepairService.class);
                    assertThat(context).hasSingleBean(RechargeCacheRepairJob.class);
                    assertThat(context).doesNotHaveBean(PendingPersistOrderPort.class);
                    assertThat(context).doesNotHaveBean(RechargeBalanceDeltaFlushPort.class);
                    assertThat(context).doesNotHaveBean(RechargeBalanceReconcilePort.class);
                    assertThat(context).doesNotHaveBean(RechargeCacheRepairPort.class);
                });
    }

    @Test
    void registersOutboxPublishJobWhenOutboxWorkerServiceIsPresent() {
        contextRunner
                .withBean(RechargeOutboxWorkerService.class, TradeEngineReliabilityConfigurationTest::outboxWorkerService)
                .run(context -> assertThat(context).hasSingleBean(RechargeOutboxPublishJob.class));
    }

    @Test
    void registersRabbitPersistListenerWhenConsumerComesFromReliabilityConfiguration() {
        new ApplicationContextRunner()
                .withUserConfiguration(
                        TradeEngineInfrastructureConfiguration.class,
                        RabbitListenerDependencyBeans.class,
                        TradeEngineReliabilityConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(PersistRechargeOrderConsumerService.class);
                    assertThat(context).hasSingleBean(RabbitPersistRechargeOrderMessageListener.class);
                });
    }

    private static final class StubPersistPublisher implements PersistRechargeOrderMessagePublisher {

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public java.util.concurrent.CompletionStage<PersistMessagePublishResult> publish(PersistTradeOrderMessage message) {
            return CompletableFuture.completedFuture(PersistMessagePublishResult.confirmed(message.messageId(), 1));
        }
    }

    private static RechargeOutboxWorkerService outboxWorkerService() {
        return new RechargeOutboxWorkerService(
                new com.cardshop.trade.engine.application.recharge.RechargeOutboxPort() {
                    @Override
                    public List<com.cardshop.trade.engine.application.recharge.RechargeOutboxEvent> selectDueForShard(
                            int shardTotal,
                            int shardIndex,
                            int limit) {
                        return List.of();
                    }

                    @Override
                    public Optional<com.cardshop.trade.engine.application.recharge.RechargeOutboxEvent> claim(
                            com.cardshop.trade.engine.application.recharge.RechargeOutboxEvent event,
                            String workerId,
                            Duration lease) {
                        return Optional.empty();
                    }

                    @Override
                    public void markSuccess(com.cardshop.trade.engine.application.recharge.RechargeOutboxEvent event) {
                    }

                    @Override
                    public void markFailed(
                            com.cardshop.trade.engine.application.recharge.RechargeOutboxEvent event,
                            String errorMessage,
                            Duration nextRetryDelay) {
                    }

                    @Override
                    public void markManualRequired(
                            com.cardshop.trade.engine.application.recharge.RechargeOutboxEvent event,
                            String errorMessage) {
                    }
                },
                new com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort() {
                    @Override
                    public Optional<com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrder> findByOrderId(
                            String orderId) {
                        return Optional.empty();
                    }

                    @Override
                    public long insertSupplierFlowStart(
                            com.cardshop.trade.engine.application.recharge.SupplierFlowStartCommand command) {
                        return 0L;
                    }

                    @Override
                    public void updateSupplierFlow(
                            com.cardshop.trade.engine.application.recharge.SupplierFlowUpdateCommand command) {
                    }

                    @Override
                    public void updateFulfillmentStatus(
                            com.cardshop.trade.engine.application.recharge.OrderFulfillmentStatusUpdate update) {
                    }

                    @Override
                    public void markBalanceReleased(String orderId) {
                    }
                },
                new com.cardshop.trade.engine.application.recharge.RechargeSupplierGatewayPort() {
                    @Override
                    public com.cardshop.trade.engine.application.recharge.SupplierGatewayResponse dispatch(
                            com.cardshop.trade.engine.application.recharge.SupplierDispatchGatewayRequest request) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public com.cardshop.trade.engine.application.recharge.SupplierGatewayResponse invalid(
                            com.cardshop.trade.engine.application.recharge.SupplierInvalidGatewayRequest request) {
                        throw new UnsupportedOperationException();
                    }
                },
                new com.cardshop.trade.engine.application.recharge.DirectRechargeFrequencyPort() {
                    @Override
                    public boolean acquire(String orgCode, String account, String orderId, Duration ttl) {
                        return true;
                    }

                    @Override
                    public void releaseIfOwned(String orgCode, String account, String orderId, Duration minimumHold) {
                    }
                },
                com.cardshop.trade.engine.application.recharge.BalanceDeltaFlushOutboxPort.noop(),
                com.cardshop.trade.engine.application.recharge.BalanceReservationReleasePort.noop(),
                task -> {
                },
                new com.cardshop.trade.engine.application.recharge.MerchantNotifyPayloadSigner() {
                    @Override
                    public Map<String, String> signOrderStatus(
                            com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrder order,
                            String orderStatus,
                            java.time.Instant now) {
                        return Map.of();
                    }

                    @Override
                    public Map<String, String> signWriteOff(
                            com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrder order,
                            java.time.Instant invalidTime) {
                        return Map.of();
                    }
                },
                Clock.systemUTC(),
                new RechargeOutboxWorkerSettings(
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(5),
                        Duration.ofSeconds(30)));
    }

    private static final class StubRepository implements AcceptAnomalyCheckRepositoryPort {

        @Override
        public List<AcceptAnomalyCase> findOpenForCheck(int limit) {
            return List.of();
        }

        @Override
        public void markChecking(String anomalyId, String workerId) {
        }

        @Override
        public void markResolvedAccepted(String anomalyId, AcceptAnomalyCheckEvidence evidence, String workerId) {
        }

        @Override
        public void markResolvedNotAccepted(String anomalyId, AcceptAnomalyCheckEvidence evidence, String workerId) {
        }

        @Override
        public void markManualRequired(
                String anomalyId,
                AcceptAnomalyCheckEvidence evidence,
                String reason,
                String workerId) {
        }
    }

    private static final class StubReliabilityPort implements RechargeAcceptReliabilityPort {

        @Override
        public java.util.concurrent.CompletionStage<ReplicationWaitResult> waitForReplication(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.CompletionStage<CancelUnconfirmedAcceptResult> cancelUnconfirmedAccept(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String reason) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.CompletionStage<MarkPersistQueuedResult> markPersistQueued(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String persistMessageId,
                int publishCount) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.CompletionStage<MarkPersistStateResult> markDbPersisted(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                long persistedVersion,
                long nowEpochMillis) {
            return java.util.concurrent.CompletableFuture.completedFuture(MarkPersistStateResult.success());
        }

        @Override
        public java.util.concurrent.CompletionStage<MarkPersistStateResult> markPersistFailed(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String failCode,
                String failReason,
                int retryCount,
                long nextRetryEpochMillis,
                long nowEpochMillis) {
            return java.util.concurrent.CompletableFuture.completedFuture(MarkPersistStateResult.success());
        }
    }

    @org.springframework.boot.test.context.TestConfiguration
    static class RabbitListenerDependencyBeans {

        @Bean
        RabbitTemplate rabbitTemplate() {
            return new TestRabbitTemplate();
        }

        @Bean
        Executor tradeMqConfirmTaskExecutor() {
            return Runnable::run;
        }

        @Bean
        RabbitListenerContainerFactory<MessageListenerContainer> rabbitListenerContainerFactory() {
            return ignored -> interfaceProxy(MessageListenerContainer.class);
        }

        @Bean
        RechargeOrderPersistencePort rechargeOrderPersistencePort() {
            return aggregate -> {
            };
        }
    }

    private static final class TestRabbitTemplate extends RabbitTemplate {

        @Override
        public void afterPropertiesSet() {
            // 装配测试只需要 RabbitTemplate bean 存在，不需要真实 RabbitMQ ConnectionFactory。
        }
    }

    private static <T> T interfaceProxy(Class<T> type) {
        Object proxy = Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[]{type},
                (ignored, method, args) -> {
                    Class<?> returnType = method.getReturnType();
                    if (returnType == boolean.class) {
                        return false;
                    }
                    if (returnType.isPrimitive()) {
                        return 0;
                    }
                    return null;
                });
        return type.cast(proxy);
    }
}
