package com.cardshop.trade.engine.bootstrap.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyAutoCheckService;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCase;
import com.cardshop.trade.engine.application.recharge.PendingPersistOrderPort;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckBatchResult;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckEvidence;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxBatchResult;
import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargePersistMqRepublishService;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import com.cardshop.trade.engine.job.recharge.AcceptAnomalyAutoCheckJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceDeltaFlushJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceReconcileJob;
import com.cardshop.trade.engine.job.recharge.RechargeCacheRepairJob;
import com.cardshop.trade.engine.job.recharge.RechargeOutboxPublishJob;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import com.xxl.job.core.handler.annotation.XxlJob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * XXL-Job 真实调度入口装配测试。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」和
 * {@code docs/开发进度.md} 第 7 节第 2 项。真实 handler 必须复用现有
 * {@link com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob#runOnce(int, int, int, String)}
 * 门面，不能另写一套补投逻辑；同时默认关闭 executor，避免 dev/test 未配置调度中心时影响应用启动。
 */
class TradeEngineXxlJobConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestBeans.class, TradeEngineReliabilityConfiguration.class)
            .withUserConfiguration(xxlJobConfigurationClass());

    @Test
    void doesNotStartXxlExecutorWhenSwitchIsDisabled() throws Exception {
        Class<?> executorType = Class.forName("com.xxl.job.core.executor.impl.XxlJobSpringExecutor");

        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(TradeEngineXxlJobHandlers.class);
            assertThat(context).doesNotHaveBean(executorType);
        });
    }

    @Test
    void registersPersistMqRepublishHandlerWhenJobFacadeIsCreatedByLateAutoConfiguration() throws Exception {
        /*
         * 复现 test188 的装配顺序：真实环境里补投 job facade 由后置 auto-configuration 创建，
         * 普通 TradeEngineXxlJobConfiguration 不能因为 @ConditionalOnBean 判断过早而跳过 @XxlJob handler。
         */
        new ApplicationContextRunner()
                .withUserConfiguration(LatePersistMqPorts.class, xxlJobConfigurationClass())
                .withConfiguration(AutoConfigurations.of(TradeEngineCompensationJobAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargePersistMqRepublishJob.class);
                    assertThat(context).hasSingleBean(TradeEngineXxlJobHandlers.class);
                });
    }

    @Test
    void registersAcceptAnomalyJobWhenPortsAreCreatedBeforeLateAutoConfiguration() throws Exception {
        /*
         * 复现 2026-07-11 test188 的第二个缺口：acceptAnomalyAutoCheckJob 的 @XxlJob handler 已注册，
         * 但普通 TradeEngineReliabilityConfiguration 在基础设施端口可见前完成条件判断，导致真正的 job facade 缺失。
         */
        new ApplicationContextRunner()
                .withUserConfiguration(LateAcceptAnomalyPorts.class, xxlJobConfigurationClass())
                .withConfiguration(AutoConfigurations.of(TradeEngineCompensationJobAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(AcceptAnomalyAutoCheckService.class);
                    assertThat(context).hasSingleBean(AcceptAnomalyAutoCheckJob.class);
                    assertThat(context).hasSingleBean(TradeEngineXxlJobHandlers.class);
                });
    }

    @Test
    void exposesAllImplementedXxlJobHandlerNames() {
        Set<String> handlerNames = Arrays.stream(TradeEngineXxlJobHandlers.class.getDeclaredMethods())
                .map(method -> method.getAnnotation(XxlJob.class))
                .filter(java.util.Objects::nonNull)
                .map(XxlJob::value)
                .collect(java.util.stream.Collectors.toSet());

        assertThat(handlerNames).containsExactlyInAnyOrder(
                "rechargePersistMqRepublishJob",
                "rechargeOutboxPublishJob",
                "rechargeNotifyRetryJob",
                "rechargeBalanceDeltaFlushJob",
                "rechargeBalanceReconcileJob",
                "rechargeCacheRepairJob",
                "acceptAnomalyAutoCheckJob");
    }

    @Test
    void createsXxlExecutorFromBoundPropertiesWhenSwitchIsEnabled() {
        TradeEngineProperties properties = new TradeEngineProperties();
        properties.getXxlJob().setEnabled(true);
        properties.getXxlJob().setAdminAddresses("http://127.0.0.1:8080/xxl-job-admin");
        properties.getXxlJob().setAccessToken("test-token");
        properties.getXxlJob().getExecutor().setAppname("cardshop-trade-engine-dev");
        properties.getXxlJob().getExecutor().setIp("127.0.0.1");
        properties.getXxlJob().getExecutor().setPort(19999);
        properties.getXxlJob().getExecutor().setLogpath("/tmp/cardshop-trade-engine/xxl-job");

        Object executor = new TradeEngineXxlJobConfiguration().xxlJobSpringExecutor(properties);

        assertThat(executor).isInstanceOf(com.xxl.job.core.executor.impl.XxlJobSpringExecutor.class);
    }

    @Test
    void handlerParsesXxlParametersAndDelegatesToExistingJobFacade() {
        RecordingPersistMqRepublishService service = new RecordingPersistMqRepublishService();
        TradeEngineXxlJobHandlers handler = new TradeEngineXxlJobHandlers(
                () -> new RechargePersistMqRepublishJob(service),
                () -> { throw new AssertionError("outbox job is not used by this test"); },
                () -> { throw new AssertionError("notify worker is not used by this test"); },
                () -> { throw new AssertionError("delta flush job is not used by this test"); },
                () -> { throw new AssertionError("reconcile job is not used by this test"); },
                () -> { throw new AssertionError("cache repair job is not used by this test"); },
                () -> { throw new AssertionError("anomaly job is not used by this test"); });

        RechargeCompensationBatchResult result = handler.runPersistMqRepublishFromXxlContext(
                4,
                2,
                99L,
                "limit=25,batchId=batch-1,workerId=xxl-worker-1");

        assertThat(result.scanned()).isEqualTo(25);
        assertThat(service.calls).containsExactly("4:2:25:xxl-worker-1");
    }

    @Test
    void everyImplementedHandlerDelegatesToItsJobFacade() {
        RechargePersistMqRepublishJob persistJob = mock(RechargePersistMqRepublishJob.class);
        RechargeOutboxPublishJob outboxJob = mock(RechargeOutboxPublishJob.class);
        RechargeNotifyWorkerService notifyWorker = mock(RechargeNotifyWorkerService.class);
        RechargeBalanceDeltaFlushJob deltaFlushJob = mock(RechargeBalanceDeltaFlushJob.class);
        RechargeBalanceReconcileJob reconcileJob = mock(RechargeBalanceReconcileJob.class);
        RechargeCacheRepairJob cacheRepairJob = mock(RechargeCacheRepairJob.class);
        AcceptAnomalyAutoCheckJob anomalyJob = mock(AcceptAnomalyAutoCheckJob.class);
        when(persistJob.runOnce(4, 2, 25, "xxl-worker-1"))
                .thenReturn(new RechargeCompensationBatchResult(25, 1, 0, 0));
        when(outboxJob.runOnce(4, 2, 25, "xxl-worker-1"))
                .thenReturn(new RechargeOutboxBatchResult(25, 1, 1, 0, 0));
        when(notifyWorker.processDue(25, "xxl-worker-1"))
                .thenReturn(new RechargeNotifyBatchResult(25, 1, 1, 0, 0));
        when(deltaFlushJob.runOnce(4, 2, 25, "xxl-worker-1"))
                .thenReturn(new RechargeCompensationBatchResult(25, 1, 0, 0));
        when(reconcileJob.runOnce(4, 2, 25, "xxl-worker-1"))
                .thenReturn(new RechargeCompensationBatchResult(25, 1, 0, 0));
        when(cacheRepairJob.runOnce(4, 2, 25, "xxl-worker-1"))
                .thenReturn(new RechargeCompensationBatchResult(25, 1, 0, 0));
        when(anomalyJob.runOnce(25, "xxl-worker-1"))
                .thenReturn(new AcceptAnomalyCheckBatchResult(25, 1, 0, 0, 0));
        TradeEngineXxlJobHandlers handler = new TradeEngineXxlJobHandlers(
                () -> persistJob,
                () -> outboxJob,
                () -> notifyWorker,
                () -> deltaFlushJob,
                () -> reconcileJob,
                () -> cacheRepairJob,
                () -> anomalyJob);
        String param = "limit=25,batchId=batch-1,workerId=xxl-worker-1";

        handler.runPersistMqRepublishFromXxlContext(4, 2, 99L, param);
        handler.runOutboxPublishFromXxlContext(4, 2, 99L, param);
        handler.runNotifyRetryFromXxlContext(4, 2, 99L, param);
        handler.runBalanceDeltaFlushFromXxlContext(4, 2, 99L, param);
        handler.runBalanceReconcileFromXxlContext(4, 2, 99L, param);
        handler.runCacheRepairFromXxlContext(4, 2, 99L, param);
        handler.runAcceptAnomalyAutoCheckFromXxlContext(4, 2, 99L, param);

        verify(persistJob).runOnce(4, 2, 25, "xxl-worker-1");
        verify(outboxJob).runOnce(4, 2, 25, "xxl-worker-1");
        verify(notifyWorker).processDue(25, "xxl-worker-1");
        verify(deltaFlushJob).runOnce(4, 2, 25, "xxl-worker-1");
        verify(reconcileJob).runOnce(4, 2, 25, "xxl-worker-1");
        verify(cacheRepairJob).runOnce(4, 2, 25, "xxl-worker-1");
        verify(anomalyJob).runOnce(25, "xxl-worker-1");
    }

    private static Class<?> xxlJobConfigurationClass() {
        try {
            return Class.forName("com.cardshop.trade.engine.bootstrap.config.TradeEngineXxlJobConfiguration");
        } catch (ClassNotFoundException ex) {
            throw new AssertionError("TradeEngineXxlJobConfiguration must exist for real XXL-Job handler wiring", ex);
        }
    }

    @EnableConfigurationProperties(TradeEngineProperties.class)
    static class TestBeans {

        @Bean
        PersistRechargeOrderMessagePublisher persistRechargeOrderMessagePublisher() {
            return new PersistRechargeOrderMessagePublisher() {
                @Override
                public boolean isAvailable() {
                    return true;
                }

                @Override
                public java.util.concurrent.CompletionStage<PersistMessagePublishResult> publish(
                        PersistTradeOrderMessage message) {
                    return CompletableFuture.completedFuture(PersistMessagePublishResult.confirmed(message.messageId(), 1));
                }
            };
        }

        @Bean
        PendingPersistOrderPort pendingPersistOrderPort() {
            return PendingPersistOrderPort.noop();
        }

        @Bean
        RechargePersistMqRepublishService rechargePersistMqRepublishService(
                PendingPersistOrderPort pendingPersistOrderPort,
                PersistRechargeOrderMessagePublisher publisher) {
            return new RechargePersistMqRepublishService(pendingPersistOrderPort, publisher);
        }
    }

    @EnableConfigurationProperties(TradeEngineProperties.class)
    static class LatePersistMqPorts {

        @Bean
        PersistRechargeOrderMessagePublisher persistRechargeOrderMessagePublisher() {
            return new TestBeans().persistRechargeOrderMessagePublisher();
        }
    }

    @EnableConfigurationProperties(TradeEngineProperties.class)
    static class LateAcceptAnomalyPorts {

        @Bean
        AcceptAnomalyCheckRepositoryPort acceptAnomalyCheckRepositoryPort() {
            return new AcceptAnomalyCheckRepositoryPort() {
                @Override
                public List<AcceptAnomalyCase> findOpenForCheck(int limit) {
                    return List.of(new AcceptAnomalyCase(
                            "accept-anomaly-1",
                            AcceptAnomalyType.ACCEPT_STATE_UNKNOWN,
                            "A36",
                            "OUT-1001",
                            "ORDER-1001",
                            "hash-1",
                            "",
                            java.util.Map.of()));
                }

                @Override
                public void markChecking(String anomalyId, String workerId) {
                    // 测试只验证 Bean 装配链路；核查状态推进由 application service 单测覆盖。
                }

                @Override
                public void markResolvedAccepted(
                        String anomalyId,
                        AcceptAnomalyCheckEvidence evidence,
                        String workerId) {
                    // 测试只验证 Bean 装配链路；核查状态推进由 application service 单测覆盖。
                }

                @Override
                public void markResolvedNotAccepted(
                        String anomalyId,
                        AcceptAnomalyCheckEvidence evidence,
                        String workerId) {
                    // 测试只验证 Bean 装配链路；核查状态推进由 application service 单测覆盖。
                }

                @Override
                public void markManualRequired(
                        String anomalyId,
                        AcceptAnomalyCheckEvidence evidence,
                        String reason,
                        String workerId) {
                    // 测试只验证 Bean 装配链路；核查状态推进由 application service 单测覆盖。
                }
            };
        }

        @Bean
        RechargeOrderQueryPort rechargeOrderQueryPort() {
            return (orgCode, outOrderSn) -> Optional.empty();
        }
    }

    private static final class RecordingPersistMqRepublishService extends RechargePersistMqRepublishService {

        private final List<String> calls = new ArrayList<>();

        private RecordingPersistMqRepublishService() {
            super(PendingPersistOrderPort.noop(), new TestBeans().persistRechargeOrderMessagePublisher());
        }

        @Override
        public RechargeCompensationBatchResult republishDue(int shardTotal, int shardIndex, int limit, String workerId) {
            calls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return new RechargeCompensationBatchResult(limit, 0, 0, 0);
        }
    }
}
