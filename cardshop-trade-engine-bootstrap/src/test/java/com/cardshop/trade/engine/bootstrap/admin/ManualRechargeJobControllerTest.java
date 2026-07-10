package com.cardshop.trade.engine.bootstrap.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.cardshop.trade.engine.application.recharge.PendingPersistOrderPort;
import com.cardshop.trade.engine.application.recharge.PendingPersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.application.recharge.BalanceReservationReleasePort;
import com.cardshop.trade.engine.application.recharge.DirectRechargeFrequencyPort;
import com.cardshop.trade.engine.application.recharge.MerchantNotifyPayloadSigner;
import com.cardshop.trade.engine.application.recharge.MerchantNotifySendResult;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushPort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushService;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcilePort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcileService;
import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairPort;
import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairService;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerSettings;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerSettings;
import com.cardshop.trade.engine.application.recharge.RechargePersistMqRepublishService;
import com.cardshop.trade.engine.application.recharge.RechargeSupplierGatewayPort;
import com.cardshop.trade.engine.bootstrap.config.TradeEngineCompensationJobAutoConfiguration;
import com.cardshop.trade.engine.bootstrap.config.TradeEngineReliabilityConfiguration;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceDeltaFlushJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceReconcileJob;
import com.cardshop.trade.engine.job.recharge.RechargeCacheRepairJob;
import com.cardshop.trade.engine.job.recharge.RechargeOutboxPublishJob;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

/**
 * Redis pending 补投人工触发入口测试。
 *
 * <p>该入口仅服务 dev/test 真实联调，默认关闭，且只允许 dev/test 测试机本机回环地址触发。测试不连接真实
 * Redis/RabbitMQ/MySQL，只验证开关、profile、参数校验和调用边界，避免为了联调入口引入额外环境依赖。
 */
class ManualRechargeJobControllerTest {

    @Test
    void registersOnlyWhenDevOrTestProfileAndManualSwitchEnabled() {
        new ApplicationContextRunner()
                .withUserConfiguration(
                        ManualJobPropertiesConfiguration.class,
                        ManualJobTestConfiguration.class,
                        ManualRechargeJobController.class)
                .withPropertyValues(
                        "spring.profiles.active=dev",
                        "trade.engine.integration.manual-job-enabled=true")
                .run(context -> assertThat(context).hasSingleBean(ManualRechargeJobController.class));

        new ApplicationContextRunner()
                .withUserConfiguration(
                        ManualJobPropertiesConfiguration.class,
                        ManualJobTestConfiguration.class,
                        ManualRechargeJobController.class)
                .withPropertyValues("spring.profiles.active=test")
                .run(context -> assertThat(context).doesNotHaveBean(ManualRechargeJobController.class));

        new ApplicationContextRunner()
                .withUserConfiguration(
                        ManualJobPropertiesConfiguration.class,
                        ManualJobTestConfiguration.class,
                        ManualRechargeJobController.class)
                .withPropertyValues(
                        "spring.profiles.active=prod",
                        "trade.engine.integration.manual-job-enabled=true")
                .run(context -> assertThat(context).doesNotHaveBean(ManualRechargeJobController.class));
    }

    @Test
    void registersWithRepublishJobWhenPublisherComesFromLaterAutoConfiguration() {
        new ApplicationContextRunner()
                .withUserConfiguration(TradeEngineReliabilityConfiguration.class, ManualRechargeJobController.class)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withConfiguration(AutoConfigurations.of(
                        LaterRepublishDependencyAutoConfiguration.class,
                        TradeEngineCompensationJobAutoConfiguration.class))
                .withPropertyValues(
                        "spring.profiles.active=dev",
                        "trade.engine.integration.manual-job-enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(PersistRechargeOrderMessagePublisher.class);
                    assertThat(context).hasSingleBean(RechargePersistMqRepublishService.class);
                    assertThat(context).hasSingleBean(RechargePersistMqRepublishJob.class);
                    assertThat(context).hasSingleBean(ManualRechargeJobController.class);
                });
    }

    @Test
    void usesLateRedisPendingPortWhenPublisherIsAvailableBeforeInfrastructurePendingPort() {
        new ApplicationContextRunner()
                .withUserConfiguration(
                        TradeEngineReliabilityConfiguration.class,
                        EarlyPublisherConfiguration.class,
                        ManualRechargeJobController.class)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withConfiguration(AutoConfigurations.of(
                        LaterRedisPendingPortAutoConfiguration.class,
                        TradeEngineCompensationJobAutoConfiguration.class))
                .withPropertyValues(
                        "spring.profiles.active=dev",
                        "trade.engine.integration.manual-job-enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(ManualRechargeJobController.class);
                    RecordingPendingPort pendingPort = context.getBean(RecordingPendingPort.class);
                    ManualRechargeJobController controller = context.getBean(ManualRechargeJobController.class);
                    var request = new ManualRechargeJobController.ManualJobRunRequest(1, 0, 10, "test188-manual-1");

                    var response = controller.runPersistMqRepublishOnce(request, exchange("127.0.0.1")).block();

                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    assertThat(response.getBody()).isNotNull();
                    assertThat(response.getBody().scanned()).isEqualTo(1);
                    assertThat(response.getBody().succeeded()).isEqualTo(1);
                    assertThat(pendingPort.selectCalls).containsExactly("1:0:10:test188-manual-1");
                });
    }

    @Test
    void registersWithOutboxPublishJobWhenWorkerComesFromLaterAutoConfiguration() {
        new ApplicationContextRunner()
                .withUserConfiguration(
                        TradeEngineReliabilityConfiguration.class,
                        EarlyPublisherConfiguration.class,
                        ManualRechargeJobController.class)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withConfiguration(AutoConfigurations.of(
                        LaterOutboxWorkerAutoConfiguration.class,
                        TradeEngineCompensationJobAutoConfiguration.class))
                .withPropertyValues(
                        "spring.profiles.active=dev",
                        "trade.engine.integration.manual-job-enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(RechargeOutboxWorkerService.class);
                    assertThat(context).hasSingleBean(RechargeOutboxPublishJob.class);
                    assertThat(context).hasSingleBean(ManualRechargeJobController.class);

                    var controller = context.getBean(ManualRechargeJobController.class);
                    var response = controller.runOutboxPublishOnce(
                                    new ManualRechargeJobController.ManualJobRunRequest(
                                            1,
                                            0,
                                            1,
                                            "test188-outbox-late"),
                                    exchange("127.0.0.1"))
                            .block();

                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    assertThat(response.getBody()).isNotNull();
                    assertThat(response.getBody().status()).isEqualTo("OK");
                });
    }


    @Test
    void runsPersistMqRepublishJobForLoopbackRequest() {
        RecordingPendingPort pendingPort = new RecordingPendingPort(List.of(pendingSnapshot("ORDER-1")));
        ManualRechargeJobController controller = controller(pendingPort);
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 0, 10, "test188-manual-1");

        var response = controller.runPersistMqRepublishOnce(request, exchange("127.0.0.1")).block();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("OK");
        assertThat(response.getBody().scanned()).isEqualTo(1);
        assertThat(response.getBody().succeeded()).isEqualTo(1);
        assertThat(pendingPort.selectCalls).containsExactly("1:0:10:test188-manual-1");
        assertThat(pendingPort.markedRepublished).containsExactly("ORDER-1");
    }

    @Test
    void offloadsPersistMqRepublishJobAwayFromCallerThread() {
        RecordingPendingPort pendingPort = new RecordingPendingPort(List.of(pendingSnapshot("ORDER-1")));
        ManualRechargeJobController controller = controller(pendingPort);
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 0, 10, "test188-manual-1");
        String callerThread = Thread.currentThread().getName();

        var response = controller.runPersistMqRepublishOnce(request, exchange("127.0.0.1")).block();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(pendingPort.selectThreads).hasSize(1);
        assertThat(pendingPort.selectThreads.getFirst()).isNotEqualTo(callerThread);
    }

    @Test
    void returnsFailedResponseWhenJobThrowsAfterValidation() {
        ManualRechargeJobController controller = controller(new FailingPendingPort("redis connection interrupted"));
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 0, 10, "test188-manual-1");

        var response = controller.runPersistMqRepublishOnce(request, exchange("127.0.0.1")).block();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("FAILED");
        assertThat(response.getBody().parameters()).isEqualTo(
                new ManualRechargeJobController.ManualJobParameters(1, 0, 10, "test188-manual-1"));
        assertThat(response.getBody().message()).contains("redis connection interrupted");
    }

    @Test
    void returnsFailedResponseWhenJobTimesOutWithoutTreatingTimeoutAsThreadInterruption() throws Exception {
        BlockingPendingPort pendingPort = new BlockingPendingPort();
        ManualRechargeJobController controller = controller(pendingPort, Duration.ofMillis(50));
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 0, 10, "test188-manual-timeout");

        try {
            var response = controller.runPersistMqRepublishOnce(request, exchange("127.0.0.1")).block(Duration.ofSeconds(2));

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.GATEWAY_TIMEOUT);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().status()).isEqualTo("FAILED");
            assertThat(response.getBody().parameters()).isEqualTo(
                    new ManualRechargeJobController.ManualJobParameters(1, 0, 10, "test188-manual-timeout"));
            assertThat(response.getBody().message()).contains("timed out");
            assertThat(pendingPort.awaitStarted()).isTrue();
            assertThat(pendingPort.isReleased()).isFalse();
        } finally {
            pendingPort.release();
        }
    }

    @Test
    void fallsBackToDefaultTimeoutWhenConfiguredTimeoutIsNonPositive() {
        TradeEngineProperties properties = new TradeEngineProperties();
        properties.getIntegration().setManualJobTimeoutMs(0);
        ManualRechargeJobController controller = controller(new DelayedPendingPort(Duration.ofMillis(25)), properties);
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 0, 10, "test188-manual-default-timeout");

        var response = controller.runPersistMqRepublishOnce(request, exchange("127.0.0.1")).block(Duration.ofSeconds(2));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("OK");
    }

    @Test
    void rejectsNonLoopbackRequestBeforeJobRuns() {
        RecordingPendingPort pendingPort = new RecordingPendingPort(List.of(pendingSnapshot("ORDER-1")));
        ManualRechargeJobController controller = controller(pendingPort);
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 0, 10, "test188-manual-1");

        var response = controller.runPersistMqRepublishOnce(request, exchange("203.0.113.10")).block();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("REJECTED");
        assertThat(pendingPort.selectCalls).isEmpty();
    }

    @Test
    void rejectsInvalidParametersBeforeJobRuns() {
        RecordingPendingPort pendingPort = new RecordingPendingPort(List.of(pendingSnapshot("ORDER-1")));
        ManualRechargeJobController controller = controller(pendingPort);
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 1, 10, "test188-manual-1");

        assertThatThrownBy(() -> controller.runPersistMqRepublishOnce(request, exchange("127.0.0.1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shardIndex must be less than shardTotal");
        assertThat(pendingPort.selectCalls).isEmpty();
    }

    @Test
    void serializesManualRunResponseWithDefaultJacksonMapper() throws Exception {
        var parameters = new ManualRechargeJobController.ManualJobParameters(1, 0, 1, "test188-smoke");
        var response = ManualRechargeJobController.ManualJobRunResponse.succeeded(
                parameters,
                new RechargeCompensationBatchResult(0, 0, 0, 0));

        String json = new ObjectMapper().writeValueAsString(response);

        assertThat(json).contains("\"status\":\"OK\"");
        assertThat(json).contains("\"triggeredAt\"");
        assertThat(json).contains("\"scanned\":0");
    }

    @Test
    void runsOutboxPublishJobForLoopbackRequest() {
        RecordingOutboxWorkerService outboxWorkerService = new RecordingOutboxWorkerService(
                new RechargeOutboxBatchResult(2, 2, 1, 1, 0));
        ManualRechargeJobController controller = controllerWithCompensationJobs(
                new RechargeOutboxPublishJob(outboxWorkerService),
                new RecordingNotifyWorkerService(new RechargeNotifyBatchResult(0, 0, 0, 0, 0)),
                new RechargeBalanceDeltaFlushJob(new RechargeBalanceDeltaFlushService(RechargeBalanceDeltaFlushPort.noop())));
        var request = new ManualRechargeJobController.ManualJobRunRequest(4, 2, 20, "test188-outbox-worker");

        var response = controller.runOutboxPublishOnce(request, exchange("127.0.0.1")).block();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("OK");
        assertThat(response.getBody().scanned()).isEqualTo(2);
        assertThat(response.getBody().succeeded()).isEqualTo(1);
        assertThat(response.getBody().failed()).isEqualTo(1);
        assertThat(outboxWorkerService.calls).containsExactly("4:2:20:test188-outbox-worker");
    }

    @Test
    void runsNotifyRetryWorkerForLoopbackRequest() {
        RecordingNotifyWorkerService notifyWorkerService = new RecordingNotifyWorkerService(
                new RechargeNotifyBatchResult(3, 3, 2, 1, 0));
        ManualRechargeJobController controller = controllerWithCompensationJobs(
                new RechargeOutboxPublishJob(new RecordingOutboxWorkerService(RechargeOutboxBatchResult.empty())),
                notifyWorkerService,
                new RechargeBalanceDeltaFlushJob(new RechargeBalanceDeltaFlushService(RechargeBalanceDeltaFlushPort.noop())));
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 0, 30, "test188-notify-worker");

        var response = controller.runNotifyRetryOnce(request, exchange("127.0.0.1")).block();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("OK");
        assertThat(response.getBody().scanned()).isEqualTo(3);
        assertThat(response.getBody().succeeded()).isEqualTo(2);
        assertThat(response.getBody().failed()).isEqualTo(1);
        assertThat(notifyWorkerService.calls).containsExactly("30:test188-notify-worker");
    }

    @Test
    void runsBalanceDeltaFlushJobForLoopbackRequest() {
        RecordingBalanceDeltaFlushPort balancePort = new RecordingBalanceDeltaFlushPort(
                new RechargeCompensationBatchResult(4, 4, 0, 0));
        ManualRechargeJobController controller = controllerWithCompensationJobs(
                new RechargeOutboxPublishJob(new RecordingOutboxWorkerService(RechargeOutboxBatchResult.empty())),
                new RecordingNotifyWorkerService(new RechargeNotifyBatchResult(0, 0, 0, 0, 0)),
                new RechargeBalanceDeltaFlushJob(new RechargeBalanceDeltaFlushService(balancePort)));
        var request = new ManualRechargeJobController.ManualJobRunRequest(8, 3, 40, "test188-balance-worker");

        var response = controller.runBalanceDeltaFlushOnce(request, exchange("127.0.0.1")).block();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("OK");
        assertThat(response.getBody().scanned()).isEqualTo(4);
        assertThat(response.getBody().succeeded()).isEqualTo(4);
        assertThat(balancePort.calls).containsExactly("8:3:40:test188-balance-worker");
    }

    @Test
    void runsBalanceReconcileJobForLoopbackRequest() {
        RecordingBalanceReconcilePort reconcilePort = new RecordingBalanceReconcilePort(
                new RechargeCompensationBatchResult(2, 2, 0, 0));
        ManualRechargeJobController controller = controllerWithAllCompensationJobs(
                new RechargeBalanceReconcileJob(new RechargeBalanceReconcileService(reconcilePort)),
                new RechargeCacheRepairJob(new RechargeCacheRepairService(RechargeCacheRepairPort.noop())));
        var request = new ManualRechargeJobController.ManualJobRunRequest(4, 1, 25, "test188-reconcile-worker");

        var response = controller.runBalanceReconcileOnce(request, exchange("127.0.0.1")).block();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("OK");
        assertThat(response.getBody().scanned()).isEqualTo(2);
        assertThat(response.getBody().succeeded()).isEqualTo(2);
        assertThat(reconcilePort.calls).containsExactly("4:1:25:test188-reconcile-worker");
    }

    @Test
    void runsCacheRepairJobForLoopbackRequest() {
        RecordingCacheRepairPort cacheRepairPort = new RecordingCacheRepairPort(
                new RechargeCompensationBatchResult(1, 1, 0, 0));
        ManualRechargeJobController controller = controllerWithAllCompensationJobs(
                new RechargeBalanceReconcileJob(new RechargeBalanceReconcileService(RechargeBalanceReconcilePort.noop())),
                new RechargeCacheRepairJob(new RechargeCacheRepairService(cacheRepairPort)));
        var request = new ManualRechargeJobController.ManualJobRunRequest(2, 0, 5, "test188-cache-worker");

        var response = controller.runCacheRepairOnce(request, exchange("127.0.0.1")).block();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("OK");
        assertThat(response.getBody().scanned()).isEqualTo(1);
        assertThat(response.getBody().succeeded()).isEqualTo(1);
        assertThat(cacheRepairPort.calls).containsExactly("2:0:5:test188-cache-worker");
    }

    @Test
    void rejectsNonLoopbackOutboxRequestBeforeJobRuns() {
        RecordingOutboxWorkerService outboxWorkerService = new RecordingOutboxWorkerService(
                new RechargeOutboxBatchResult(1, 1, 1, 0, 0));
        ManualRechargeJobController controller = controllerWithCompensationJobs(
                new RechargeOutboxPublishJob(outboxWorkerService),
                new RecordingNotifyWorkerService(new RechargeNotifyBatchResult(0, 0, 0, 0, 0)),
                new RechargeBalanceDeltaFlushJob(new RechargeBalanceDeltaFlushService(RechargeBalanceDeltaFlushPort.noop())));
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 0, 10, "test188-outbox-worker");

        var response = controller.runOutboxPublishOnce(request, exchange("203.0.113.10")).block();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("REJECTED");
        assertThat(outboxWorkerService.calls).isEmpty();
    }

    @Test
    void rejectsInvalidBalanceDeltaFlushParametersBeforeJobRuns() {
        RecordingBalanceDeltaFlushPort balancePort = new RecordingBalanceDeltaFlushPort(
                new RechargeCompensationBatchResult(1, 1, 0, 0));
        ManualRechargeJobController controller = controllerWithCompensationJobs(
                new RechargeOutboxPublishJob(new RecordingOutboxWorkerService(RechargeOutboxBatchResult.empty())),
                new RecordingNotifyWorkerService(new RechargeNotifyBatchResult(0, 0, 0, 0, 0)),
                new RechargeBalanceDeltaFlushJob(new RechargeBalanceDeltaFlushService(balancePort)));
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 1, 10, "test188-balance-worker");

        assertThatThrownBy(() -> controller.runBalanceDeltaFlushOnce(request, exchange("127.0.0.1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shardIndex must be less than shardTotal");
        assertThat(balancePort.calls).isEmpty();
    }

    private static ManualRechargeJobController controller(RecordingPendingPort pendingPort) {
        return controller(pendingPort, Duration.ofSeconds(30));
    }

    private static ManualRechargeJobController controller(PendingPersistOrderPort pendingPort) {
        return controller(pendingPort, Duration.ofSeconds(30));
    }

    private static ManualRechargeJobController controller(PendingPersistOrderPort pendingPort, Duration timeout) {
        var service = new RechargePersistMqRepublishService(pendingPort, new AlwaysConfirmedPublisher());
        return new ManualRechargeJobController(new RechargePersistMqRepublishJob(service), timeout);
    }

    private static ManualRechargeJobController controller(
            PendingPersistOrderPort pendingPort,
            TradeEngineProperties properties) {
        var service = new RechargePersistMqRepublishService(pendingPort, new AlwaysConfirmedPublisher());
        return new ManualRechargeJobController(new RechargePersistMqRepublishJob(service), properties);
    }

    private static MockServerWebExchange exchange(String host) {
        var request = MockServerHttpRequest.post("/internal/integration/recharge/jobs/persist-mq-republish/run-once")
                .remoteAddress(new InetSocketAddress(host, 18080))
                .build();
        return MockServerWebExchange.from(request);
    }

    private static ManualRechargeJobController controllerWithCompensationJobs(
            RechargeOutboxPublishJob outboxPublishJob,
            RechargeNotifyWorkerService notifyWorkerService,
            RechargeBalanceDeltaFlushJob balanceDeltaFlushJob) {
        return new ManualRechargeJobController(
                new RechargePersistMqRepublishJob(
                        new RechargePersistMqRepublishService(
                                PendingPersistOrderPort.noop(),
                                new AlwaysConfirmedPublisher())),
                Optional.of(outboxPublishJob),
                Optional.of(notifyWorkerService),
                Optional.of(balanceDeltaFlushJob),
                Optional.empty(),
                Optional.empty(),
                Duration.ofSeconds(30));
    }

    private static ManualRechargeJobController controllerWithAllCompensationJobs(
            RechargeBalanceReconcileJob balanceReconcileJob,
            RechargeCacheRepairJob cacheRepairJob) {
        return new ManualRechargeJobController(
                new RechargePersistMqRepublishJob(
                        new RechargePersistMqRepublishService(
                                PendingPersistOrderPort.noop(),
                                new AlwaysConfirmedPublisher())),
                Optional.of(new RechargeOutboxPublishJob(new RecordingOutboxWorkerService(RechargeOutboxBatchResult.empty()))),
                Optional.of(new RecordingNotifyWorkerService(new RechargeNotifyBatchResult(0, 0, 0, 0, 0))),
                Optional.of(new RechargeBalanceDeltaFlushJob(new RechargeBalanceDeltaFlushService(RechargeBalanceDeltaFlushPort.noop()))),
                Optional.of(balanceReconcileJob),
                Optional.of(cacheRepairJob),
                Duration.ofSeconds(30));
    }

    private static PendingPersistOrderSnapshot pendingSnapshot(String orderId) {
        return new PendingPersistOrderSnapshot(
                "MSG-" + orderId,
                "A05",
                orderId,
                "OUT-" + orderId,
                "HASH-" + orderId,
                1L,
                Map.of("order_id", orderId));
    }

    private static final class RecordingPendingPort implements PendingPersistOrderPort {

        private final List<PendingPersistOrderSnapshot> snapshots;
        private final List<String> selectCalls = new ArrayList<>();
        private final List<String> selectThreads = new ArrayList<>();
        private final List<String> markedRepublished = new ArrayList<>();

        private RecordingPendingPort(List<PendingPersistOrderSnapshot> snapshots) {
            this.snapshots = snapshots;
        }

        @Override
        public List<PendingPersistOrderSnapshot> selectDueForShard(
                int shardTotal,
                int shardIndex,
                int limit,
                String workerId) {
            selectCalls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            selectThreads.add(Thread.currentThread().getName());
            return snapshots;
        }

        @Override
        public void markRepublished(
                PendingPersistOrderSnapshot snapshot,
                PersistMessagePublishResult result,
                String workerId) {
            markedRepublished.add(snapshot.orderId());
        }

        @Override
        public void markRepublishFailed(PendingPersistOrderSnapshot snapshot, String reason, String workerId) {
        }
    }

    private static final class FailingPendingPort implements PendingPersistOrderPort {

        private final String message;

        private FailingPendingPort(String message) {
            this.message = message;
        }

        @Override
        public List<PendingPersistOrderSnapshot> selectDueForShard(
                int shardTotal,
                int shardIndex,
                int limit,
                String workerId) {
            throw new IllegalStateException(message);
        }

        @Override
        public void markRepublished(
                PendingPersistOrderSnapshot snapshot,
                PersistMessagePublishResult result,
                String workerId) {
        }

        @Override
        public void markRepublishFailed(PendingPersistOrderSnapshot snapshot, String reason, String workerId) {
        }
    }

    private static final class BlockingPendingPort implements PendingPersistOrderPort {

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile boolean released;

        @Override
        public List<PendingPersistOrderSnapshot> selectDueForShard(
                int shardTotal,
                int shardIndex,
                int limit,
                String workerId) {
            started.countDown();
            boolean interrupted = false;
            try {
                while (true) {
                    try {
                        if (release.await(100, TimeUnit.MILLISECONDS)) {
                            released = true;
                            return List.of();
                        }
                    } catch (InterruptedException ex) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void markRepublished(
                PendingPersistOrderSnapshot snapshot,
                PersistMessagePublishResult result,
                String workerId) {
        }

        @Override
        public void markRepublishFailed(PendingPersistOrderSnapshot snapshot, String reason, String workerId) {
        }

        private boolean awaitStarted() throws InterruptedException {
            return started.await(1, TimeUnit.SECONDS);
        }

        private void release() {
            release.countDown();
        }

        private boolean isReleased() {
            return released;
        }
    }

    private static final class DelayedPendingPort implements PendingPersistOrderPort {

        private final Duration delay;

        private DelayedPendingPort(Duration delay) {
            this.delay = delay;
        }

        @Override
        public List<PendingPersistOrderSnapshot> selectDueForShard(
                int shardTotal,
                int shardIndex,
                int limit,
                String workerId) {
            try {
                Thread.sleep(delay.toMillis());
                return List.of();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("delayed pending scan interrupted", ex);
            }
        }

        @Override
        public void markRepublished(
                PendingPersistOrderSnapshot snapshot,
                PersistMessagePublishResult result,
                String workerId) {
        }

        @Override
        public void markRepublishFailed(PendingPersistOrderSnapshot snapshot, String reason, String workerId) {
        }
    }

    private static final class AlwaysConfirmedPublisher implements PersistRechargeOrderMessagePublisher {

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public java.util.concurrent.CompletionStage<PersistMessagePublishResult> publish(
                PersistTradeOrderMessage message) {
            return CompletableFuture.completedFuture(PersistMessagePublishResult.confirmed(message.messageId(), 1));
        }
    }

    private static final class RecordingOutboxWorkerService extends RechargeOutboxWorkerService {

        private final RechargeOutboxBatchResult result;
        private final List<String> calls = new ArrayList<>();

        private RecordingOutboxWorkerService(RechargeOutboxBatchResult result) {
            super(
                    unusedInterface(RechargeOutboxPort.class),
                    unusedInterface(RechargeFulfillmentOrderPort.class),
                    unusedInterface(RechargeSupplierGatewayPort.class),
                    unusedInterface(DirectRechargeFrequencyPort.class),
                    command -> {
                    },
                    BalanceReservationReleasePort.noop(),
                    task -> {
                    },
                    new EmptyMerchantNotifyPayloadSigner(),
                    Clock.systemUTC(),
                    new RechargeOutboxWorkerSettings(
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(1)));
            this.result = result;
        }

        @Override
        public RechargeOutboxBatchResult processDue(int shardTotal, int shardIndex, int limit, String workerId) {
            calls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return result;
        }
    }

    private static final class RecordingNotifyWorkerService extends RechargeNotifyWorkerService {

        private final RechargeNotifyBatchResult result;
        private final List<String> calls = new ArrayList<>();

        private RecordingNotifyWorkerService(RechargeNotifyBatchResult result) {
            super(
                    unusedInterface(RechargeNotifyWorkerTaskPort.class),
                    request -> MerchantNotifySendResult.success(200, "{}"),
                    Clock.systemUTC(),
                    new RechargeNotifyWorkerSettings(Duration.ofSeconds(1), Duration.ofSeconds(1)));
            this.result = result;
        }

        @Override
        public RechargeNotifyBatchResult processDue(int limit, String workerId) {
            calls.add(limit + ":" + workerId);
            return result;
        }
    }

    private static final class RecordingBalanceDeltaFlushPort implements RechargeBalanceDeltaFlushPort {

        private final RechargeCompensationBatchResult result;
        private final List<String> calls = new ArrayList<>();

        private RecordingBalanceDeltaFlushPort(RechargeCompensationBatchResult result) {
            this.result = result;
        }

        @Override
        public RechargeCompensationBatchResult flushDue(
                int shardTotal,
                int shardIndex,
                int limit,
                String workerId) {
            calls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return result;
        }
    }

    private static final class RecordingBalanceReconcilePort implements RechargeBalanceReconcilePort {

        private final RechargeCompensationBatchResult result;
        private final List<String> calls = new ArrayList<>();

        private RecordingBalanceReconcilePort(RechargeCompensationBatchResult result) {
            this.result = result;
        }

        @Override
        public RechargeCompensationBatchResult reconcileDue(
                int shardTotal,
                int shardIndex,
                int limit,
                String workerId) {
            calls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return result;
        }
    }

    private static final class RecordingCacheRepairPort implements RechargeCacheRepairPort {

        private final RechargeCompensationBatchResult result;
        private final List<String> calls = new ArrayList<>();

        private RecordingCacheRepairPort(RechargeCompensationBatchResult result) {
            this.result = result;
        }

        @Override
        public RechargeCompensationBatchResult repairDue(
                int shardTotal,
                int shardIndex,
                int limit,
                String workerId) {
            calls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return result;
        }
    }

    private static final class EmptyMerchantNotifyPayloadSigner implements MerchantNotifyPayloadSigner {
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
    }

    @SuppressWarnings("unchecked")
    private static <T> T unusedInterface(Class<T> type) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (proxy, method, args) -> defaultValue(method.getReturnType()));
    }

    private static Object defaultValue(Class<?> returnType) {
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == void.class) {
            return null;
        }
        if (returnType == Optional.class) {
            return Optional.empty();
        }
        if (returnType == List.class) {
            return List.of();
        }
        return null;
    }

    @AutoConfiguration(before = TradeEngineCompensationJobAutoConfiguration.class)
    static class LaterRepublishDependencyAutoConfiguration {

        @Bean
        PendingPersistOrderPort pendingPersistOrderPort() {
            return PendingPersistOrderPort.noop();
        }

        @Bean
        PersistRechargeOrderMessagePublisher persistRechargeOrderMessagePublisher() {
            return new AlwaysConfirmedPublisher();
        }
    }

    static class EarlyPublisherConfiguration {

        @Bean
        PersistRechargeOrderMessagePublisher persistRechargeOrderMessagePublisher() {
            return new AlwaysConfirmedPublisher();
        }
    }

    @AutoConfiguration(before = TradeEngineCompensationJobAutoConfiguration.class)
    static class LaterRedisPendingPortAutoConfiguration {

        @Bean
        RecordingPendingPort pendingPersistOrderPort() {
            return new RecordingPendingPort(List.of(pendingSnapshot("ORDER-LATE")));
        }
    }

    @AutoConfiguration(before = TradeEngineCompensationJobAutoConfiguration.class)
    static class LaterOutboxWorkerAutoConfiguration {

        @Bean
        RechargeOutboxWorkerService rechargeOutboxWorkerService() {
            return new RecordingOutboxWorkerService(RechargeOutboxBatchResult.empty());
        }
    }

    static class ManualJobTestConfiguration {

        @Bean
        RechargePersistMqRepublishJob rechargePersistMqRepublishJob() {
            return new RechargePersistMqRepublishJob(
                    new RechargePersistMqRepublishService(
                            PendingPersistOrderPort.noop(),
                            new AlwaysConfirmedPublisher()));
        }
    }

    static class ManualJobPropertiesConfiguration {

        @Bean
        TradeEngineProperties tradeEngineProperties() {
            return new TradeEngineProperties();
        }
    }
}
