package com.cardshop.trade.engine.bootstrap.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.cardshop.trade.engine.application.recharge.PendingPersistOrderPort;
import com.cardshop.trade.engine.application.recharge.PendingPersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.application.recharge.RechargePersistMqRepublishService;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
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
                .withUserConfiguration(ManualJobTestConfiguration.class, ManualRechargeJobController.class)
                .withPropertyValues(
                        "spring.profiles.active=dev",
                        "trade.engine.integration.manual-job-enabled=true")
                .run(context -> assertThat(context).hasSingleBean(ManualRechargeJobController.class));

        new ApplicationContextRunner()
                .withUserConfiguration(ManualJobTestConfiguration.class, ManualRechargeJobController.class)
                .withPropertyValues("spring.profiles.active=test")
                .run(context -> assertThat(context).doesNotHaveBean(ManualRechargeJobController.class));

        new ApplicationContextRunner()
                .withUserConfiguration(ManualJobTestConfiguration.class, ManualRechargeJobController.class)
                .withPropertyValues(
                        "spring.profiles.active=prod",
                        "trade.engine.integration.manual-job-enabled=true")
                .run(context -> assertThat(context).doesNotHaveBean(ManualRechargeJobController.class));
    }

    @Test
    void runsPersistMqRepublishJobForLoopbackRequest() {
        RecordingPendingPort pendingPort = new RecordingPendingPort(List.of(pendingSnapshot("ORDER-1")));
        ManualRechargeJobController controller = controller(pendingPort);
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 0, 10, "test188-manual-1");

        var response = controller.runPersistMqRepublishOnce(request, exchange("127.0.0.1"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("OK");
        assertThat(response.getBody().scanned()).isEqualTo(1);
        assertThat(response.getBody().succeeded()).isEqualTo(1);
        assertThat(pendingPort.selectCalls).containsExactly("1:0:10:test188-manual-1");
        assertThat(pendingPort.markedRepublished).containsExactly("ORDER-1");
    }

    @Test
    void rejectsNonLoopbackRequestBeforeJobRuns() {
        RecordingPendingPort pendingPort = new RecordingPendingPort(List.of(pendingSnapshot("ORDER-1")));
        ManualRechargeJobController controller = controller(pendingPort);
        var request = new ManualRechargeJobController.ManualJobRunRequest(1, 0, 10, "test188-manual-1");

        var response = controller.runPersistMqRepublishOnce(request, exchange("203.0.113.10"));

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

    private static ManualRechargeJobController controller(RecordingPendingPort pendingPort) {
        var service = new RechargePersistMqRepublishService(pendingPort, new AlwaysConfirmedPublisher());
        return new ManualRechargeJobController(new RechargePersistMqRepublishJob(service));
    }

    private static MockServerWebExchange exchange(String host) {
        var request = MockServerHttpRequest.post("/internal/integration/recharge/jobs/persist-mq-republish/run-once")
                .remoteAddress(new InetSocketAddress(host, 18080))
                .build();
        return MockServerWebExchange.from(request);
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

    static class ManualJobTestConfiguration {

        @Bean
        RechargePersistMqRepublishJob rechargePersistMqRepublishJob() {
            return new RechargePersistMqRepublishJob(
                    new RechargePersistMqRepublishService(
                            PendingPersistOrderPort.noop(),
                            new AlwaysConfirmedPublisher()));
        }
    }
}
