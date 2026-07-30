package com.cardshop.trade.engine.job.recharge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushService;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcileService;
import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairService;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxWorkerSettings;
import com.cardshop.trade.engine.application.recharge.PendingPersistOrderPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushPort;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcilePort;
import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairPort;
import com.cardshop.trade.engine.application.recharge.RechargePersistMqRepublishService;
import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * D11/D12 补偿任务 facade 测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统高可用方案设计.md}「17. 分片补偿与对账任务」。
 * job 模块不直接绑定 XXL-Job API，而是先提供可复用的 runOnce 入口；后续接入调度框架时只负责解析分片参数。
 */
class RechargeCompensationJobsTest {

    @Test
    void persistMqRepublishJobNormalizesShardAndLimit() {
        RecordingPersistMqRepublishService service = new RecordingPersistMqRepublishService();
        RechargePersistMqRepublishJob job = new RechargePersistMqRepublishJob(service);

        RechargeCompensationBatchResult result = job.runOnce(8, 7, 5_000, "executor-A");

        assertThat(result.scanned()).isEqualTo(1000);
        assertThat(service.calls).containsExactly("8:7:1000:executor-A");
    }

    @Test
    void usesDefaultLimitWhenLimitIsNotPositive() {
        RecordingPersistMqRepublishService service = new RecordingPersistMqRepublishService();
        RechargePersistMqRepublishJob job = new RechargePersistMqRepublishJob(service);

        RechargeCompensationBatchResult result = job.runOnce(8, 7, 0, "executor-A");

        assertThat(result.scanned()).isEqualTo(500);
        assertThat(service.calls).containsExactly("8:7:500:executor-A");
    }

    @Test
    void rejectsShardIndexOutsideShardTotalInsteadOfMergingIntoLastShard() {
        RecordingPersistMqRepublishService service = new RecordingPersistMqRepublishService();
        RechargePersistMqRepublishJob job = new RechargePersistMqRepublishJob(service);

        assertThatThrownBy(() -> job.runOnce(8, 20, 0, "executor-A"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shardIndex");
        assertThat(service.calls).isEmpty();
    }

    @Test
    void outboxPublishJobDelegatesToWorkerWithNormalizedLimit() {
        RecordingOutboxWorkerService service = new RecordingOutboxWorkerService();
        RechargeOutboxPublishJob job = new RechargeOutboxPublishJob(service);

        job.runOnce(4, -1, 10, "executor-B");

        assertThat(service.calls).containsExactly("4:0:10:executor-B");
    }

    @Test
    void balanceAndCacheJobsDelegateWithShardContext() {
        RecordingBalanceDeltaFlushService deltaService = new RecordingBalanceDeltaFlushService();
        RecordingBalanceReconcileService reconcileService = new RecordingBalanceReconcileService();
        RecordingCacheRepairService cacheRepairService = new RecordingCacheRepairService();

        new RechargeBalanceDeltaFlushJob(deltaService).runOnce(2, 1, 50, "executor-C");
        new RechargeBalanceReconcileJob(reconcileService).runOnce(2, 1, 50, "executor-C");
        new RechargeCacheRepairJob(cacheRepairService).runOnce(2, 1, 50, "executor-C");

        assertThat(deltaService.calls).containsExactly("2:1:50:executor-C");
        assertThat(reconcileService.calls).containsExactly("2:1:50:executor-C");
        assertThat(cacheRepairService.calls).containsExactly("2:1:50:executor-C");
    }

    private static final class RecordingPersistMqRepublishService extends RechargePersistMqRepublishService {

        private final List<String> calls = new ArrayList<>();

        private RecordingPersistMqRepublishService() {
            super(PendingPersistOrderPort.noop(), new NoopPersistPublisher());
        }

        @Override
        public RechargeCompensationBatchResult republishDue(int shardTotal, int shardIndex, int limit, String workerId) {
            calls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return new RechargeCompensationBatchResult(limit, 0, 0, 0);
        }
    }

    private static final class RecordingOutboxWorkerService extends RechargeOutboxWorkerService {

        private final List<String> calls = new ArrayList<>();

        private RecordingOutboxWorkerService() {
            super(
                    interfaceProxy(com.cardshop.trade.engine.application.recharge.RechargeOutboxPort.class),
                    interfaceProxy(com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort.class),
                    interfaceProxy(com.cardshop.trade.engine.application.recharge.RechargeSupplierGatewayPort.class),
                    interfaceProxy(com.cardshop.trade.engine.application.recharge.DirectRechargeFrequencyPort.class),
                    interfaceProxy(com.cardshop.trade.engine.application.recharge.BalanceDeltaFlushOutboxPort.class),
                    interfaceProxy(com.cardshop.trade.engine.application.recharge.BalanceReservationReleasePort.class),
                    interfaceProxy(com.cardshop.trade.engine.application.recharge.RechargeNotifyTaskPort.class),
                    interfaceProxy(com.cardshop.trade.engine.application.recharge.MerchantNotifyPayloadSigner.class),
                    java.time.Clock.systemUTC(),
                    new RechargeOutboxWorkerSettings(
                            Duration.ofMinutes(2),
                            Duration.ofSeconds(5),
                            Duration.ofSeconds(30),
                            Duration.ofMinutes(30),
                            Duration.ofSeconds(5)));
        }

        @Override
        public com.cardshop.trade.engine.application.recharge.RechargeOutboxBatchResult processDue(
                int shardTotal,
                int shardIndex,
                int limit,
                String workerId,
                List<com.cardshop.trade.engine.domain.order.OutboxEventType> eventTypes,
                int parallelism) {
            // 2e95a956 起 RechargeOutboxPublishJob.runOnce(4 参) 经 6 参 runOnce 调用 6 参 processDue，
            // 故在此覆盖 6 参入口以录制调用并短路真实扫描，避免接口代理返回 null 触发 NPE。
            calls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return com.cardshop.trade.engine.application.recharge.RechargeOutboxBatchResult.empty();
        }
    }

    private static final class RecordingBalanceDeltaFlushService extends RechargeBalanceDeltaFlushService {

        private final List<String> calls = new ArrayList<>();

        private RecordingBalanceDeltaFlushService() {
            super(RechargeBalanceDeltaFlushPort.noop());
        }

        @Override
        public RechargeCompensationBatchResult flushDue(int shardTotal, int shardIndex, int limit, String workerId) {
            calls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return RechargeCompensationBatchResult.empty();
        }
    }

    private static final class RecordingBalanceReconcileService extends RechargeBalanceReconcileService {

        private final List<String> calls = new ArrayList<>();

        private RecordingBalanceReconcileService() {
            super(RechargeBalanceReconcilePort.noop());
        }

        @Override
        public RechargeCompensationBatchResult reconcileDue(int shardTotal, int shardIndex, int limit, String workerId) {
            calls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return RechargeCompensationBatchResult.empty();
        }
    }

    private static final class RecordingCacheRepairService extends RechargeCacheRepairService {

        private final List<String> calls = new ArrayList<>();

        private RecordingCacheRepairService() {
            super(RechargeCacheRepairPort.noop());
        }

        @Override
        public RechargeCompensationBatchResult repairDue(int shardTotal, int shardIndex, int limit, String workerId) {
            calls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return RechargeCompensationBatchResult.empty();
        }
    }

    private static final class NoopPersistPublisher implements PersistRechargeOrderMessagePublisher {

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public java.util.concurrent.CompletionStage<PersistMessagePublishResult> publish(PersistTradeOrderMessage message) {
            return CompletableFuture.completedFuture(PersistMessagePublishResult.confirmed(message.messageId(), 1));
        }
    }

    private static <T> T interfaceProxy(Class<T> interfaceType) {
        Object proxy = Proxy.newProxyInstance(
                interfaceType.getClassLoader(),
                new Class<?>[]{interfaceType},
                (ignored, method, args) -> defaultValue(method.getReturnType()));
        return interfaceType.cast(proxy);
    }

    private static Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (boolean.class.equals(returnType)) {
            return false;
        }
        if (void.class.equals(returnType)) {
            return null;
        }
        return 0;
    }
}
