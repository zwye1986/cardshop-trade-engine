package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.PersistSource;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilitySnapshotProvider;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeBalanceDeltaOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeDispatchOutboxDO;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionOperations;

/**
 * outbox 异步写入 adapter 测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer · Outbox 异步落库模式」。
 * writer 必须在独立事务语义内复用批路径相同的字段映射、稳定排序和 ODKU 写法，且不能受
 * {@code recharge_order_ext} 二次确认跳过影响。
 */
class MybatisRechargePersistOutboxAsyncWriterTest {

    @Test
    void writesDispatchThenBalanceOutboxInStableBatchOrderAndRecordsMetrics() {
        var registry = new SimpleMeterRegistry();
        var metrics = new TradeEngineObservabilityMetrics(
                TradeEngineObservabilitySnapshotProvider.empty(),
                "test",
                Duration.ofMillis(30));
        metrics.bindTo(registry);
        RecordingOutboxStore store = new RecordingOutboxStore();
        RecordingTransactionOperations transactionOperations = new RecordingTransactionOperations();
        MybatisRechargePersistOutboxAsyncWriter writer = new MybatisRechargePersistOutboxAsyncWriter(
                store,
                store,
                store,
                store,
                new ObjectMapper(),
                metrics,
                Duration.ofSeconds(5),
                true,
                transactionOperations);

        writer.writeBatch(List.of(
                aggregate("ORDER-1002", "OUT-1002", true, true),
                aggregate("ORDER-1001", "OUT-1001", true, true)));

        assertThat(transactionOperations.executeCalls).isEqualTo(1);
        assertThat(store.events).containsExactly(
                "dispatchBatch:dispatch-ORDER-1001,dispatch-ORDER-1002",
                "balanceBatch:balance-flush-ORDER-1001,balance-flush-ORDER-1002");
        assertThat(registry.get("trade.engine.persist.batch.mysql.phase.duration")
                        .tag("environment", "test")
                        .tag("phase", "recharge_dispatch_outbox")
                        .timer()
                        .count())
                .isEqualTo(1L);
        assertThat(registry.get("trade.engine.persist.batch.mysql.phase.duration")
                        .tag("environment", "test")
                        .tag("phase", "recharge_balance_delta_outbox")
                        .timer()
                        .count())
                .isEqualTo(1L);
        assertThat(registry.get("trade.engine.persist.outbox.writer.duration")
                        .tag("environment", "test")
                        .timer()
                        .count())
                .isEqualTo(1L);
    }

    @Test
    void canFallbackToSingleRowWritesInStableTableOrder() {
        RecordingOutboxStore store = new RecordingOutboxStore();
        MybatisRechargePersistOutboxAsyncWriter writer = new MybatisRechargePersistOutboxAsyncWriter(
                store,
                store,
                store,
                store,
                new ObjectMapper(),
                TradeEngineObservabilityMetrics.noop(),
                Duration.ofSeconds(5),
                false,
                new RecordingTransactionOperations());

        writer.writeBatch(List.of(
                aggregate("ORDER-1002", "OUT-1002", true, true),
                aggregate("ORDER-1001", "OUT-1001", true, true)));

        assertThat(store.events).containsExactly(
                "dispatchSingle:dispatch-ORDER-1001",
                "dispatchSingle:dispatch-ORDER-1002",
                "balanceSingle:balance-flush-ORDER-1001",
                "balanceSingle:balance-flush-ORDER-1002");
    }

    @Test
    void filtersRowsByDispatchAndBalanceSnapshotFlagsOnly() {
        RecordingOutboxStore store = new RecordingOutboxStore();
        MybatisRechargePersistOutboxAsyncWriter writer = new MybatisRechargePersistOutboxAsyncWriter(
                store,
                store,
                store,
                store,
                new ObjectMapper(),
                TradeEngineObservabilityMetrics.noop(),
                Duration.ofSeconds(5),
                true,
                new RecordingTransactionOperations());

        writer.writeBatch(List.of(
                aggregate("ORDER-1001", "OUT-1001", true, true),
                aggregate("ORDER-1002", "OUT-1002", false, false)));

        assertThat(store.dispatchByEventId).containsOnlyKeys("dispatch-ORDER-1001");
        assertThat(store.balanceByEventId).containsOnlyKeys("balance-flush-ORDER-1001");
    }

    private static OrderPersistAggregate aggregate(
            String orderId,
            String outOrderSn,
            boolean dispatchAllowed,
            boolean balanceReserved) {
        return new OrderPersistAggregate(
                "persist-" + orderId + "-01",
                snapshot(orderId, outOrderSn, dispatchAllowed, balanceReserved),
                PersistSource.MQ_SNAPSHOT_FALLBACK,
                Instant.parse("2026-07-07T01:30:00Z"));
    }

    private static PersistOrderSnapshot snapshot(
            String orderId,
            String outOrderSn,
            boolean dispatchAllowed,
            boolean balanceReserved) {
        return new PersistOrderSnapshot(
                "A36",
                "app-gray",
                orderId,
                outOrderSn,
                "PARENT-100",
                "SKU-100",
                "13800138000",
                "https://merchant.example/callback",
                1,
                5200L,
                "交行话费100元",
                10000L,
                1,
                "codekey",
                "BOCOM",
                "request-hash-" + orderId,
                "v1",
                "SHA-256",
                dispatchAllowed ? "ACCEPTED" : "VALIDATION_FAILED",
                dispatchAllowed ? "PASSED" : "FAILED",
                dispatchAllowed ? "" : "MERCHANT_CONFIG_STALE",
                dispatchAllowed ? "" : "商户配置缺失或版本不一致",
                dispatchAllowed,
                balanceReserved,
                "PERSIST_QUEUED",
                1L,
                0L,
                1);
    }

    private static final class RecordingOutboxStore
            implements RechargeDispatchOutboxSinglePersistWriter,
            RechargeDispatchOutboxBatchPersistWriter,
            RechargeBalanceDeltaOutboxSinglePersistWriter,
            RechargeBalanceDeltaOutboxBatchPersistWriter {

        private final List<String> events = new ArrayList<>();
        private final Map<String, RechargeDispatchOutboxDO> dispatchByEventId = new LinkedHashMap<>();
        private final Map<String, RechargeBalanceDeltaOutboxDO> balanceByEventId = new LinkedHashMap<>();

        @Override
        public int insertDispatchOutboxIfAbsent(RechargeDispatchOutboxDO row) {
            events.add("dispatchSingle:" + row.getEventId());
            dispatchByEventId.putIfAbsent(row.getEventId(), row);
            return 1;
        }

        @Override
        public int insertDispatchOutboxBatchIfAbsent(List<RechargeDispatchOutboxDO> rows) {
            events.add("dispatchBatch:" + dispatchEventIds(rows));
            rows.forEach(row -> dispatchByEventId.putIfAbsent(row.getEventId(), row));
            return rows.size();
        }

        @Override
        public int insertBalanceDeltaIfAbsent(RechargeBalanceDeltaOutboxDO row) {
            events.add("balanceSingle:" + row.getEventId());
            balanceByEventId.putIfAbsent(row.getEventId(), row);
            return 1;
        }

        @Override
        public int insertBalanceDeltaBatchIfAbsent(List<RechargeBalanceDeltaOutboxDO> rows) {
            events.add("balanceBatch:" + balanceEventIds(rows));
            rows.forEach(row -> balanceByEventId.putIfAbsent(row.getEventId(), row));
            return rows.size();
        }

        private static String dispatchEventIds(List<RechargeDispatchOutboxDO> rows) {
            return String.join(",", rows.stream().map(RechargeDispatchOutboxDO::getEventId).toList());
        }

        private static String balanceEventIds(List<RechargeBalanceDeltaOutboxDO> rows) {
            return String.join(",", rows.stream().map(RechargeBalanceDeltaOutboxDO::getEventId).toList());
        }
    }

    private static final class RecordingTransactionOperations implements TransactionOperations {

        private int executeCalls;

        @Override
        public <T> T execute(TransactionCallback<T> action) {
            executeCalls++;
            return action.doInTransaction(null);
        }
    }
}
