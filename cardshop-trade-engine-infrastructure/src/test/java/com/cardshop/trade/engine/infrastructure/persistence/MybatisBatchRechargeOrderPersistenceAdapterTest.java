package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.PersistSource;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilitySnapshotProvider;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartRecordDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * MyBatis 批量异步落库适配器测试。
 *
     * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。批 adapter
     * 独立于单笔 adapter，必须调用显式 multi-row mapper，并由数据库唯一键保证重复 MQ/补投重试的幂等性。
 */
class MybatisBatchRechargeOrderPersistenceAdapterTest {

    @Test
    void writesPreDeductRecordByDatabaseIdempotencyInBatchPath() {
        RecordingStore store = new RecordingStore();
        MybatisBatchRechargeOrderPersistenceAdapter adapter = new MybatisBatchRechargeOrderPersistenceAdapter(
                store,
                store,
                store,
                store,
                new ObjectMapper());

        adapter.persistBatch(List.of(aggregate("ORDER-1001", "OUT-1001")));

        assertThat(store.events).contains(
                "orderBatch:ORDER-1001",
                "extBatch:ORDER-1001",
                "departBatch:ORDER-1001");
        assertThat(store.departRecords).hasSize(1);
        assertThat(store.departRecords.getFirst().getOrderId()).isEqualTo("ORDER-1001");
        assertThat(store.outboxesByEventId).containsKeys("dispatch-ORDER-1001", "balance-flush-ORDER-1001");
    }

    @Test
    void writesBatchInStableOrderAndGroupsStatementsByTable() {
        RecordingStore store = new RecordingStore();
        MybatisBatchRechargeOrderPersistenceAdapter adapter = new MybatisBatchRechargeOrderPersistenceAdapter(
                store,
                store,
                store,
                store,
                new ObjectMapper());

        adapter.persistBatch(List.of(
                aggregate("ORDER-1002", "OUT-1002"),
                aggregate("ORDER-1001", "OUT-1001")));

        assertThat(store.events).containsExactly(
                "orderBatch:ORDER-1001,ORDER-1002",
                "extBatch:ORDER-1001,ORDER-1002",
                "departBatch:ORDER-1001,ORDER-1002",
                "outboxBatch:dispatch-ORDER-1001,balance-flush-ORDER-1001,dispatch-ORDER-1002,balance-flush-ORDER-1002");
    }

    @Test
    void recordsMysqlDurationOnlyAfterTransactionCompletionSoCommitPhaseIsIncluded() {
        var registry = new SimpleMeterRegistry();
        var metrics = new TradeEngineObservabilityMetrics(
                TradeEngineObservabilitySnapshotProvider.empty(),
                "test",
                Duration.ofMillis(30));
        metrics.bindTo(registry);
        RecordingStore store = new RecordingStore();
        MybatisBatchRechargeOrderPersistenceAdapter adapter = new MybatisBatchRechargeOrderPersistenceAdapter(
                store,
                store,
                store,
                store,
                new ObjectMapper(),
                metrics);

        TransactionSynchronizationManager.initSynchronization();
        try {
            adapter.persistBatch(List.of(aggregate("ORDER-1003", "OUT-1003")));

            assertThat(registry.find("trade.engine.persist.batch.mysql.duration").timer()).isNull();
            assertThat(registry.get("trade.engine.persist.batch.mysql.phase.duration")
                            .tag("environment", "test")
                            .tag("phase", "mc_order")
                            .timer()
                            .count())
                    .isEqualTo(1L);
            assertThat(registry.get("trade.engine.persist.batch.mysql.phase.duration")
                            .tag("environment", "test")
                            .tag("phase", "recharge_order_ext")
                            .timer()
                            .count())
                    .isEqualTo(1L);
            assertThat(registry.find("trade.engine.persist.batch.mysql.phase.duration")
                            .tag("environment", "test")
                            .tag("phase", "commit")
                            .timer())
                    .isNull();

            List<TransactionSynchronization> synchronizations =
                    List.copyOf(TransactionSynchronizationManager.getSynchronizations());
            for (TransactionSynchronization synchronization : synchronizations) {
                synchronization.beforeCommit(false);
                synchronization.afterCompletion(TransactionSynchronization.STATUS_COMMITTED);
            }

            assertThat(registry.get("trade.engine.persist.batch.mysql.duration")
                            .tag("environment", "test")
                            .timer()
                            .count())
                    .isEqualTo(1L);
            assertThat(registry.get("trade.engine.persist.batch.mysql.phase.duration")
                            .tag("environment", "test")
                            .tag("phase", "commit")
                            .timer()
                            .count())
                    .isEqualTo(1L);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    private static OrderPersistAggregate aggregate(String orderId, String outOrderSn) {
        return new OrderPersistAggregate(
                "persist-" + orderId + "-01",
                snapshot(orderId, outOrderSn),
                PersistSource.MQ_SNAPSHOT_FALLBACK,
                Instant.parse("2026-07-07T01:30:00Z"));
    }

    private static PersistOrderSnapshot snapshot(String orderId, String outOrderSn) {
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
                "ACCEPTED",
                "PASSED",
                "",
                "",
                true,
                true,
                "PERSIST_QUEUED",
                1L,
                0L,
                1);
    }

    private static final class RecordingStore
            implements McOrderBatchPersistWriter,
            RechargeOrderExtBatchPersistWriter,
            RechargeOutboxBatchPersistWriter,
            McDepartRecordBatchPersistWriter {

        private final List<String> events = new ArrayList<>();
        private final List<McDepartRecordDO> departRecords = new ArrayList<>();
        private final Map<String, McDepartRecordDO> departRecordsByKey = new LinkedHashMap<>();
        private final Map<String, RechargeOutboxDO> outboxesByEventId = new LinkedHashMap<>();

        @Override
        public int insertOrderBatchIfAbsent(List<McOrderDO> orders) {
            events.add("orderBatch:" + orderIds(orders));
            return orders.size();
        }

        @Override
        public int insertBatchOrUpdatePersistState(List<RechargeOrderExtDO> rows) {
            events.add("extBatch:" + extOrderIds(rows));
            return rows.size();
        }

        @Override
        public int insertOutboxBatchIfAbsent(List<RechargeOutboxDO> rows) {
            events.add("outboxBatch:" + eventIds(rows));
            for (RechargeOutboxDO outbox : rows) {
                outboxesByEventId.putIfAbsent(outbox.getEventId(), outbox);
            }
            return rows.size();
        }

        @Override
        public int insertPreDeductBatchIfAbsent(List<McDepartRecordDO> rows) {
            events.add("departBatch:" + departOrderIds(rows));
            int inserted = 0;
            for (McDepartRecordDO record : rows) {
                String recordKey = record.getOrderId() + ":" + record.getType();
                if (departRecordsByKey.containsKey(recordKey)) {
                    continue;
                }
                departRecordsByKey.put(recordKey, record);
                departRecords.add(record);
                inserted++;
            }
            return inserted;
        }

        private static String orderIds(List<McOrderDO> rows) {
            return String.join(",", rows.stream().map(McOrderDO::getOrderId).toList());
        }

        private static String extOrderIds(List<RechargeOrderExtDO> rows) {
            return String.join(",", rows.stream().map(RechargeOrderExtDO::getOrderId).toList());
        }

        private static String departOrderIds(List<McDepartRecordDO> rows) {
            return String.join(",", rows.stream().map(McDepartRecordDO::getOrderId).toList());
        }

        private static String eventIds(List<RechargeOutboxDO> rows) {
            return String.join(",", rows.stream().map(RechargeOutboxDO::getEventId).toList());
        }
    }
}
