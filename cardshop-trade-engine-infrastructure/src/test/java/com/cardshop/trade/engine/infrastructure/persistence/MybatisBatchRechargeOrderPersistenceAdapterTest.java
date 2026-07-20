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
 * 独立于单笔 adapter，不能依赖 MyBatis BATCH 模式下不稳定的即时 row count 做资金流水控制流。
 */
class MybatisBatchRechargeOrderPersistenceAdapterTest {

    @Test
    void writesPreDeductRecordByDatabaseIdempotencyWhenOrderInsertResultIsZero() {
        RecordingStore store = new RecordingStore();
        store.nextOrderInsertResult = 0;
        MybatisBatchRechargeOrderPersistenceAdapter adapter = new MybatisBatchRechargeOrderPersistenceAdapter(
                store,
                store,
                store,
                store,
                new ObjectMapper());

        adapter.persistBatch(List.of(aggregate("ORDER-1001", "OUT-1001")));

        assertThat(store.events).contains("order:ORDER-1001", "ext:ORDER-1001", "depart:ORDER-1001");
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
                "order:ORDER-1001",
                "order:ORDER-1002",
                "ext:ORDER-1001",
                "ext:ORDER-1002",
                "depart:ORDER-1001",
                "depart:ORDER-1002",
                "outbox:dispatch-ORDER-1001",
                "outbox:balance-flush-ORDER-1001",
                "outbox:dispatch-ORDER-1002",
                "outbox:balance-flush-ORDER-1002");
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
            implements McOrderPersistWriter,
            RechargeOrderExtPersistWriter,
            RechargeOutboxPersistWriter,
            McDepartRecordPersistWriter {

        private final List<String> events = new ArrayList<>();
        private final List<McDepartRecordDO> departRecords = new ArrayList<>();
        private final Map<String, McDepartRecordDO> departRecordsByKey = new LinkedHashMap<>();
        private final Map<String, RechargeOutboxDO> outboxesByEventId = new LinkedHashMap<>();
        private int nextOrderInsertResult = 1;

        @Override
        public int insertIfAbsent(McOrderDO order) {
            events.add("order:" + order.getOrderId());
            return nextOrderInsertResult;
        }

        @Override
        public int insertOrUpdatePersistState(RechargeOrderExtDO ext) {
            events.add("ext:" + ext.getOrderId());
            return 1;
        }

        @Override
        public int insertIfAbsent(RechargeOutboxDO outbox) {
            events.add("outbox:" + outbox.getEventId());
            outboxesByEventId.putIfAbsent(outbox.getEventId(), outbox);
            return 1;
        }

        @Override
        public int insertPreDeductIfAbsent(McDepartRecordDO record) {
            events.add("depart:" + record.getOrderId());
            String recordKey = record.getOrderId() + ":" + record.getType();
            if (departRecordsByKey.containsKey(recordKey)) {
                return 0;
            }
            departRecordsByKey.put(recordKey, record);
            departRecords.add(record);
            return 1;
        }
    }
}
