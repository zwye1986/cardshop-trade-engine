package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.PersistSource;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartRecordDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * MyBatis 异步落库适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。
 * 该测试不启动数据库，只验证 D7A 聚合到 DO/Mapper 的字段契约和 outbox 生成边界。
 */
class MybatisRechargeOrderPersistenceAdapterTest {

    @Test
    void writesAcceptedOrderExtSupplierDispatchAndBalanceFlushOutbox() {
        RecordingStore store = new RecordingStore();
        MybatisRechargeOrderPersistenceAdapter adapter = new MybatisRechargeOrderPersistenceAdapter(
                store,
                store,
                store,
                store,
                new ObjectMapper());

        adapter.persist(aggregate(snapshot("PASSED", true, "", "")));

        assertThat(store.order.getOrderId()).isEqualTo("ORDER-1001");
        assertThat(store.order.getOutOrderId()).isEqualTo("OUT-1001");
        assertThat(store.order.getProductCode()).isEqualTo("SKU-100");
        assertThat(store.order.getParentId()).isEqualTo("PARENT-100");
        assertThat(store.order.getProductName()).isEqualTo("交行话费100元");
        assertThat(store.order.getOrderPrice()).isEqualTo(5200L);
        assertThat(store.order.getRefundOrderPrice()).isZero();
        assertThat(store.order.getOrderStatus()).isEqualTo(2);
        assertThat(store.order.getDataStatus()).isEqualTo("PROCESSED");
        assertThat(store.order.getCallbackUrl()).isEqualTo("https://merchant.example/callback");
        assertThat(store.order.getCouponType()).isEqualTo("codekey");
        assertThat(store.order.getCreateTime()).isEqualTo(LocalDateTime.of(2026, 7, 7, 9, 30));
        assertThat(store.order.getEditTime()).isEqualTo(LocalDateTime.of(2026, 7, 7, 9, 30));

        assertThat(store.ext.getOrderId()).isEqualTo("ORDER-1001");
        assertThat(store.ext.getPersistMsgId()).isEqualTo("persist-ORDER-1001-01");
        assertThat(store.ext.getPersistStatus()).isEqualTo("DB_PERSISTED");
        assertThat(store.ext.getPersistSource()).isEqualTo(PersistSource.MQ_SNAPSHOT_FALLBACK.name());
        assertThat(store.ext.getRequestHashAlg()).isEqualTo("SHA-256");
        assertThat(store.ext.getRedisOrderVersion()).isEqualTo(1L);
        assertThat(store.ext.getPersistedVersion()).isEqualTo(1L);
        assertThat(store.ext.getDispatchAllowed()).isTrue();
        assertThat(store.ext.getBalanceReserved()).isTrue();
        assertThat(store.ext.getCreatedAt()).isEqualTo(LocalDateTime.of(2026, 7, 7, 9, 30));
        assertThat(store.ext.getUpdatedAt()).isEqualTo(LocalDateTime.of(2026, 7, 7, 9, 30));

        assertThat(store.departRecord.getOrgCode()).isEqualTo("A36");
        assertThat(store.departRecord.getOrderId()).isEqualTo("ORDER-1001");
        assertThat(store.departRecord.getType()).isEqualTo("1");
        assertThat(store.departRecord.getValue()).isEqualTo(5200L);
        assertThat(store.departRecord.getBeforeBalance()).isNull();
        assertThat(store.departRecord.getBalance()).isNull();
        assertThat(store.departRecord.getCreateTime()).isEqualTo(LocalDateTime.of(2026, 7, 7, 9, 30));

        RechargeOutboxDO dispatchOutbox = store.outboxByEventId("dispatch-ORDER-1001");
        assertThat(dispatchOutbox.getEventType()).isEqualTo(OutboxEventType.SUPPLIER_DISPATCH.name());
        assertThat(dispatchOutbox.getStatus()).isEqualTo(TaskStatus.INIT.name());
        assertThat(dispatchOutbox.getRetryCount()).isZero();
        assertThat(dispatchOutbox.getMaxRetryCount()).isEqualTo(10);
        assertThat(dispatchOutbox.getShardKey()).isEqualTo("dispatch-ORDER-1001");
        assertThat(dispatchOutbox.getNextRetryTime()).isEqualTo(LocalDateTime.of(2026, 7, 7, 9, 30));
        assertThat(dispatchOutbox.getPayload())
                .contains("\"order_id\":\"ORDER-1001\"")
                .contains("\"supplier_channel\":\"BOCOM\"");

        RechargeOutboxDO balanceOutbox = store.outboxByEventId("balance-flush-ORDER-1001");
        assertThat(balanceOutbox.getEventType()).isEqualTo(OutboxEventType.BALANCE_DELTA_FLUSH.name());
        assertThat(balanceOutbox.getStatus()).isEqualTo(TaskStatus.INIT.name());
        assertThat(balanceOutbox.getRetryCount()).isZero();
        assertThat(balanceOutbox.getMaxRetryCount()).isEqualTo(10);
        assertThat(balanceOutbox.getShardKey()).isEqualTo("balance-flush-ORDER-1001");
        assertThat(balanceOutbox.getPayload())
                .contains("\"delta_amount\":-5200")
                .contains("\"org_code\":\"A36\"")
                .contains("\"order_id\":\"ORDER-1001\"");
    }

    @Test
    void doesNotCreateSupplierDispatchOutboxForValidationFailedOrder() {
        RecordingStore store = new RecordingStore();
        MybatisRechargeOrderPersistenceAdapter adapter = new MybatisRechargeOrderPersistenceAdapter(
                store,
                store,
                store,
                store,
                new ObjectMapper());

        adapter.persist(aggregate(snapshot("FAILED", false, "PRODUCT_NOT_ALLOWED", "商品无权限")));

        assertThat(store.order.getOrderStatus()).isEqualTo(6);
        assertThat(store.order.getFailCode()).isEqualTo("PRODUCT_NOT_ALLOWED");
        assertThat(store.order.getFailReason()).isEqualTo("商品无权限");
        assertThat(store.ext.getValidationStatus()).isEqualTo("FAILED");
        assertThat(store.ext.getDispatchAllowed()).isFalse();
        assertThat(store.departRecord).isNull();
        assertThat(store.outboxes).isEmpty();
    }

    @Test
    void skipsPreDeductRecordWhenOrderAlreadyExists() {
        RecordingStore store = new RecordingStore();
        store.nextOrderInsertResult = 0;
        MybatisRechargeOrderPersistenceAdapter adapter = new MybatisRechargeOrderPersistenceAdapter(
                store,
                store,
                store,
                store,
                new ObjectMapper());

        adapter.persist(aggregate(snapshot("PASSED", true, "", "")));

        assertThat(store.ext.getOrderId()).isEqualTo("ORDER-1001");
        assertThat(store.departRecord).isNull();
        assertThat(store.outboxByEventId("dispatch-ORDER-1001").getEventId()).isEqualTo("dispatch-ORDER-1001");
        assertThat(store.outboxByEventId("balance-flush-ORDER-1001").getEventId()).isEqualTo("balance-flush-ORDER-1001");
    }

    private static OrderPersistAggregate aggregate(PersistOrderSnapshot snapshot) {
        return new OrderPersistAggregate(
                "persist-ORDER-1001-01",
                snapshot,
                PersistSource.MQ_SNAPSHOT_FALLBACK,
                Instant.parse("2026-07-07T01:30:00Z"));
    }

    private static PersistOrderSnapshot snapshot(
            String validationStatus,
            boolean dispatchAllowed,
            String failCode,
            String failReason) {
        return new PersistOrderSnapshot(
                "A36",
                "app-gray",
                "ORDER-1001",
                "OUT-1001",
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
                "request-hash-1001",
                "v1",
                "SHA-256",
                dispatchAllowed ? "ACCEPTED" : "VALIDATION_FAILED",
                validationStatus,
                failCode,
                failReason,
                dispatchAllowed,
                dispatchAllowed,
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

        private McOrderDO order;
        private RechargeOrderExtDO ext;
        private final List<RechargeOutboxDO> outboxes = new ArrayList<>();
        private McDepartRecordDO departRecord;
        private int nextOrderInsertResult = 1;

        @Override
        public int insertIfAbsent(McOrderDO order) {
            this.order = order;
            return nextOrderInsertResult;
        }

        @Override
        public int insertOrUpdatePersistState(RechargeOrderExtDO ext) {
            this.ext = ext;
            return 1;
        }

        @Override
        public int insertIfAbsent(RechargeOutboxDO outbox) {
            this.outboxes.add(outbox);
            return 1;
        }

        @Override
        public int insertPreDeductIfAbsent(McDepartRecordDO record) {
            this.departRecord = record;
            return 1;
        }

        private RechargeOutboxDO outboxByEventId(String eventId) {
            return outboxes.stream()
                    .filter(outbox -> eventId.equals(outbox.getEventId()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("missing outbox event " + eventId));
        }
    }
}
