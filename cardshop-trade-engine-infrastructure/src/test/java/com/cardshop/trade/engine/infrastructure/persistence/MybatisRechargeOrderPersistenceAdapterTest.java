package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.PersistSource;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartRecordDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeBalanceDeltaOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeDispatchOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * MyBatis 异步落库适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」「9.1 派单 Outbox」
 * 「9.2 余额 Delta 刷账 Outbox」。该测试不启动数据库，只验证 D7A 聚合到拆表 DO/Mapper 的字段契约
 * 与 outbox 生成边界：派单写入 {@code recharge_dispatch_outbox}，余额刷账写入
 * {@code recharge_balance_delta_outbox}，不再写入旧混合表 {@code recharge_outbox}。
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
        assertThat(store.ext.getSupplierBatchId()).isEqualTo("100023");
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

        RechargeDispatchOutboxDO dispatchOutbox = store.dispatchByEventId("dispatch-ORDER-1001");
        assertThat(dispatchOutbox.getEventType()).isEqualTo(OutboxEventType.SUPPLIER_DISPATCH.name());
        assertThat(dispatchOutbox.getStatus()).isEqualTo(TaskStatus.INIT.name());
        assertThat(dispatchOutbox.getRetryCount()).isZero();
        assertThat(dispatchOutbox.getMaxRetryCount()).isEqualTo(3);
        assertThat(dispatchOutbox.getShardKey()).isEqualTo("dispatch-ORDER-1001");
        assertThat(dispatchOutbox.getNextRetryTime()).isEqualTo(LocalDateTime.of(2026, 7, 7, 9, 30, 1));
        assertThat(dispatchOutbox.getPayload())
                .contains("\"order_id\":\"ORDER-1001\"")
                .contains("\"supplier_channel\":\"BOCOM\"")
                .contains("\"supplier_batch_id\":\"100023\"");

        RechargeBalanceDeltaOutboxDO balanceOutbox = store.balanceByEventId("balance-flush-ORDER-1001");
        assertThat(balanceOutbox.getDeltaType()).isEqualTo("PRE_DEDUCT");
        assertThat(balanceOutbox.getDeltaAmount()).isEqualTo(-5200L);
        assertThat(balanceOutbox.getOrgCode()).isEqualTo("A36");
        assertThat(balanceOutbox.getOrderId()).isEqualTo("ORDER-1001");
        assertThat(balanceOutbox.getStatus()).isEqualTo(TaskStatus.INIT.name());
        assertThat(balanceOutbox.getRetryCount()).isZero();
        assertThat(balanceOutbox.getMaxRetryCount()).isEqualTo(10);
        assertThat(balanceOutbox.getNextRetryTime()).isEqualTo(LocalDateTime.of(2026, 7, 7, 9, 30));
    }

    @Test
    void doesNotCreateSupplierDispatchOutboxForValidationFailedOrder() {
        RecordingStore store = new RecordingStore();
        MybatisRechargeOrderPersistenceAdapter adapter = new MybatisRechargeOrderPersistenceAdapter(
                store,
                store,
                store,
                store,
                store,
                new ObjectMapper());

        adapter.persist(aggregate(snapshot("FAILED", false, "PRODUCT_NOT_ALLOWED", "商品无权限")));

        assertThat(store.order.getOrderStatus()).isEqualTo(6);
        assertThat(store.order.getFailCode()).isEqualTo("PRODUCT_NOT_ALLOWED");
        assertThat(store.order.getFailReason()).isEqualTo("商品无权限");
        assertThat(store.order.getRemark()).isEqualTo("商品无权限");
        assertThat(store.ext.getValidationStatus()).isEqualTo("FAILED");
        assertThat(store.ext.getDispatchAllowed()).isFalse();
        assertThat(store.departRecord).isNull();
        assertThat(store.dispatchOutboxes).isEmpty();
        assertThat(store.balanceOutboxes).isEmpty();
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
                store,
                new ObjectMapper());

        adapter.persist(aggregate(snapshot("PASSED", true, "", "")));

        assertThat(store.ext.getOrderId()).isEqualTo("ORDER-1001");
        assertThat(store.departRecord).isNull();
        assertThat(store.dispatchByEventId("dispatch-ORDER-1001").getEventId()).isEqualTo("dispatch-ORDER-1001");
        assertThat(store.balanceByEventId("balance-flush-ORDER-1001").getEventId()).isEqualTo("balance-flush-ORDER-1001");
    }

    @Test
    void repeatedMqConsumptionDoesNotDuplicatePreDeductRecordOrOutbox() {
        RecordingStore store = new RecordingStore();
        MybatisRechargeOrderPersistenceAdapter adapter = new MybatisRechargeOrderPersistenceAdapter(
                store,
                store,
                store,
                store,
                store,
                new ObjectMapper());

        adapter.persist(aggregate(snapshot("PASSED", true, "", "")));
        adapter.persist(aggregate(snapshot("PASSED", true, "", "")));

        // 对应详细设计「8.3 异步落库 Consumer」：重复 MQ 消费第二次命中 mc_order 唯一键后，
        // 不能重复写 PRE_DEDUCT 资金流水，也不能重复创建拆表后的派单/余额刷账事件。
        assertThat(store.orderInsertResults).containsExactly(1, 0);
        assertThat(store.extPersistCalls).isEqualTo(2);
        assertThat(store.departRecords).hasSize(1);
        assertThat(store.dispatchOutboxes)
                .extracting(RechargeDispatchOutboxDO::getEventId)
                .containsExactly("dispatch-ORDER-1001");
        assertThat(store.balanceOutboxes)
                .extracting(RechargeBalanceDeltaOutboxDO::getEventId)
                .containsExactly("balance-flush-ORDER-1001");
        assertThat(store.ext.getPersistedVersion()).isEqualTo(1L);
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
                1).withSupplierBatchId("100023");
    }

    private static final class RecordingStore
            implements McOrderPersistWriter,
            RechargeOrderExtPersistWriter,
            RechargeDispatchOutboxPersistWriter,
            RechargeBalanceDeltaOutboxPersistWriter,
            McDepartRecordPersistWriter {

        private McOrderDO order;
        private RechargeOrderExtDO ext;
        private final List<RechargeDispatchOutboxDO> dispatchOutboxes = new ArrayList<>();
        private final List<RechargeBalanceDeltaOutboxDO> balanceOutboxes = new ArrayList<>();
        private final List<McDepartRecordDO> departRecords = new ArrayList<>();
        private final List<Integer> orderInsertResults = new ArrayList<>();
        private final Map<String, McOrderDO> ordersByDirectKey = new LinkedHashMap<>();
        private final Map<String, RechargeDispatchOutboxDO> dispatchByEventId = new LinkedHashMap<>();
        private final Map<String, RechargeBalanceDeltaOutboxDO> balanceByEventId = new LinkedHashMap<>();
        private final Map<String, McDepartRecordDO> departRecordsByKey = new LinkedHashMap<>();
        private McDepartRecordDO departRecord;
        private int extPersistCalls;
        private int nextOrderInsertResult = 1;

        @Override
        public int insertIfAbsent(McOrderDO order) {
            /*
             * 测试桩按旧表生成列唯一键 uk_direct_recharge_out_order_key 模拟 MySQL 行为：
             * 首次 insert 返回 1，重复 MQ 消费再次写同一商户订单时返回 0，让适配器走“订单已存在”分支。
             */
            if (nextOrderInsertResult == 0) {
                orderInsertResults.add(0);
                return 0;
            }
            String directKey = order.getOrgCode() + ":" + order.getOutOrderId() + ":" + order.getProductType();
            if (ordersByDirectKey.containsKey(directKey)) {
                orderInsertResults.add(0);
                return 0;
            }
            this.order = order;
            ordersByDirectKey.put(directKey, order);
            orderInsertResults.add(1);
            return 1;
        }

        @Override
        public int insertOrUpdatePersistState(RechargeOrderExtDO ext) {
            extPersistCalls++;
            this.ext = ext;
            return 1;
        }

        @Override
        public int insertIfAbsent(RechargeDispatchOutboxDO outbox) {
            // recharge_dispatch_outbox 的 event_id 是派单事件幂等键；重复 MQ 消费只能保留首次事件。
            if (dispatchByEventId.containsKey(outbox.getEventId())) {
                return 0;
            }
            this.dispatchOutboxes.add(outbox);
            this.dispatchByEventId.put(outbox.getEventId(), outbox);
            return 1;
        }

        @Override
        public int insertIfAbsent(RechargeBalanceDeltaOutboxDO outbox) {
            // recharge_balance_delta_outbox 的 event_id 是余额刷账幂等键；重复 MQ 消费只能保留首次事件。
            if (balanceByEventId.containsKey(outbox.getEventId())) {
                return 0;
            }
            this.balanceOutboxes.add(outbox);
            this.balanceByEventId.put(outbox.getEventId(), outbox);
            return 1;
        }

        @Override
        public int insertPreDeductIfAbsent(McDepartRecordDO record) {
            // mc_depart_record 通过 type=1 + order_id 生成列唯一键防止重复扣款流水。
            String recordKey = record.getOrderId() + ":" + record.getType();
            if (departRecordsByKey.containsKey(recordKey)) {
                return 0;
            }
            this.departRecords.add(record);
            this.departRecordsByKey.put(recordKey, record);
            this.departRecord = record;
            return 1;
        }

        private RechargeDispatchOutboxDO dispatchByEventId(String eventId) {
            return dispatchOutboxes.stream()
                    .filter(outbox -> eventId.equals(outbox.getEventId()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("missing dispatch outbox event " + eventId));
        }

        private RechargeBalanceDeltaOutboxDO balanceByEventId(String eventId) {
            return balanceOutboxes.stream()
                    .filter(outbox -> eventId.equals(outbox.getEventId()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("missing balance delta outbox event " + eventId));
        }
    }
}
