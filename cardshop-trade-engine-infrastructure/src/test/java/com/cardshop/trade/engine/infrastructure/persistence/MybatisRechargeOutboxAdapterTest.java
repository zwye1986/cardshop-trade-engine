package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.BalanceDeltaFlushOutboxCommand;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxCommand;
import com.cardshop.trade.engine.application.recharge.SupplierQueryOutboxCommand;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * 作废 outbox MyBatis 适配器测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「9.2 作废设计」。DB_PERSISTED 订单作废
 * 只写 INVALID_ORDER 可靠事件，接口线程不调用供应商。
 */
class MybatisRechargeOutboxAdapterTest {

    @Test
    void enqueuesInvalidOrderOutboxWithEventIdShardKeyAndReasonPayload() {
        RecordingOutboxWriter writer = new RecordingOutboxWriter();
        MybatisRechargeOutboxAdapter adapter = new MybatisRechargeOutboxAdapter(
                writer,
                new ObjectMapper(),
                Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC));

        adapter.enqueueInvalidOrder(new InvalidRechargeOrderOutboxCommand(
                "A36", "ORDER-1", "OUT-1", "用户申请作废"));

        assertThat(writer.outbox.getEventId()).isEqualTo("invalid-ORDER-1");
        assertThat(writer.outbox.getEventType()).isEqualTo(OutboxEventType.INVALID_ORDER.name());
        assertThat(writer.outbox.getOrgCode()).isEqualTo("A36");
        assertThat(writer.outbox.getOrderId()).isEqualTo("ORDER-1");
        assertThat(writer.outbox.getOutOrderSn()).isEqualTo("OUT-1");
        assertThat(writer.outbox.getStatus()).isEqualTo(TaskStatus.INIT.name());
        assertThat(writer.outbox.getRetryCount()).isZero();
        assertThat(writer.outbox.getMaxRetryCount()).isEqualTo(10);
        assertThat(writer.outbox.getShardKey()).isEqualTo("invalid-ORDER-1");
        assertThat(writer.outbox.getNextRetryTime()).isEqualTo(LocalDateTime.of(2026, 7, 8, 11, 20));
        assertThat(writer.outbox.getPayload())
                .contains("\"reason\":\"用户申请作废\"")
                .contains("\"order_id\":\"ORDER-1\"");
    }

    @Test
    void enqueuesReleaseBalanceDeltaFlushOutboxWithPositiveAmountAndIdempotentEventId() {
        RecordingOutboxWriter writer = new RecordingOutboxWriter();
        MybatisRechargeOutboxAdapter adapter = new MybatisRechargeOutboxAdapter(
                writer,
                new ObjectMapper(),
                Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC));

        adapter.enqueueBalanceDeltaFlush(new BalanceDeltaFlushOutboxCommand(
                "A36", "ORDER-1", "OUT-1", 5200L, "RELEASE", "供应商失败释放余额"));

        assertThat(writer.outbox.getEventId()).isEqualTo("balance-flush-release-ORDER-1");
        assertThat(writer.outbox.getEventType()).isEqualTo(OutboxEventType.BALANCE_DELTA_FLUSH.name());
        assertThat(writer.outbox.getOrgCode()).isEqualTo("A36");
        assertThat(writer.outbox.getOrderId()).isEqualTo("ORDER-1");
        assertThat(writer.outbox.getOutOrderSn()).isEqualTo("OUT-1");
        assertThat(writer.outbox.getStatus()).isEqualTo(TaskStatus.INIT.name());
        assertThat(writer.outbox.getRetryCount()).isZero();
        assertThat(writer.outbox.getMaxRetryCount()).isEqualTo(10);
        assertThat(writer.outbox.getShardKey()).isEqualTo("balance-flush-release-ORDER-1");
        assertThat(writer.outbox.getNextRetryTime()).isEqualTo(LocalDateTime.of(2026, 7, 8, 11, 20));
        assertThat(writer.outbox.getPayload())
                .contains("\"delta_amount\":5200")
                .contains("\"delta_type\":\"RELEASE\"")
                .contains("\"reason\":\"供应商失败释放余额\"");
    }

    @Test
    void enqueuesSupplierQueryOutboxWithStableEventIdAndOrderIdentityPayload() {
        RecordingOutboxWriter writer = new RecordingOutboxWriter();
        MybatisRechargeOutboxAdapter adapter = new MybatisRechargeOutboxAdapter(
                writer,
                new ObjectMapper(),
                Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC));

        adapter.enqueueSupplierQuery(new SupplierQueryOutboxCommand(
                "supplier-query-ORDER-1",
                "A36",
                "ORDER-1",
                "OUT-1",
                "13800138000",
                "FNK_CGS_BX",
                "{\"selected_product_id\":\"SKU-1\"}"));

        assertThat(writer.outbox.getEventId()).isEqualTo("supplier-query-ORDER-1");
        assertThat(writer.outbox.getEventType()).isEqualTo(OutboxEventType.SUPPLIER_QUERY.name());
        assertThat(writer.outbox.getOrgCode()).isEqualTo("A36");
        assertThat(writer.outbox.getOrderId()).isEqualTo("ORDER-1");
        assertThat(writer.outbox.getOutOrderSn()).isEqualTo("OUT-1");
        assertThat(writer.outbox.getStatus()).isEqualTo(TaskStatus.INIT.name());
        assertThat(writer.outbox.getShardKey()).isEqualTo("supplier-query-ORDER-1");
        assertThat(writer.outbox.getPayload())
                .contains("\"account\":\"13800138000\"")
                .contains("\"supplier_channel\":\"FNK_CGS_BX\"")
                .contains("selected_product_id");
    }

    private static final class RecordingOutboxWriter implements RechargeOutboxPersistWriter {

        private RechargeOutboxDO outbox;

        @Override
        public int insertIfAbsent(RechargeOutboxDO outbox) {
            this.outbox = outbox;
            return 1;
        }
    }
}
