package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.cardshop.trade.engine.application.recharge.BalanceDeltaFlushOutboxCommand;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeBalanceDeltaOutboxDO;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * 余额 Delta 刷账 outbox 写入适配器测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」：
 * RELEASE 使用 {@code balance-flush-release-{orderId}}；结构化 {@code delta_type}/{@code delta_amount}；
 * 无 payload / event_type / shard_key。
 */
class MybatisBalanceDeltaOutboxWriteAdapterTest {

    @Test
    void enqueuesReleaseBalanceDeltaWithPositiveAmountAndIdempotentEventId() {
        RecordingBalanceWriter writer = new RecordingBalanceWriter();
        MybatisBalanceDeltaOutboxWriteAdapter adapter = new MybatisBalanceDeltaOutboxWriteAdapter(
                writer,
                Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC));

        adapter.enqueueBalanceDeltaFlush(new BalanceDeltaFlushOutboxCommand(
                "A36", "ORDER-1", "OUT-1", 5200L, "RELEASE", "供应商失败释放余额"));

        assertThat(writer.outbox.getEventId()).isEqualTo("balance-flush-release-ORDER-1");
        assertThat(writer.outbox.getOrgCode()).isEqualTo("A36");
        assertThat(writer.outbox.getOrderId()).isEqualTo("ORDER-1");
        assertThat(writer.outbox.getOutOrderSn()).isEqualTo("OUT-1");
        assertThat(writer.outbox.getDeltaType()).isEqualTo("RELEASE");
        assertThat(writer.outbox.getDeltaAmount()).isEqualTo(5200L);
        assertThat(writer.outbox.getStatus()).isEqualTo(TaskStatus.INIT.name());
        assertThat(writer.outbox.getRetryCount()).isZero();
        assertThat(writer.outbox.getMaxRetryCount()).isEqualTo(10);
        assertThat(writer.outbox.getNextRetryTime()).isEqualTo(LocalDateTime.of(2026, 7, 8, 11, 20));
    }

    @Test
    void defaultsBlankDeltaTypeToReleaseEventId() {
        RecordingBalanceWriter writer = new RecordingBalanceWriter();
        MybatisBalanceDeltaOutboxWriteAdapter adapter = new MybatisBalanceDeltaOutboxWriteAdapter(
                writer,
                Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC));

        adapter.enqueueBalanceDeltaFlush(new BalanceDeltaFlushOutboxCommand(
                "A36", "ORDER-2", "OUT-2", 100L, "  ", "空白类型"));

        assertThat(writer.outbox.getEventId()).isEqualTo("balance-flush-release-ORDER-2");
        assertThat(writer.outbox.getDeltaType()).isEqualTo("RELEASE");
    }

    @Test
    void enqueuesPreDeductWithShortEventIdWithoutTypeSegment() {
        RecordingBalanceWriter writer = new RecordingBalanceWriter();
        MybatisBalanceDeltaOutboxWriteAdapter adapter = new MybatisBalanceDeltaOutboxWriteAdapter(
                writer,
                Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC));

        adapter.enqueueBalanceDeltaFlush(new BalanceDeltaFlushOutboxCommand(
                "A36", "ORDER-3", "OUT-3", -5200L, "PRE_DEDUCT", "落库预扣"));

        assertThat(writer.outbox.getEventId()).isEqualTo("balance-flush-ORDER-3");
        assertThat(writer.outbox.getDeltaType()).isEqualTo("PRE_DEDUCT");
        assertThat(writer.outbox.getDeltaAmount()).isEqualTo(-5200L);
    }

    @Test
    void rejectsUnsupportedDeltaType() {
        MybatisBalanceDeltaOutboxWriteAdapter adapter = new MybatisBalanceDeltaOutboxWriteAdapter(
                outbox -> 1,
                Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC));

        assertThatThrownBy(() -> adapter.enqueueBalanceDeltaFlush(new BalanceDeltaFlushOutboxCommand(
                        "A36", "ORDER-4", "OUT-4", 1L, "REFUND", "不支持")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsupported balance delta_type");
    }

    private static final class RecordingBalanceWriter implements RechargeBalanceDeltaOutboxPersistWriter {

        private RechargeBalanceDeltaOutboxDO outbox;

        @Override
        public int insertIfAbsent(RechargeBalanceDeltaOutboxDO outbox) {
            this.outbox = outbox;
            return 1;
        }
    }
}
