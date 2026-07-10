package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecord;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeAcceptAnomalyDO;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Redis 受理异常登记 MyBatis 适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * WAIT 失败且 cancel 失败时，接口不能确认 Redis 是否最终保留订单，因此必须把最小核查上下文写入
 * `recharge_accept_anomaly`，供后续自动核查任务或人工处理使用。
 */
class MybatisAcceptAnomalyRecordAdapterTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-06T08:30:00Z"), ZoneOffset.UTC);

    @Test
    void mapsApplicationRecordToRechargeAcceptAnomalyRow() throws Exception {
        RecordingAcceptAnomalyWriter writer = new RecordingAcceptAnomalyWriter();
        MybatisAcceptAnomalyRecordAdapter adapter = new MybatisAcceptAnomalyRecordAdapter(
                writer,
                new ObjectMapper(),
                CLOCK);

        adapter.record(new AcceptAnomalyRecord(
                "accept-WAIT_CANCEL_FAILED-202607051337458961474292",
                AcceptAnomalyType.WAIT_CANCEL_FAILED,
                "A36",
                "OUT-WAIT-UNKNOWN",
                "202607051337458961474292",
                "request-hash-1",
                "",
                Map.of(
                        "wait_reason", "WAIT replicas not reached",
                        "cancel_fail_code", "CANCEL_FAILED",
                        "cancel_fail_reason", "订单状态不允许回滚"),
                "订单状态不允许回滚"));

        RechargeAcceptAnomalyDO row = writer.lastRow;
        assertThat(row).isNotNull();
        assertThat(row.getAnomalyId()).isEqualTo("accept-WAIT_CANCEL_FAILED-202607051337458961474292");
        assertThat(row.getOrgCode()).isEqualTo("A36");
        assertThat(row.getOutOrderSn()).isEqualTo("OUT-WAIT-UNKNOWN");
        assertThat(row.getOrderId()).isEqualTo("202607051337458961474292");
        assertThat(row.getRequestHash()).isEqualTo("request-hash-1");
        assertThat(row.getAnomalyType()).isEqualTo("WAIT_CANCEL_FAILED");
        assertThat(row.getStatus()).isEqualTo("INIT");
        assertThat(row.getLastError()).isEqualTo("订单状态不允许回滚");
        assertThat(row.getCreatedAt()).isEqualTo(LocalDateTime.parse("2026-07-06T08:30:00"));
        assertThat(row.getUpdatedAt()).isEqualTo(LocalDateTime.parse("2026-07-06T08:30:00"));
        Map<String, Object> rawContext = new ObjectMapper().readValue(
                row.getRawContext(),
                new TypeReference<>() {
                });
        assertThat(rawContext)
                .containsEntry("wait_reason", "WAIT replicas not reached")
                .containsEntry("cancel_fail_code", "CANCEL_FAILED")
                .containsEntry("cancel_fail_reason", "订单状态不允许回滚");
    }

    private static final class RecordingAcceptAnomalyWriter implements AcceptAnomalyWriter {

        private RechargeAcceptAnomalyDO lastRow;

        @Override
        public int insertOrRefresh(RechargeAcceptAnomalyDO row) {
            this.lastRow = row;
            return 1;
        }
    }
}
