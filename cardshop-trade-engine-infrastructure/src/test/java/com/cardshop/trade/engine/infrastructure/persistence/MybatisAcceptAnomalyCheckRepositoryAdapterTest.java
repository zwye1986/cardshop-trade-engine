package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckEvidence;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeAcceptAnomalyDO;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * 接单异常自动核查 MyBatis 仓储适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「14.3 MySQL 表字段」和
 * 「6. Redis 受理与缓存层」。该测试只验证 application 端口到 `recharge_accept_anomaly` 行的字段映射，
 * 不启动真实 MyBatis 容器。
 */
class MybatisAcceptAnomalyCheckRepositoryAdapterTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-06T08:30:00Z"), ZoneOffset.UTC);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void mapsOpenRowsToApplicationCasesAndKeepsBooleanRawContextValues() {
        RecordingAcceptAnomalyCheckStore store = new RecordingAcceptAnomalyCheckStore();
        store.openRows = List.of(row("""
                {"balance_reserved":true,"dispatch_allowed":false,"wait_reason":"WAIT_ERROR"}
                """));
        var adapter = new MybatisAcceptAnomalyCheckRepositoryAdapter(store, OBJECT_MAPPER, CLOCK);

        var cases = adapter.findOpenForCheck(10);

        assertThat(cases).hasSize(1);
        assertThat(cases.getFirst().anomalyId()).isEqualTo("accept-WAIT_CANCEL_FAILED-202607051337458961474292");
        assertThat(cases.getFirst().rawContext())
                .containsEntry("balance_reserved", true)
                .containsEntry("dispatch_allowed", false)
                .containsEntry("wait_reason", "WAIT_ERROR");
    }

    @Test
    void writesResolvedAcceptedEvidenceToCheckResultColumns() throws Exception {
        RecordingAcceptAnomalyCheckStore store = new RecordingAcceptAnomalyCheckStore();
        var adapter = new MybatisAcceptAnomalyCheckRepositoryAdapter(store, OBJECT_MAPPER, CLOCK);
        AcceptAnomalyCheckEvidence evidence = new AcceptAnomalyCheckEvidence(
                Map.of("checked", false),
                Map.of("found", true, "order_id", "202607051337458961474292"),
                Map.of("checked", false, "persist_msg_id", ""));

        adapter.markResolvedAccepted(
                "accept-WAIT_CANCEL_FAILED-202607051337458961474292",
                evidence,
                "d5b-checker");

        RechargeAcceptAnomalyDO updated = store.updatedRow;
        assertThat(updated.getStatus()).isEqualTo(AcceptAnomalyStatus.RESOLVED_ACCEPTED.name());
        assertThat(updated.getResolution()).isEqualTo("RESOLVED_ACCEPTED");
        assertThat(updated.getResolvedBy()).isEqualTo("d5b-checker");
        assertThat(updated.getResolvedAt()).isEqualTo(LocalDateTime.parse("2026-07-06T08:30:00"));
        assertThat(json(updated.getMysqlCheckResult()))
                .containsEntry("found", true)
                .containsEntry("order_id", "202607051337458961474292");
    }

    private static RechargeAcceptAnomalyDO row(String rawContext) {
        RechargeAcceptAnomalyDO row = new RechargeAcceptAnomalyDO();
        row.setAnomalyId("accept-WAIT_CANCEL_FAILED-202607051337458961474292");
        row.setAnomalyType("WAIT_CANCEL_FAILED");
        row.setOrgCode("A36");
        row.setOutOrderSn("OUT-WAIT-UNKNOWN");
        row.setOrderId("202607051337458961474292");
        row.setRequestHash("request-hash-1");
        row.setPersistMsgId("");
        row.setRawContext(rawContext);
        return row;
    }

    private static Map<String, Object> json(String value) throws Exception {
        return OBJECT_MAPPER.readValue(value, new TypeReference<>() {
        });
    }

    private static final class RecordingAcceptAnomalyCheckStore implements AcceptAnomalyCheckStore {

        private List<RechargeAcceptAnomalyDO> openRows = List.of();
        private RechargeAcceptAnomalyDO updatedRow;
        private String checkingId;

        @Override
        public List<RechargeAcceptAnomalyDO> findOpenForCheck(int limit) {
            return openRows.subList(0, Math.min(limit, openRows.size()));
        }

        @Override
        public int markChecking(String anomalyId, LocalDateTime now) {
            this.checkingId = anomalyId;
            return 1;
        }

        @Override
        public int updateCheckResult(RechargeAcceptAnomalyDO row) {
            this.updatedRow = row;
            return 1;
        }
    }
}
