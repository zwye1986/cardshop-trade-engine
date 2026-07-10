package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

/**
 * Redis 接单异常自动核查服务测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」和
 * 「6. Redis 受理与缓存层」。自动核查任务先实现 Redis/MySQL 读侧可判定的最小闭环：
 * 查到同一订单则认为已受理；查不到快照或证据冲突时先转人工，等待后续显式 Redis/MQ probe 补齐。
 */
@ExtendWith(OutputCaptureExtension.class)
class AcceptAnomalyAutoCheckServiceTest {

    @Test
    void resolvesAcceptedWhenOrderSnapshotExistsForSameOrder() {
        RecordingRepository repository = new RecordingRepository();
        repository.openCases.add(sampleCase("202607051337458961474292", ""));
        RechargeOrderQueryPort queryPort = (orgCode, outOrderSn) -> Optional.of(new RechargeOrderSnapshot(
                "202607051337458961474292",
                outOrderSn,
                "ACCEPTED",
                "PENDING_MQ",
                "PASSED",
                "",
                "",
                true));
        var service = new AcceptAnomalyAutoCheckService(repository, queryPort);

        AcceptAnomalyCheckBatchResult result = service.checkBatch(10, "d5b-checker");

        assertThat(result.checked()).isEqualTo(1);
        assertThat(result.resolvedAccepted()).isEqualTo(1);
        assertThat(repository.checkingIds).containsExactly("accept-WAIT_CANCEL_FAILED-202607051337458961474292");
        assertThat(repository.acceptedIds).containsExactly("accept-WAIT_CANCEL_FAILED-202607051337458961474292");
        assertThat(repository.lastEvidence.mysqlCheckResult())
                .containsEntry("found", true)
                .containsEntry("order_id", "202607051337458961474292")
                .containsEntry("persist_status", "PENDING_MQ");
    }

    @Test
    void marksManualRequiredWhenNoOrderSnapshotBecauseMergedQueryCannotProveRedisEmpty() {
        RecordingRepository repository = new RecordingRepository();
        repository.openCases.add(sampleCase("202607051337458961474292", ""));
        RechargeOrderQueryPort queryPort = (orgCode, outOrderSn) -> Optional.empty();
        var service = new AcceptAnomalyAutoCheckService(repository, queryPort);

        AcceptAnomalyCheckBatchResult result = service.checkBatch(10, "d5b-checker");

        assertThat(result.checked()).isEqualTo(1);
        assertThat(result.manualRequired()).isEqualTo(1);
        assertThat(repository.manualIds).containsExactly("accept-WAIT_CANCEL_FAILED-202607051337458961474292");
        assertThat(repository.lastManualReason).contains("snapshot missing");
        assertThat(repository.lastEvidence.mysqlCheckResult())
                .containsEntry("found", false)
                .containsEntry("out_order_sn", "OUT-WAIT-UNKNOWN");
    }

    @Test
    void marksManualRequiredWhenOrderSnapshotConflictsWithAnomalyOrderId() {
        RecordingRepository repository = new RecordingRepository();
        repository.openCases.add(sampleCase("202607051337458961474292", ""));
        RechargeOrderQueryPort queryPort = (orgCode, outOrderSn) -> Optional.of(new RechargeOrderSnapshot(
                "DIFFERENT-ORDER-ID",
                outOrderSn,
                "ACCEPTED",
                "PENDING_MQ",
                "PASSED",
                "",
                "",
                true));
        var service = new AcceptAnomalyAutoCheckService(repository, queryPort);

        AcceptAnomalyCheckBatchResult result = service.checkBatch(10, "d5b-checker");

        assertThat(result.checked()).isEqualTo(1);
        assertThat(result.manualRequired()).isEqualTo(1);
        assertThat(repository.manualIds).containsExactly("accept-WAIT_CANCEL_FAILED-202607051337458961474292");
        assertThat(repository.lastManualReason).contains("order_id mismatch");
    }

    @Test
    void logsErrorContextAndContinuesWhenSingleAnomalyCheckFails(CapturedOutput output) {
        RecordingRepository repository = new RecordingRepository();
        repository.openCases.add(sampleCase("202607051337458961474292", ""));
        RechargeOrderQueryPort queryPort = (orgCode, outOrderSn) -> {
            throw new IllegalStateException("query store unavailable");
        };
        var service = new AcceptAnomalyAutoCheckService(repository, queryPort);

        AcceptAnomalyCheckBatchResult result = service.checkBatch(10, "d5b-checker");

        assertThat(result.checked()).isEqualTo(1);
        assertThat(result.failed()).isEqualTo(1);
        assertThat(output)
                .contains("接单异常自动核查失败")
                .contains("anomalyId=accept-WAIT_CANCEL_FAILED-202607051337458961474292")
                .contains("orgCode=A36")
                .contains("outOrderSn=OUT-WAIT-UNKNOWN")
                .contains("query store unavailable");
    }

    private static AcceptAnomalyCase sampleCase(String orderId, String persistMsgId) {
        return new AcceptAnomalyCase(
                "accept-WAIT_CANCEL_FAILED-202607051337458961474292",
                AcceptAnomalyType.WAIT_CANCEL_FAILED,
                "A36",
                "OUT-WAIT-UNKNOWN",
                orderId,
                "request-hash-1",
                persistMsgId,
                Map.of("wait_reason", "WAIT replicas not reached"));
    }

    private static final class RecordingRepository implements AcceptAnomalyCheckRepositoryPort {

        private final List<AcceptAnomalyCase> openCases = new ArrayList<>();
        private final List<String> checkingIds = new ArrayList<>();
        private final List<String> acceptedIds = new ArrayList<>();
        private final List<String> notAcceptedIds = new ArrayList<>();
        private final List<String> manualIds = new ArrayList<>();
        private AcceptAnomalyCheckEvidence lastEvidence;
        private String lastManualReason;

        @Override
        public List<AcceptAnomalyCase> findOpenForCheck(int limit) {
            return openCases.subList(0, Math.min(limit, openCases.size()));
        }

        @Override
        public void markChecking(String anomalyId, String workerId) {
            checkingIds.add(anomalyId);
        }

        @Override
        public void markResolvedAccepted(String anomalyId, AcceptAnomalyCheckEvidence evidence, String workerId) {
            acceptedIds.add(anomalyId);
            lastEvidence = evidence;
        }

        @Override
        public void markResolvedNotAccepted(String anomalyId, AcceptAnomalyCheckEvidence evidence, String workerId) {
            notAcceptedIds.add(anomalyId);
            lastEvidence = evidence;
        }

        @Override
        public void markManualRequired(
                String anomalyId,
                AcceptAnomalyCheckEvidence evidence,
                String reason,
                String workerId) {
            manualIds.add(anomalyId);
            lastEvidence = evidence;
            lastManualReason = reason;
        }
    }
}
