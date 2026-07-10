package com.cardshop.trade.engine.job.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyAutoCheckService;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckBatchResult;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * 接单异常自动核查任务入口测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」和
 * 「17. 分片补偿与对账任务」。job 模块先提供无调度框架依赖的任务入口，后续接入 XXL-Job 时只需要把
 * handler 参数解析后委托给该类。
 */
class AcceptAnomalyAutoCheckJobTest {

    @Test
    void delegatesLimitAndWorkerIdToApplicationService() {
        RecordingAutoCheckService service = new RecordingAutoCheckService();
        var job = new AcceptAnomalyAutoCheckJob(service);

        AcceptAnomalyCheckBatchResult result = job.runOnce(25, "shard-0");

        assertThat(result.checked()).isEqualTo(25);
        assertThat(service.lastLimit).isEqualTo(25);
        assertThat(service.lastWorkerId).isEqualTo("shard-0");
    }

    @Test
    void usesDefaultLimitWhenJobParamIsMissingOrInvalid() {
        RecordingAutoCheckService service = new RecordingAutoCheckService();
        var job = new AcceptAnomalyAutoCheckJob(service);

        AcceptAnomalyCheckBatchResult result = job.runOnce(0, "shard-1");

        assertThat(result.checked()).isEqualTo(AcceptAnomalyAutoCheckJob.DEFAULT_LIMIT);
        assertThat(service.lastLimit).isEqualTo(AcceptAnomalyAutoCheckJob.DEFAULT_LIMIT);
    }

    private static final class RecordingAutoCheckService extends AcceptAnomalyAutoCheckService {

        private int lastLimit;
        private String lastWorkerId;

        private RecordingAutoCheckService() {
            super(emptyRepository(), emptyQueryPort());
        }

        @Override
        public AcceptAnomalyCheckBatchResult checkBatch(int limit, String workerId) {
            this.lastLimit = limit;
            this.lastWorkerId = workerId;
            return new AcceptAnomalyCheckBatchResult(limit, 0, 0, 0, 0);
        }
    }

    private static AcceptAnomalyCheckRepositoryPort emptyRepository() {
        return new AcceptAnomalyCheckRepositoryPort() {
            @Override
            public List<com.cardshop.trade.engine.application.recharge.AcceptAnomalyCase> findOpenForCheck(int limit) {
                return List.of();
            }

            @Override
            public void markChecking(String anomalyId, String workerId) {
            }

            @Override
            public void markResolvedAccepted(
                    String anomalyId,
                    com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckEvidence evidence,
                    String workerId) {
            }

            @Override
            public void markResolvedNotAccepted(
                    String anomalyId,
                    com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckEvidence evidence,
                    String workerId) {
            }

            @Override
            public void markManualRequired(
                    String anomalyId,
                    com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckEvidence evidence,
                    String reason,
                    String workerId) {
            }
        };
    }

    private static RechargeOrderQueryPort emptyQueryPort() {
        return (orgCode, outOrderSn) -> Optional.empty();
    }
}
