package com.cardshop.trade.engine.bootstrap.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyAutoCheckService;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCase;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckEvidence;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.CancelUnconfirmedAcceptResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistQueuedResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistStateResult;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshotPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.recharge.ReplicationWaitResult;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.job.recharge.AcceptAnomalyAutoCheckJob;
import java.time.Clock;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * 交易可靠性补偿服务装配测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.3 Redis HA Guard」。只有异常核查仓储和订单查询端口
 * 同时存在时，才装配自动核查服务。
 */
class TradeEngineReliabilityConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TradeEngineReliabilityConfiguration.class);

    @Test
    void registersAcceptAnomalyAutoCheckServiceWhenPortsArePresent() {
        contextRunner
                .withBean(AcceptAnomalyCheckRepositoryPort.class, StubRepository::new)
                .withBean(RechargeOrderQueryPort.class, () -> (orgCode, outOrderSn) -> java.util.Optional.empty())
                .run(context -> {
                    assertThat(context).hasSingleBean(AcceptAnomalyAutoCheckService.class);
                    assertThat(context).hasSingleBean(AcceptAnomalyAutoCheckJob.class);
                });
    }

    @Test
    void doesNotRegisterAcceptAnomalyAutoCheckServiceWhenRepositoryIsMissing() {
        contextRunner
                .withBean(RechargeOrderQueryPort.class, () -> (orgCode, outOrderSn) -> java.util.Optional.empty())
                .run(context -> assertThat(context).doesNotHaveBean(AcceptAnomalyAutoCheckService.class));
    }

    @Test
    void registersPersistRechargeOrderConsumerServiceWhenPortsArePresent() {
        contextRunner
                .withBean(PersistOrderSnapshotPort.class, PersistOrderSnapshotPort::noop)
                .withBean(RechargeOrderPersistencePort.class, () -> aggregate -> {
                })
                .withBean(RechargeAcceptReliabilityPort.class, StubReliabilityPort::new)
                .withBean(Clock.class, Clock::systemUTC)
                .run(context -> assertThat(context).hasSingleBean(PersistRechargeOrderConsumerService.class));
    }

    private static final class StubRepository implements AcceptAnomalyCheckRepositoryPort {

        @Override
        public List<AcceptAnomalyCase> findOpenForCheck(int limit) {
            return List.of();
        }

        @Override
        public void markChecking(String anomalyId, String workerId) {
        }

        @Override
        public void markResolvedAccepted(String anomalyId, AcceptAnomalyCheckEvidence evidence, String workerId) {
        }

        @Override
        public void markResolvedNotAccepted(String anomalyId, AcceptAnomalyCheckEvidence evidence, String workerId) {
        }

        @Override
        public void markManualRequired(
                String anomalyId,
                AcceptAnomalyCheckEvidence evidence,
                String reason,
                String workerId) {
        }
    }

    private static final class StubReliabilityPort implements RechargeAcceptReliabilityPort {

        @Override
        public java.util.concurrent.CompletionStage<ReplicationWaitResult> waitForReplication(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.CompletionStage<CancelUnconfirmedAcceptResult> cancelUnconfirmedAccept(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String reason) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.CompletionStage<MarkPersistQueuedResult> markPersistQueued(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String persistMessageId,
                int publishCount) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.CompletionStage<MarkPersistStateResult> markDbPersisted(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                long nowEpochMillis) {
            return java.util.concurrent.CompletableFuture.completedFuture(MarkPersistStateResult.success());
        }

        @Override
        public java.util.concurrent.CompletionStage<MarkPersistStateResult> markPersistFailed(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String failCode,
                String failReason,
                int retryCount,
                long nextRetryEpochMillis,
                long nowEpochMillis) {
            return java.util.concurrent.CompletableFuture.completedFuture(MarkPersistStateResult.success());
        }
    }
}
