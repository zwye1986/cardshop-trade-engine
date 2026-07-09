package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 余额 delta 刷账适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统高可用方案设计.md}「14.7 `mc_ext_depart.balance` 刷账协议」。
 * 测试不启动 MySQL，只验证 D12 端口的 outbox 过滤、CAS 抢占、delta 解析和成功标记契约。
 */
class MybatisRechargeBalanceDeltaFlushAdapterTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-08T04:30:00Z"), ZoneOffset.UTC);

    @Test
    void claimsBalanceFlushEventAndAppliesDeltaAmount() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        operations.due.add(balanceEvent("balance-flush-ORDER-1", 0L, -5200L));
        MybatisRechargeBalanceDeltaFlushAdapter adapter = new MybatisRechargeBalanceDeltaFlushAdapter(
                operations,
                new ObjectMapper(),
                CLOCK,
                Duration.ofMinutes(2),
                Duration.ofSeconds(30));

        RechargeCompensationBatchResult result = adapter.flushDue(4, 1, 20, "worker-A");

        assertThat(result.scanned()).isEqualTo(1);
        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(result.failed()).isZero();
        assertThat(result.skipped()).isZero();
        assertThat(operations.selectCalls).containsExactly("4:1:20:2026-07-08T12:30");
        assertThat(operations.claimCalls)
                .containsExactly("balance-flush-ORDER-1:0:worker-A:2026-07-08T12:32:2026-07-08T12:30");
        assertThat(operations.balanceCalls).containsExactly("A36:-5200");
        assertThat(operations.successCalls).containsExactly("balance-flush-ORDER-1:1");
    }

    @Test
    void skipsEventWhenClaimFails() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        operations.claimResult = 0;
        operations.due.add(balanceEvent("balance-flush-ORDER-1", 0L, -5200L));
        MybatisRechargeBalanceDeltaFlushAdapter adapter = new MybatisRechargeBalanceDeltaFlushAdapter(
                operations,
                new ObjectMapper(),
                CLOCK,
                Duration.ofMinutes(2),
                Duration.ofSeconds(30));

        RechargeCompensationBatchResult result = adapter.flushDue(4, 1, 20, "worker-A");

        assertThat(result.scanned()).isEqualTo(1);
        assertThat(result.succeeded()).isZero();
        assertThat(result.failed()).isZero();
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(operations.balanceCalls).isEmpty();
        assertThat(operations.successCalls).isEmpty();
    }

    @Test
    void sendsExhaustedFailureToManualRequiredWithoutSchedulingAnotherRetry() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        operations.incrementResult = 0;
        RechargeOutboxDO event = balanceEvent("balance-flush-ORDER-1", 3L, -5200L);
        event.setRetryCount(9);
        event.setMaxRetryCount(10);
        operations.due.add(event);
        MybatisRechargeBalanceDeltaFlushAdapter adapter = new MybatisRechargeBalanceDeltaFlushAdapter(
                operations,
                new ObjectMapper(),
                CLOCK,
                Duration.ofMinutes(2),
                Duration.ofSeconds(30));

        RechargeCompensationBatchResult result = adapter.flushDue(4, 1, 20, "worker-A");

        assertThat(result.failed()).isEqualTo(1);
        assertThat(operations.manualRequiredCalls)
                .containsExactly("balance-flush-ORDER-1:4:true:mc_ext_depart balance row not found for orgCode=A36");
        assertThat(operations.failedCalls).isEmpty();
    }

    @Test
    void sendsAlreadyExhaustedFailedEventToManualRequiredWithoutClaimingAgain() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        RechargeOutboxDO event = balanceEvent("balance-flush-ORDER-1", 3L, -5200L);
        event.setStatus(TaskStatus.FAILED.name());
        event.setRetryCount(10);
        event.setMaxRetryCount(10);
        event.setLastError("payload damaged");
        operations.due.add(event);
        MybatisRechargeBalanceDeltaFlushAdapter adapter = new MybatisRechargeBalanceDeltaFlushAdapter(
                operations,
                new ObjectMapper(),
                CLOCK,
                Duration.ofMinutes(2),
                Duration.ofSeconds(30));

        RechargeCompensationBatchResult result = adapter.flushDue(4, 1, 20, "worker-A");

        assertThat(result.failed()).isEqualTo(1);
        assertThat(operations.claimCalls).isEmpty();
        assertThat(operations.balanceCalls).isEmpty();
        assertThat(operations.manualRequiredCalls)
                .containsExactly("balance-flush-ORDER-1:3:false:balance delta flush retry exhausted: payload damaged");
    }

    @Test
    void delegatesInjectedClockToOutboxSelectionAndClaimRecovery() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        operations.due.add(balanceEvent("balance-flush-ORDER-1", 0L, -5200L));
        MybatisRechargeBalanceDeltaFlushAdapter adapter = new MybatisRechargeBalanceDeltaFlushAdapter(
                operations,
                new ObjectMapper(),
                CLOCK,
                Duration.ofMinutes(2),
                Duration.ofSeconds(30));

        adapter.flushDue(4, 1, 20, "worker-A");

        assertThat(operations.selectCalls).containsExactly("4:1:20:2026-07-08T12:30");
        assertThat(operations.claimCalls)
                .containsExactly("balance-flush-ORDER-1:0:worker-A:2026-07-08T12:32:2026-07-08T12:30");
    }

    private static RechargeOutboxDO balanceEvent(String eventId, long version, long deltaAmount) {
        RechargeOutboxDO outbox = new RechargeOutboxDO();
        outbox.setEventId(eventId);
        outbox.setEventType(OutboxEventType.BALANCE_DELTA_FLUSH.name());
        outbox.setOrgCode("A36");
        outbox.setOrderId("ORDER-1");
        outbox.setOutOrderSn("OUT-1");
        outbox.setPayload("{\"org_code\":\"A36\",\"order_id\":\"ORDER-1\",\"delta_amount\":" + deltaAmount + "}");
        outbox.setStatus(TaskStatus.INIT.name());
        outbox.setRetryCount(0);
        outbox.setMaxRetryCount(10);
        outbox.setNextRetryTime(LocalDateTime.of(2026, 7, 8, 12, 29));
        outbox.setVersion(version);
        outbox.setLastError("");
        return outbox;
    }

    private static final class RecordingBalanceFlushOperations
            implements MybatisRechargeBalanceDeltaFlushAdapter.BalanceDeltaFlushOperations {

        private final List<RechargeOutboxDO> due = new ArrayList<>();
        private final List<String> selectCalls = new ArrayList<>();
        private final List<String> claimCalls = new ArrayList<>();
        private final List<String> balanceCalls = new ArrayList<>();
        private final List<String> successCalls = new ArrayList<>();
        private final List<String> failedCalls = new ArrayList<>();
        private final List<String> manualRequiredCalls = new ArrayList<>();
        private int claimResult = 1;
        private int incrementResult = 1;

        @Override
        public List<RechargeOutboxDO> selectDueBalanceFlush(
                int shardTotal,
                int shardIndex,
                int limit,
                LocalDateTime now) {
            selectCalls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + now);
            return due;
        }

        @Override
        public int claimProcessing(
                String eventId,
                long currentVersion,
                String workerId,
                LocalDateTime lockedUntil,
                LocalDateTime now) {
            claimCalls.add(eventId + ":" + currentVersion + ":" + workerId + ":" + lockedUntil + ":" + now);
            return claimResult;
        }

        @Override
        public int incrementBalance(String orgCode, long deltaAmount) {
            balanceCalls.add(orgCode + ":" + deltaAmount);
            return incrementResult;
        }

        @Override
        public int markSuccess(String eventId, long currentVersion, LocalDateTime updatedAt) {
            successCalls.add(eventId + ":" + currentVersion);
            return 1;
        }

        @Override
        public int markFailed(
                String eventId,
                long currentVersion,
                String errorMessage,
                LocalDateTime nextRetryTime,
                LocalDateTime updatedAt) {
            failedCalls.add(eventId + ":" + currentVersion + ":" + errorMessage);
            return 1;
        }

        @Override
        public int markManualRequired(
                String eventId,
                long currentVersion,
                String errorMessage,
                LocalDateTime updatedAt,
                boolean incrementRetryCount) {
            manualRequiredCalls.add(eventId + ":" + currentVersion + ":" + incrementRetryCount + ":" + errorMessage);
            return 1;
        }
    }
}
