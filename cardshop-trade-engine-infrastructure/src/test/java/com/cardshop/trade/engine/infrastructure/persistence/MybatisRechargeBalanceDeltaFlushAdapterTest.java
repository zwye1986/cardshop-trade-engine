package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeBalanceDeltaOutboxDO;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * 余额 delta 刷账适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」与
 * {@code docs/交行直充业务系统高可用方案设计.md}「14.7 {@code mc_ext_depart.balance} 刷账协议」。
 * 验证按 org 聚合一次 balance 增量、claim miss 跳过、失败退避序列、重试耗尽转人工。
 */
class MybatisRechargeBalanceDeltaFlushAdapterTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-08T04:30:00Z"), ZoneOffset.UTC);
    private static final List<Duration> BACKOFFS = List.of(
            Duration.ofSeconds(5),
            Duration.ofSeconds(30),
            Duration.ofSeconds(120),
            Duration.ofSeconds(600));

    @Test
    void aggregatesTwoEventsSameOrgIntoOneBalanceIncrement() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        operations.due.add(balanceEvent("balance-flush-ORDER-1", "A36", 0L, -5200L));
        operations.due.add(balanceEvent("balance-flush-ORDER-2", "A36", 0L, -800L));
        MybatisRechargeBalanceDeltaFlushAdapter adapter = newAdapter(operations);

        RechargeCompensationBatchResult result = adapter.flushDue(4, 1, 20, "worker-A");

        assertThat(result.scanned()).isEqualTo(2);
        assertThat(result.succeeded()).isEqualTo(2);
        assertThat(result.failed()).isZero();
        assertThat(result.skipped()).isZero();
        assertThat(operations.selectCalls).containsExactly("4:1:20:2026-07-08T12:30");
        assertThat(operations.balanceCalls).containsExactly("A36:-6000");
        assertThat(operations.successCalls)
                .containsExactly("balance-flush-ORDER-1:1", "balance-flush-ORDER-2:1");
    }

    @Test
    void skipsEventWhenClaimFails() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        operations.claimResult = 0;
        operations.due.add(balanceEvent("balance-flush-ORDER-1", "A36", 0L, -5200L));
        MybatisRechargeBalanceDeltaFlushAdapter adapter = newAdapter(operations);

        RechargeCompensationBatchResult result = adapter.flushDue(4, 1, 20, "worker-A");

        assertThat(result.scanned()).isEqualTo(1);
        assertThat(result.succeeded()).isZero();
        assertThat(result.failed()).isZero();
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(operations.balanceCalls).isEmpty();
        assertThat(operations.successCalls).isEmpty();
    }

    @Test
    void rollsBackOrgGroupWhenMarkSuccessCasMisses() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        operations.due.add(balanceEvent("balance-flush-ORDER-1", "A36", 0L, -100L));
        operations.due.add(balanceEvent("balance-flush-ORDER-2", "A36", 0L, -200L));
        operations.successResultSequence.add(1);
        operations.successResultSequence.add(0);
        AtomicInteger txnRuns = new AtomicInteger();
        MybatisRechargeBalanceDeltaFlushAdapter adapter = new MybatisRechargeBalanceDeltaFlushAdapter(
                operations,
                CLOCK,
                Duration.ofMinutes(2),
                BACKOFFS,
                500,
                action -> {
                    txnRuns.incrementAndGet();
                    try {
                        action.run();
                    } catch (RuntimeException ex) {
                        // 模拟事务回滚：不保留组内副作用（测试桩本身无持久状态）
                        throw ex;
                    }
                });

        RechargeCompensationBatchResult result = adapter.flushDue(1, 0, 10, "worker-A");

        assertThat(result.failed()).isEqualTo(2);
        assertThat(result.succeeded()).isZero();
        assertThat(txnRuns.get()).isEqualTo(1);
        assertThat(operations.balanceCalls).containsExactly("A36:-300");
        assertThat(operations.failedCalls).hasSize(2);
        assertThat(operations.failedCalls.get(0)).contains("balance-flush-ORDER-1:1:");
        assertThat(operations.failedCalls.get(0)).contains(":2026-07-08T12:30:05");
    }

    @Test
    void marksFailedWithBackoffSequenceIndexedByRetryCount() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        operations.incrementResult = 0;
        RechargeBalanceDeltaOutboxDO event = balanceEvent("balance-flush-ORDER-1", "A36", 3L, -5200L);
        event.setRetryCount(2);
        event.setMaxRetryCount(10);
        operations.due.add(event);
        MybatisRechargeBalanceDeltaFlushAdapter adapter = newAdapter(operations);

        RechargeCompensationBatchResult result = adapter.flushDue(4, 1, 20, "worker-A");

        assertThat(result.failed()).isEqualTo(1);
        assertThat(operations.manualRequiredCalls).isEmpty();
        // retry_count=2 → backoff[2]=120s
        assertThat(operations.failedCalls)
                .containsExactly(
                        "balance-flush-ORDER-1:4:mc_ext_depart balance row not found for orgCode=A36:2026-07-08T12:32");
    }

    @Test
    void sendsExhaustedFailureToManualRequiredWithoutSchedulingAnotherRetry() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        operations.incrementResult = 0;
        RechargeBalanceDeltaOutboxDO event = balanceEvent("balance-flush-ORDER-1", "A36", 3L, -5200L);
        event.setRetryCount(9);
        event.setMaxRetryCount(10);
        operations.due.add(event);
        MybatisRechargeBalanceDeltaFlushAdapter adapter = newAdapter(operations);

        RechargeCompensationBatchResult result = adapter.flushDue(4, 1, 20, "worker-A");

        assertThat(result.failed()).isEqualTo(1);
        assertThat(operations.manualRequiredCalls)
                .containsExactly(
                        "balance-flush-ORDER-1:4:true:mc_ext_depart balance row not found for orgCode=A36");
        assertThat(operations.failedCalls).isEmpty();
    }

    @Test
    void sendsAlreadyExhaustedFailedEventToManualRequiredWithoutClaimingAgain() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        RechargeBalanceDeltaOutboxDO event = balanceEvent("balance-flush-ORDER-1", "A36", 3L, -5200L);
        event.setStatus(TaskStatus.FAILED.name());
        event.setRetryCount(10);
        event.setMaxRetryCount(10);
        event.setLastError("payload damaged");
        operations.due.add(event);
        MybatisRechargeBalanceDeltaFlushAdapter adapter = newAdapter(operations);

        RechargeCompensationBatchResult result = adapter.flushDue(4, 1, 20, "worker-A");

        assertThat(result.failed()).isEqualTo(1);
        assertThat(operations.claimCalls).isEmpty();
        assertThat(operations.balanceCalls).isEmpty();
        assertThat(operations.manualRequiredCalls)
                .containsExactly(
                        "balance-flush-ORDER-1:3:false:balance delta flush retry exhausted: payload damaged");
    }

    @Test
    void usesConfiguredBatchSizeWhenJobLimitIsNonPositive() {
        RecordingBalanceFlushOperations operations = new RecordingBalanceFlushOperations();
        MybatisRechargeBalanceDeltaFlushAdapter adapter = new MybatisRechargeBalanceDeltaFlushAdapter(
                operations,
                CLOCK,
                Duration.ofMinutes(2),
                BACKOFFS,
                333);

        adapter.flushDue(2, 0, 0, "worker-A");

        assertThat(operations.selectCalls).containsExactly("2:0:333:2026-07-08T12:30");
    }

    private static MybatisRechargeBalanceDeltaFlushAdapter newAdapter(RecordingBalanceFlushOperations operations) {
        return new MybatisRechargeBalanceDeltaFlushAdapter(
                operations,
                CLOCK,
                Duration.ofMinutes(2),
                BACKOFFS,
                500);
    }

    private static RechargeBalanceDeltaOutboxDO balanceEvent(
            String eventId, String orgCode, long version, long deltaAmount) {
        RechargeBalanceDeltaOutboxDO outbox = new RechargeBalanceDeltaOutboxDO();
        outbox.setEventId(eventId);
        outbox.setOrgCode(orgCode);
        outbox.setOrderId("ORDER-1");
        outbox.setOutOrderSn("OUT-1");
        outbox.setDeltaType("PRE_DEDUCT");
        outbox.setDeltaAmount(deltaAmount);
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

        private final List<RechargeBalanceDeltaOutboxDO> due = new ArrayList<>();
        private final List<String> selectCalls = new ArrayList<>();
        private final List<String> claimCalls = new ArrayList<>();
        private final List<String> balanceCalls = new ArrayList<>();
        private final List<String> successCalls = new ArrayList<>();
        private final List<String> failedCalls = new ArrayList<>();
        private final List<String> manualRequiredCalls = new ArrayList<>();
        private final List<Integer> successResultSequence = new ArrayList<>();
        private int claimResult = 1;
        private int incrementResult = 1;
        private int successCallIndex;

        @Override
        public List<RechargeBalanceDeltaOutboxDO> selectDueForFlush(
                int shardTotal, int shardIndex, int limit, LocalDateTime now) {
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
            if (!successResultSequence.isEmpty()) {
                int index = Math.min(successCallIndex++, successResultSequence.size() - 1);
                return successResultSequence.get(index);
            }
            return 1;
        }

        @Override
        public int markFailed(
                String eventId,
                long currentVersion,
                String errorMessage,
                LocalDateTime nextRetryTime,
                LocalDateTime updatedAt) {
            failedCalls.add(eventId + ":" + currentVersion + ":" + errorMessage + ":" + nextRetryTime);
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
