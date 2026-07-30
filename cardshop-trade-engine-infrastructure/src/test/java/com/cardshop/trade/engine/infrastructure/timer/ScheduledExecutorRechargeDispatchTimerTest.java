package com.cardshop.trade.engine.infrastructure.timer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * 供应商履约 timer 测试。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。timer 只负责按 eventId 热唤醒，
 * 真正多节点互斥仍依赖 outbox CAS。
 */
class ScheduledExecutorRechargeDispatchTimerTest {

    @Test
    void scheduleReplacesOlderFutureForSameEventAndDelegatesToWorker() throws Exception {
        CountDownLatch executed = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        List<String> workerIds = new ArrayList<>();

        try (ScheduledExecutorRechargeDispatchTimer timer = new ScheduledExecutorRechargeDispatchTimer(
                (eventId, eventType, workerId) -> {
                    calls.incrementAndGet();
                    workerIds.add(workerId);
                    executed.countDown();
                },
                "timer-node-A",
                1)) {
            timer.schedule("dispatch-ORDER-1", OutboxEventType.SUPPLIER_DISPATCH, Duration.ofSeconds(5));
            timer.schedule("dispatch-ORDER-1", OutboxEventType.SUPPLIER_DISPATCH, Duration.ofMillis(10));

            assertThat(executed.await(2, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(80);

            assertThat(calls).hasValue(1);
            assertThat(workerIds).containsExactly("timer-node-A");
            assertThat(timer.pendingCount()).isZero();
        }
    }

    @Test
    void rejectsNewEventWhenPendingLimitReachedButAllowsReplacingSameEvent() {
        try (ScheduledExecutorRechargeDispatchTimer timer = new ScheduledExecutorRechargeDispatchTimer(
                (eventId, eventType, workerId) -> {
                },
                "timer-node-A",
                1,
                1)) {
            timer.schedule("dispatch-ORDER-1", OutboxEventType.SUPPLIER_DISPATCH, Duration.ofSeconds(60));

            timer.schedule("dispatch-ORDER-1", OutboxEventType.SUPPLIER_DISPATCH, Duration.ofSeconds(30));

            assertThat(timer.pendingCount()).isEqualTo(1);
            assertThatThrownBy(() -> timer.schedule(
                    "dispatch-ORDER-2",
                    OutboxEventType.SUPPLIER_DISPATCH,
                    Duration.ofSeconds(30)))
                    .isInstanceOf(RejectedExecutionException.class)
                    .hasMessageContaining("max pending");
            assertThat(timer.pendingCount()).isEqualTo(1);
            assertThat(timer.rejectedScheduleCount()).isEqualTo(1);
        }
    }

    @Test
    void laterWakeupsStillLeaveTheSchedulerWhenEarlierWorkerBlocks() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondFinished = new CountDownLatch(1);
        List<String> workerEvents = new CopyOnWriteArrayList<>();

        try (ScheduledExecutorRechargeDispatchTimer timer = new ScheduledExecutorRechargeDispatchTimer(
                (eventId, eventType, workerId) -> {
                    workerEvents.add(eventId);
                    if ("dispatch-ORDER-1".equals(eventId)) {
                        firstStarted.countDown();
                        try {
                            releaseFirst.await(2, TimeUnit.SECONDS);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("test worker interrupted", ex);
                        }
                    } else if ("dispatch-ORDER-2".equals(eventId)) {
                        secondFinished.countDown();
                    }
                },
                "timer-node-A",
                1,
                10)) {
            timer.schedule("dispatch-ORDER-1", OutboxEventType.SUPPLIER_DISPATCH, Duration.ZERO);
            assertThat(firstStarted.await(2, TimeUnit.SECONDS)).isTrue();

            timer.schedule("dispatch-ORDER-2", OutboxEventType.SUPPLIER_DISPATCH, Duration.ofMillis(10));

            Thread.sleep(150);
            assertThat(timer.pendingCount()).isZero();
            assertThat(secondFinished.getCount()).isEqualTo(1);

            releaseFirst.countDown();
            assertThat(secondFinished.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(workerEvents).containsExactly("dispatch-ORDER-1", "dispatch-ORDER-2");
        }
    }
}
