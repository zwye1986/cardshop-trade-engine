package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * 回调重试 Worker 测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。回调失败必须回写重试
 * 时间和错误原因，成功必须标记 SUCCESS，避免重复通知商户。
 */
class RechargeNotifyWorkerServiceTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC);

    @Test
    void marksNotifyTaskSuccessWhenMerchantReturns200() {
        FakeNotifyWorkerTaskPort taskPort = new FakeNotifyWorkerTaskPort(task());
        RechargeNotifyWorkerService service = new RechargeNotifyWorkerService(
                taskPort,
                request -> MerchantNotifySendResult.success(200, "{\"code\":200}"),
                CLOCK,
                new RechargeNotifyWorkerSettings(Duration.ofMinutes(1), Duration.ofSeconds(10)));

        RechargeNotifyBatchResult result = service.processDue(10, "notify-worker");

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(taskPort.successes).containsExactly("notify-order-ORDER-1-12");
        assertThat(taskPort.failures).isEmpty();
    }

    @Test
    void schedulesRetryWhenMerchantReturnsNon200() {
        FakeNotifyWorkerTaskPort taskPort = new FakeNotifyWorkerTaskPort(task());
        RechargeNotifyWorkerService service = new RechargeNotifyWorkerService(
                taskPort,
                request -> MerchantNotifySendResult.failed(500, "server error"),
                CLOCK,
                new RechargeNotifyWorkerSettings(Duration.ofMinutes(1), Duration.ofSeconds(10)));

        RechargeNotifyBatchResult result = service.processDue(10, "notify-worker");

        assertThat(result.failed()).isEqualTo(1);
        assertThat(taskPort.successes).isEmpty();
        assertThat(taskPort.failures).singleElement().satisfies(failure -> {
            assertThat(failure.taskId()).isEqualTo("notify-order-ORDER-1-12");
            assertThat(failure.error()).contains("server error");
            assertThat(failure.nextRetryDelay()).isEqualTo(Duration.ofSeconds(10));
        });
    }

    @Test
    void marksBlankNotifyUrlTaskSuccessWithoutSendingHttp() {
        FakeNotifyWorkerTaskPort taskPort = new FakeNotifyWorkerTaskPort(taskWithNotifyUrl(""));
        List<MerchantNotifyRequest> sent = new ArrayList<>();
        RechargeNotifyWorkerService service = new RechargeNotifyWorkerService(
                taskPort,
                request -> {
                    sent.add(request);
                    return MerchantNotifySendResult.success(200, "{\"code\":200}");
                },
                CLOCK,
                new RechargeNotifyWorkerSettings(Duration.ofMinutes(1), Duration.ofSeconds(10)));

        RechargeNotifyBatchResult result = service.processDue(10, "notify-worker");

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(sent).isEmpty();
        assertThat(taskPort.successes).containsExactly("notify-order-ORDER-1-12");
        assertThat(taskPort.failures).isEmpty();
    }

    @Test
    void closesBlankNotifyUrlTasksBeforeScanningDueTasks() {
        FakeNotifyWorkerTaskPort taskPort = new FakeNotifyWorkerTaskPort(task());
        taskPort.blankClosedCount = 3;
        RechargeNotifyWorkerService service = new RechargeNotifyWorkerService(
                taskPort,
                request -> MerchantNotifySendResult.success(200, "{\"code\":200}"),
                CLOCK,
                new RechargeNotifyWorkerSettings(Duration.ofMinutes(1), Duration.ofSeconds(10)));

        RechargeNotifyBatchResult result = service.processDue(10, "notify-worker");

        assertThat(taskPort.closeBlankInvocations).isEqualTo(1);
        assertThat(result.succeeded()).isEqualTo(1);
    }

    @Test
    void exhaustedTaskDoesNotSendAgainAndMovesToManual() {
        var base = task();
        var exhausted =
                new RechargeNotifyTask(
                        base.taskId(),
                        base.notifyType(),
                        base.orgCode(),
                        base.orderId(),
                        base.outOrderSn(),
                        base.notifyUrl(),
                        base.bizStatus(),
                        base.payload(),
                        10,
                        10,
                        base.nextRetryTime(),
                        "last failure");
        var port = new FakeNotifyWorkerTaskPort(exhausted);
        var sent = new ArrayList<MerchantNotifyRequest>();
        var service =
                new RechargeNotifyWorkerService(
                        port,
                        request -> {
                            sent.add(request);
                            return MerchantNotifySendResult.success(200, "{}");
                        },
                        CLOCK,
                        new RechargeNotifyWorkerSettings(
                                Duration.ofMinutes(1), Duration.ofSeconds(10)));
        service.processDue(10, "worker");
        assertThat(sent).isEmpty();
        assertThat(port.manual).containsExactly(base.taskId());
    }

    private static RechargeNotifyTask task() {
        return taskWithNotifyUrl("https://merchant.example/notify");
    }

    private static RechargeNotifyTask taskWithNotifyUrl(String notifyUrl) {
        return new RechargeNotifyTask(
                "notify-order-ORDER-1-12",
                "ORDER_STATUS",
                "A36",
                "ORDER-1",
                "OUT-1",
                notifyUrl,
                "12",
                "{\"orderId\":\"ORDER-1\",\"sign\":\"abc\"}",
                0,
                10,
                LocalDateTime.of(2026, 7, 8, 11, 20),
                "");
    }

    private static final class FakeNotifyWorkerTaskPort implements RechargeNotifyWorkerTaskPort {

        private final RechargeNotifyTask task;
        private final List<String> successes = new ArrayList<>();
        private final List<NotifyFailure> failures = new ArrayList<>();
        private final List<String> manual = new ArrayList<>();

        public void markManualRequired(RechargeNotifyTask task, String reason) {
            manual.add(task.taskId());
        }

        private int blankClosedCount;
        private int closeBlankInvocations;

        private FakeNotifyWorkerTaskPort(RechargeNotifyTask task) {
            this.task = task;
        }

        @Override
        public List<RechargeNotifyTask> selectDueTasks(int limit) {
            return List.of(task);
        }

        @Override
        public int closeBlankNotifyUrlTasks() {
            closeBlankInvocations++;
            return blankClosedCount;
        }

        @Override
        public Optional<RechargeNotifyTask> claim(RechargeNotifyTask task, String workerId, Duration lease) {
            return Optional.of(task);
        }

        @Override
        public void markSuccess(RechargeNotifyTask task) {
            successes.add(task.taskId());
        }

        @Override
        public void markFailed(RechargeNotifyTask task, String errorMessage, Duration nextRetryDelay) {
            failures.add(new NotifyFailure(task.taskId(), errorMessage, nextRetryDelay));
        }
    }

    private record NotifyFailure(String taskId, String error, Duration nextRetryDelay) {
    }
}
