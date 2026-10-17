package com.cardshop.trade.engine.application.recharge;

import java.time.Clock;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 商户回调重试 Worker。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。本服务只处理
 * {@code recharge_notify_task} 的抢占、发送和重试状态回写，不重新生成回调 payload。
 */
public class RechargeNotifyWorkerService {

    private static final Logger log = LoggerFactory.getLogger(RechargeNotifyWorkerService.class);

    private final RechargeNotifyWorkerTaskPort taskPort;
    private final MerchantNotifyClientPort notifyClientPort;
    private final Clock clock;
    private final RechargeNotifyWorkerSettings settings;

    public RechargeNotifyWorkerService(
            RechargeNotifyWorkerTaskPort taskPort,
            MerchantNotifyClientPort notifyClientPort,
            Clock clock,
            RechargeNotifyWorkerSettings settings) {
        this.taskPort = Objects.requireNonNull(taskPort, "taskPort must not be null");
        this.notifyClientPort = Objects.requireNonNull(notifyClientPort, "notifyClientPort must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
    }

    public RechargeNotifyBatchResult processDue(int limit, String workerId) {
        /*
         * 先批量终态历史空 notify_url 任务，再扫描到期任务。
         * 参见 docs/交行直充业务系统详细设计.md「9.3 回调设计」与 scripts/cleanup-blank-notify-tasks.sql。
         */
        int closedBlank = taskPort.closeBlankNotifyUrlTasks();
        if (closedBlank > 0) {
            log.warn("已批量清理空notify_url历史回调任务 closedCount={}", closedBlank);
        }
        var tasks = taskPort.selectDueTasks(limit);
        RechargeNotifyBatchResult result = RechargeNotifyBatchResult.empty().scanned(tasks.size());
        for (RechargeNotifyTask task : tasks) {
            var claimed = taskPort.claim(task, workerId, settings.claimLease());
            if (claimed.isEmpty()) {
                result = result.skippedOne();
                continue;
            }
            result = result.claimedOne();
            result = handle(claimed.get()) ? result.succeededOne() : result.failedOne();
        }
        return result;
    }

    private boolean handle(RechargeNotifyTask task) {
        /*
         * 订单 notify_url 为空时本不应创建回调任务；若历史脏数据或防御缺口写入了空 URL 任务，
         * 直接终态成功并跳过 HTTP，避免 URI.create("") 异常和对空地址的无效重试。
         * 参见 docs/交行直充业务系统详细设计.md「9.3 回调设计」。
         */
        if (!hasNotifyUrl(task)) {
            log.warn("商户回调任务notify_url为空，跳过发送并标记成功 taskId={}, orderId={}, notifyType={}",
                    task.taskId(), task.orderId(), task.notifyType());
            taskPort.markSuccess(task);
            return true;
        }
        // 次数达到上限后不再发HTTP，避免持久任务变成无限重试；参见详细设计9.5。
        if (task.retryCount() >= task.maxRetryCount()) {
            taskPort.markManualRequired(task, "RETRY_EXHAUSTED");
            return false;
        }
        try {
            MerchantNotifySendResult sendResult = notifyClientPort.send(new MerchantNotifyRequest(
                    task.taskId(),
                    task.notifyUrl(),
                    task.payload(), task.orgCode(), task.orderId(), task.outOrderSn()));
            if (sendResult.success()) {
                taskPort.markSuccess(task);
                return true;
            }
            taskPort.markFailed(task, failureMessage(sendResult), settings.retryBackoff());
            return false;
        } catch (Exception ex) {
            log.error("商户回调发送异常 taskId={}, orderId={}, notifyType={}",
                    task.taskId(), task.orderId(), task.notifyType(), ex);
            taskPort.markFailed(task, ex.getMessage(), settings.retryBackoff());
            return false;
        }
    }

    private static boolean hasNotifyUrl(RechargeNotifyTask task) {
        return task.notifyUrl() != null && !task.notifyUrl().isBlank();
    }

    private static String failureMessage(MerchantNotifySendResult sendResult) {
        if (sendResult.errorMessage() != null && !sendResult.errorMessage().isBlank()) {
            return sendResult.errorMessage();
        }
        return "merchant notify failed, statusCode=" + sendResult.statusCode();
    }
}
