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
        try {
            MerchantNotifySendResult sendResult = notifyClientPort.send(new MerchantNotifyRequest(
                    task.taskId(),
                    task.notifyUrl(),
                    task.payload()));
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

    private static String failureMessage(MerchantNotifySendResult sendResult) {
        if (sendResult.errorMessage() != null && !sendResult.errorMessage().isBlank()) {
            return sendResult.errorMessage();
        }
        return "merchant notify failed, statusCode=" + sendResult.statusCode();
    }
}
