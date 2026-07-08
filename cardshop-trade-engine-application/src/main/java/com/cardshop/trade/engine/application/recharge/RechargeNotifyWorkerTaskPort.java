package com.cardshop.trade.engine.application.recharge;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * 回调重试任务抢占端口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」。实现必须通过状态和租约
 * 做 CAS 抢占，避免多节点重复推送商户回调。
 */
public interface RechargeNotifyWorkerTaskPort {

    List<RechargeNotifyTask> selectDueTasks(int limit);

    Optional<RechargeNotifyTask> claim(RechargeNotifyTask task, String workerId, Duration lease);

    void markSuccess(RechargeNotifyTask task);

    void markFailed(RechargeNotifyTask task, String errorMessage, Duration nextRetryDelay);
}
