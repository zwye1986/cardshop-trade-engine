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

    /**
     * 将历史空 {@code notify_url} 的非终态任务批量标记为 SUCCESS。
     *
     * <p>空 URL 本不应写入 {@code recharge_notify_task}；本方法用于清理防御缺口或旧版本
     * 残留脏数据，避免 XXL-Job 持续扫描制造无效回调噪声。
     * 参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。
     *
     * @return 实际更新行数
     */
    int closeBlankNotifyUrlTasks();

    Optional<RechargeNotifyTask> claim(RechargeNotifyTask task, String workerId, Duration lease);

    void markSuccess(RechargeNotifyTask task);

    void markFailed(RechargeNotifyTask task, String errorMessage, Duration nextRetryDelay);
}
