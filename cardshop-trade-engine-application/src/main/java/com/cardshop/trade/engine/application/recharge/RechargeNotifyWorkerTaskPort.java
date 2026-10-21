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
     * 仅将历史直连空 {@code notify_url} 的非终态任务批量标记为 SUCCESS，银行任务不得被清理。
     *
     * <p>空 URL 本不应写入 {@code recharge_notify_task}；本方法用于清理防御缺口或旧版本
     * 残留脏数据，避免 XXL-Job 持续扫描制造无效回调噪声。
     * 参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。
     *
     * @return 实际更新行数
     */
    int closeBlankNotifyUrlTasks();

    Optional<RechargeNotifyTask> claim(RechargeNotifyTask task, String workerId, Duration lease);

    /** 次数耗尽后转人工并清理租约；参见详细设计9.4、9.5。 */
    boolean markManualRequired(RechargeNotifyTask task, String reason);

    boolean markSuccess(RechargeNotifyTask task);

    boolean markFailed(RechargeNotifyTask task, String errorMessage, Duration nextRetryDelay);

    /** 配置暂不可用时保留任务，不消耗物理发送重试次数；参见详细设计2104实施细化。 */
    boolean defer(RechargeNotifyTask task, String reason, Duration delay);
}
