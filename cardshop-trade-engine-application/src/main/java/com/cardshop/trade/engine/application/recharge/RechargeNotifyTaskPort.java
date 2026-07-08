package com.cardshop.trade.engine.application.recharge;

/**
 * 商户回调任务创建端口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」中
 * {@code recharge_notify_task} 的幂等约束。
 */
public interface RechargeNotifyTaskPort {

    void insertIfAbsent(RechargeNotifyTaskDraft task);
}
