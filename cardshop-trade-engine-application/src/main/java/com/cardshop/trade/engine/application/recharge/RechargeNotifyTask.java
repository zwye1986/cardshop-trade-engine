package com.cardshop.trade.engine.application.recharge;

import java.time.LocalDateTime;

/**
 * 待推送商户回调任务。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。payload 是已经签名的
 * JSON 字符串，重试 Worker 不重新计算业务签名。
 *
 * @param taskId 回调任务唯一 ID，对应 {@code recharge_notify_task.task_id}
 * @param notifyType 回调类型，对应 {@code recharge_notify_task.notify_type}
 * @param orgCode 商户编码，对应 {@code recharge_notify_task.org_code}
 * @param orderId 平台订单号，对应 {@code recharge_notify_task.order_id}
 * @param outOrderSn 商户外部订单号，对应 {@code recharge_notify_task.out_order_sn}
 * @param notifyUrl 商户回调地址，对应 {@code recharge_notify_task.notify_url}
 * @param bizStatus 本次回调业务状态，对应 {@code recharge_notify_task.biz_status}
 * @param payload 已签名回调 JSON，对应 {@code recharge_notify_task.payload}
 * @param retryCount 已回调次数，对应 {@code recharge_notify_task.retry_count}
 * @param maxRetryCount 最大回调次数，对应 {@code recharge_notify_task.max_retry_count}
 * @param nextRetryTime 下次可重试时间，对应 {@code recharge_notify_task.next_retry_time}
 * @param lastError 最近一次失败原因，对应 {@code recharge_notify_task.last_error}
 */
public record RechargeNotifyTask(
        String taskId,
        String notifyType,
        String orgCode,
        String orderId,
        String outOrderSn,
        String notifyUrl,
        String bizStatus,
        String payload,
        int retryCount,
        int maxRetryCount,
        LocalDateTime nextRetryTime,
        String lastError) {
}
