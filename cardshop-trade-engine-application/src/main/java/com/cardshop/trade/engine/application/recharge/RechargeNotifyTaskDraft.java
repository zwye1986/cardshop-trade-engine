package com.cardshop.trade.engine.application.recharge;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 创建商户回调任务的命令。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。payload 中空字段必须保留，
 * 并且 sign 已在创建任务前计算完成，Worker 重试时不重新解释业务字段。
 *
 * @param taskId 回调任务唯一 ID，对应 {@code recharge_notify_task.task_id}
 * @param notifyType 回调类型，对应 {@code recharge_notify_task.notify_type}
 * @param orgCode 商户编码，对应 {@code recharge_notify_task.org_code}
 * @param orderId 平台订单号，对应 {@code recharge_notify_task.order_id}
 * @param outOrderSn 商户外部订单号，对应 {@code recharge_notify_task.out_order_sn}
 * @param notifyUrl 商户回调地址，对应 {@code recharge_notify_task.notify_url}
 * @param bizStatus 本次回调业务状态，对应 {@code recharge_notify_task.biz_status}
 * @param payload 已签名回调字段集合，写库前序列化为 {@code recharge_notify_task.payload}
 * @param maxRetryCount 最大回调次数，对应 {@code recharge_notify_task.max_retry_count}
 * @param nextRetryTime 首次可推送时间，对应 {@code recharge_notify_task.next_retry_time}
 */
public record RechargeNotifyTaskDraft(
        String taskId,
        String notifyType,
        String orgCode,
        String orderId,
        String outOrderSn,
        String notifyUrl,
        String bizStatus,
        Map<String, String> payload,
        int maxRetryCount,
        LocalDateTime nextRetryTime) {
}
