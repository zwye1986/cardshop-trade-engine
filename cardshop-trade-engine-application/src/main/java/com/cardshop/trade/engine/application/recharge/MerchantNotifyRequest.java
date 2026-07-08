package com.cardshop.trade.engine.application.recharge;

/**
 * 商户回调 HTTP 请求。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.12 回调设计」。payload 保留所有协议 key。
 *
 * @param taskId 回调任务唯一 ID，对应 {@code recharge_notify_task.task_id}
 * @param notifyUrl 商户回调地址，对应 {@code recharge_notify_task.notify_url}
 * @param payload 已完成签名的回调 JSON 字符串，对应 {@code recharge_notify_task.payload}
 */
public record MerchantNotifyRequest(
        String taskId,
        String notifyUrl,
        String payload) {
}
