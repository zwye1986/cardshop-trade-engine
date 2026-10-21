package com.cardshop.trade.engine.application.recharge;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 创建商户回调任务的命令。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。payload 中空字段必须保留，
 * 直连 sign 在创建前计算，银行保存无业务签名事实及编码文本，Worker 重试不重建正文。
 *
 * @param taskId 回调任务唯一 ID，对应 {@code recharge_notify_task.task_id}
 * @param notifyType 回调类型，对应 {@code recharge_notify_task.notify_type}
 * @param orgCode 商户编码，对应 {@code recharge_notify_task.org_code}
 * @param orderId 平台订单号，对应 {@code recharge_notify_task.order_id}
 * @param outOrderSn 商户外部订单号，对应 {@code recharge_notify_task.out_order_sn}
 * @param notifyUrl 通知目标地址，直连为商户地址、银行为可信网关，对应 {@code recharge_notify_task.notify_url}
 * @param bizStatus 本次回调业务状态，对应 {@code recharge_notify_task.biz_status}
 * @param payload 通知业务字段，直连已签名、银行无业务签名，写库前序列化为 {@code recharge_notify_task.payload}
 * @param maxRetryCount 最大回调次数，对应 {@code recharge_notify_task.max_retry_count}
 * @param nextRetryTime 首次可推送时间，对应 {@code recharge_notify_task.next_retry_time}
 * @param deliveryChannel 投递通道
 * @param routeSnapshot 通知路由快照
 * @param encodedPayload 银行通知编码正文
 */
public record RechargeNotifyTaskDraft(
        String taskId,
        String notifyType,
        String orgCode,
        String orderId,
        String outOrderSn,
        String notifyUrl,
        String bizStatus,
        Map<String, Object> payload,
        int maxRetryCount,
        LocalDateTime nextRetryTime,
        NotifyDeliveryChannel deliveryChannel,
        MerchantNotifyRoute routeSnapshot,
        String encodedPayload) {
    /** 历史入口明确保持直连；参见详细设计「交行 2104 通知实施细化」。 */
    public RechargeNotifyTaskDraft(String taskId, String notifyType, String orgCode, String orderId,
            String outOrderSn, String notifyUrl, String bizStatus, Map<String, ?> payload,
            int maxRetryCount, LocalDateTime nextRetryTime) {
        this(taskId, notifyType, orgCode, orderId, outOrderSn, notifyUrl, bizStatus, new java.util.LinkedHashMap<>(payload),
                maxRetryCount, nextRetryTime, NotifyDeliveryChannel.DIRECT_JSON, null, null);
    }
}
