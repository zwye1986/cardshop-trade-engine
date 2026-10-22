package com.cardshop.trade.engine.application.recharge;

import java.time.LocalDateTime;

/**
 * 待推送商户回调任务。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「交行 2104 通知实施细化」。payload 为
 * 冻结业务JSON，直连已签名、银行不含业务签名；重试不重建正文或路由。
 *
 * @param taskId 回调任务唯一 ID，对应 {@code recharge_notify_task.task_id}
 * @param notifyType 回调类型，对应 {@code recharge_notify_task.notify_type}
 * @param orgCode 商户编码，对应 {@code recharge_notify_task.org_code}
 * @param orderId 平台订单号，对应 {@code recharge_notify_task.order_id}
 * @param outOrderSn 商户外部订单号，对应 {@code recharge_notify_task.out_order_sn}
 * @param notifyUrl 通知目标地址，直连为商户地址、银行为可信网关
 * @param bizStatus 本次回调业务状态，对应 {@code recharge_notify_task.biz_status}
 * @param payload 冻结业务JSON，对应 {@code recharge_notify_task.payload}
 * @param retryCount 已回调次数，对应 {@code recharge_notify_task.retry_count}
 * @param maxRetryCount 最大回调次数，对应 {@code recharge_notify_task.max_retry_count}
 * @param nextRetryTime 下次可重试时间，对应 {@code recharge_notify_task.next_retry_time}
 * @param lastError 最近一次失败原因，对应 {@code recharge_notify_task.last_error}
 * @param deliveryChannel 投递通道
 * @param routeSnapshot 通知路由快照
 * @param encodedPayload 银行通知编码正文
 * @param claimVersion 领取租约代次
 * @param lockedBy 锁持有者
 * @param lockedUntil 锁过期时间
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
        String lastError,
        NotifyDeliveryChannel deliveryChannel,
        String routeSnapshot,
        String encodedPayload,
        long claimVersion,
        String lockedBy,
        LocalDateTime lockedUntil) {
    /** 旧构造入口明确保留直连通道；实际领取后必须携带新版租约凭证。 */
    public RechargeNotifyTask(String taskId,String notifyType,String orgCode,String orderId,String outOrderSn,
            String notifyUrl,String bizStatus,String payload,int retryCount,int maxRetryCount,
            LocalDateTime nextRetryTime,String lastError) {
        this(taskId,notifyType,orgCode,orderId,outOrderSn,notifyUrl,bizStatus,payload,retryCount,maxRetryCount,
                nextRetryTime,lastError,NotifyDeliveryChannel.DIRECT_JSON,null,null,0,null,null);
    }
    /** 领取使用新代次快照，不能复用扫描时的无租约对象。参见详细设计2104实施细化。 */
    public RechargeNotifyTask claimed(long version, String owner, LocalDateTime until) {
        return new RechargeNotifyTask(taskId,notifyType,orgCode,orderId,outOrderSn,notifyUrl,bizStatus,payload,
                retryCount,maxRetryCount,nextRetryTime,lastError,deliveryChannel,routeSnapshot,encodedPayload,version,owner,until);
    }
}
