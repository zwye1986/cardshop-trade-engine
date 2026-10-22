package com.cardshop.trade.engine.application.recharge;

/**
 * 商户回调 HTTP 请求。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。payload 保留所有协议 key。
 *
 * @param taskId 回调任务唯一 ID，对应 {@code recharge_notify_task.task_id}
 * @param notifyUrl 冻结通知目标（直连商户URL或可信银行网关）
 * @param payload 冻结业务JSON，银行无内层签名，直连已签名
 */
public record MerchantNotifyRequest(
        String taskId,
        String notifyUrl,
        String payload, String orgCode, String orderId, String outOrderSn,
        NotifyDeliveryChannel deliveryChannel, String routeSnapshot, String encodedPayload,
        long claimVersion, String lockedBy, java.time.LocalDateTime lockedUntil,
        String notifyType,String bizStatus) {
    public MerchantNotifyRequest(String taskId,String notifyUrl,String payload,String orgCode,String orderId,String outOrderSn,
            NotifyDeliveryChannel deliveryChannel,String routeSnapshot,String encodedPayload,long claimVersion,String lockedBy,
            java.time.LocalDateTime lockedUntil) {
        this(taskId,notifyUrl,payload,orgCode,orderId,outOrderSn,deliveryChannel,routeSnapshot,encodedPayload,claimVersion,
                lockedBy,lockedUntil,null,null);
    }
    public MerchantNotifyRequest(String taskId,String notifyUrl,String payload,String orgCode,String orderId,String outOrderSn) {
        this(taskId,notifyUrl,payload,orgCode,orderId,outOrderSn,NotifyDeliveryChannel.DIRECT_JSON,null,null,0,null,null);
    }
    public MerchantNotifyRequest(String taskId,String notifyUrl,String payload) {
        this(taskId,notifyUrl,payload,null,null,null);
    }
    /** 发送沿用已领取的冻结任务，不查询当前商户路由；参见详细设计2104实施细化。 */
    public static MerchantNotifyRequest from(RechargeNotifyTask task) {
        return new MerchantNotifyRequest(task.taskId(),task.notifyUrl(),task.payload(),task.orgCode(),task.orderId(),task.outOrderSn(),
                task.deliveryChannel(),task.routeSnapshot(),task.encodedPayload(),task.claimVersion(),task.lockedBy(),task.lockedUntil(),
                task.notifyType(),task.bizStatus());
    }
}
