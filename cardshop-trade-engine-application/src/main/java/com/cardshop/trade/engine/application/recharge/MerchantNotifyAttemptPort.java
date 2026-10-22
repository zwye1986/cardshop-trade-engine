package com.cardshop.trade.engine.application.recharge;

/**
 * 银行物理请求审计，登记成功才允许发送；参见详细设计「交行 2104 通知实施细化」。
 * 配置等待不登记，完成记录允许迟到，但任务状态必须另以租约CAS隔离。
 */
public interface MerchantNotifyAttemptPort {
    boolean begin(MerchantNotifyRequest request, String msgId);
    void finish(MerchantNotifyRequest request, String msgId, MerchantNotifyAttemptResult result);
}
