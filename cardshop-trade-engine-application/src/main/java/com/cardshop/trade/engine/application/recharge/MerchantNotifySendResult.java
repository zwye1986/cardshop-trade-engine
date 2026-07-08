package com.cardshop.trade.engine.application.recharge;

/**
 * 商户回调发送结果。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.12 回调设计」。只有商户明确返回
 * {@code code=200} 或适配器判定成功时才能标记任务 SUCCESS。
 *
 * @param success 是否可将回调任务标记为 SUCCESS
 * @param statusCode HTTP 响应状态码；网络或解析异常时为 0
 * @param responseBody 商户响应原文，仅在成功或需要排障时保留
 * @param errorMessage 失败原因，用于写入 {@code recharge_notify_task.last_error}
 */
public record MerchantNotifySendResult(boolean success, int statusCode, String responseBody, String errorMessage) {

    public static MerchantNotifySendResult success(int statusCode, String responseBody) {
        return new MerchantNotifySendResult(true, statusCode, responseBody, "");
    }

    public static MerchantNotifySendResult failed(int statusCode, String errorMessage) {
        return new MerchantNotifySendResult(false, statusCode, "", errorMessage);
    }
}
