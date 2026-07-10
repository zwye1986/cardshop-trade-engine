package com.cardshop.trade.engine.application.recharge;

/**
 * 商户回调发送端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。基础设施实现负责 HTTP
 * 超时、响应 JSON 解析和网络异常转换。
 */
@FunctionalInterface
public interface MerchantNotifyClientPort {

    MerchantNotifySendResult send(MerchantNotifyRequest request);
}
