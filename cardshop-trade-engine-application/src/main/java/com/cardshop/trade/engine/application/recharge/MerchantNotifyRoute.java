package com.cardshop.trade.engine.application.recharge;

import java.net.URI;

/**
 * 不含密钥正文的冻结路由，不能由下单方指定银行目标或凭据。
 * 参见详细设计「交行 2104 通知实施细化」。发送开关不属于路由，暂停不改变历史任务通道。
 */
public record MerchantNotifyRoute(NotifyDeliveryChannel channel, String orgCode, String routeVersion,
        String gatewayUrl, String protocol, String appId, String signatureAlgorithm,
        String privateKeyRef, String bankPublicKeyRef) {
    private static final int MAX_PROTOCOL_CHARACTERS=200;
    public MerchantNotifyRoute {
        if (channel == null || blank(orgCode)) throw new IllegalArgumentException("notify route identity missing");
        if (channel == NotifyDeliveryChannel.BOCOM_OPENAPI) {
            if (blank(routeVersion) || blank(gatewayUrl) || blank(protocol) || blank(appId)
                    || blank(privateKeyRef) || blank(bankPublicKeyRef)
                    || !("RSA".equals(signatureAlgorithm) || "SM2".equals(signatureAlgorithm)))
                throw new IllegalArgumentException("bank route configuration incomplete");
            if (protocol.codePointCount(0, protocol.length()) > MAX_PROTOCOL_CHARACTERS)
                throw new IllegalArgumentException("bank protocol exceeds character limit");
            URI target;
            try { target = URI.create(gatewayUrl); }
            catch (IllegalArgumentException ex) { throw new IllegalArgumentException("bank gateway URL invalid"); }
            if (!"https".equals(target.getScheme()) || target.getHost() == null
                    || target.getRawUserInfo() != null || target.getRawQuery() != null || target.getRawFragment() != null
                    || !"/api/trade/J_IEP_001_2104/v1".equals(target.getPath()))
                throw new IllegalArgumentException("bank gateway must be a trusted HTTPS 2104 endpoint");
        }
    }
    public static MerchantNotifyRoute direct(String org, String url) {
        return new MerchantNotifyRoute(NotifyDeliveryChannel.DIRECT_JSON, org, "legacy", url, "", "", "", "", "");
    }
    private static boolean blank(String value) { return value == null || value.isBlank(); }
}
