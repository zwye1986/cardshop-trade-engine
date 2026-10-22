package com.cardshop.trade.engine.infrastructure.notify.bocom;

import com.cardshop.trade.engine.application.recharge.*;
import java.util.Optional;

/** 仅使用可信商户注册，发送暂停不改变通道。参见详细设计「交行 2104 通知实施细化」。 */
public final class ConfiguredMerchantNotifyRouteAdapter implements MerchantNotifyRoutePort {
    private final BocomNotifyProperties properties;
    public ConfiguredMerchantNotifyRouteAdapter(BocomNotifyProperties properties) { this.properties = properties; }
    public Optional<MerchantNotifyRoute> resolve(String orgCode, String directNotifyUrl) {
        if (!properties.getMerchants().containsKey(orgCode))
            return MerchantNotifyRoutePort.directOnly().resolve(orgCode, directNotifyUrl);
        var m = properties.getMerchants().get(orgCode);
        if (m == null) throw new IllegalStateException("bank merchant route unavailable");
        try {
            return Optional.of(new MerchantNotifyRoute(NotifyDeliveryChannel.BOCOM_OPENAPI, orgCode,
                    m.getRouteVersion(), m.getGatewayUrl(), m.getProtocol(), m.getAppId(),
                    m.getSignatureAlgorithm(), m.getPrivateKeyRef(), m.getBankPublicKeyRef()));
        } catch (IllegalArgumentException ex) {
            // 不把可能含URL凭证的配置正文带进异常；原Outbox/核销事件保留待恢复。
            throw new IllegalStateException("bank merchant route incomplete or invalid");
        }
    }
}
