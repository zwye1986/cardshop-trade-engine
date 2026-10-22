package com.cardshop.trade.engine.application.recharge;

import java.util.Optional;

/** 由可信商户解析通道；银行配置缺失须抛异常，不能解释为无需通知。参见详细设计2104实施细化。 */
@FunctionalInterface
public interface MerchantNotifyRoutePort {
    Optional<MerchantNotifyRoute> resolve(String orgCode, String directNotifyUrl);

    static MerchantNotifyRoutePort directOnly() {
        return (org, url) -> url == null || url.isBlank() ? Optional.empty()
                : Optional.of(MerchantNotifyRoute.direct(org, url));
    }
}
