package com.cardshop.trade.engine.infrastructure.notify.bocom;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import com.cardshop.trade.engine.application.recharge.NotifyDeliveryChannel;

/** 显式注册银行商户，暂停不能回退直连；参见详细设计2104实施细化。 */
class ConfiguredMerchantNotifyRouteAdapterTest {
    @Test void configuredBankRouterMustExist() {
        assertThatCode(() -> Class.forName("com.cardshop.trade.engine.infrastructure.notify.bocom.ConfiguredMerchantNotifyRouteAdapter"))
                .doesNotThrowAnyException();
    }
    @Test void registeredMerchantUsesBankEvenWhenSendingDisabledAndOriginalUrlBlank() {
        var p = configured();
        var router = new ConfiguredMerchantNotifyRouteAdapter(p);
        var route = router.resolve("A35", "").orElseThrow();
        assertThat(route.channel()).isEqualTo(NotifyDeliveryChannel.BOCOM_OPENAPI);
        assertThat(route.protocol()).isEqualTo("p1");
        p.getMerchants().get("A35").setProtocol("p2");
        assertThat(route.protocol()).isEqualTo("p1");
    }
    @Test void incompleteRegistrationFailsWithoutFallingBackToUntrustedUrl() {
        var p = configured();
        p.getMerchants().get("A35").setProtocol("");
        assertThatIllegalStateException().isThrownBy(() -> new ConfiguredMerchantNotifyRouteAdapter(p)
                .resolve("A35","https://untrusted.invalid"));
    }
    @Test void unregisteredMerchantKeepsLegacyBlankUrlAndDirectSemantics() {
        var router = new ConfiguredMerchantNotifyRouteAdapter(configured());
        assertThat(router.resolve("OTHER", "")).isEmpty();
        assertThat(router.resolve("OTHER", "https://merchant.invalid").orElseThrow().channel()).isEqualTo(NotifyDeliveryChannel.DIRECT_JSON);
    }
    private static BocomNotifyProperties configured() {
        var p = new BocomNotifyProperties();
        var m = new BocomNotifyProperties.Merchant();
        m.setRouteVersion("r1"); m.setGatewayUrl("https://bank.invalid/api/trade/J_IEP_001_2104/v1");
        m.setProtocol("p1"); m.setAppId("app"); m.setPrivateKeyRef("private-v1"); m.setBankPublicKeyRef("bank-v1");
        p.getMerchants().put("A35",m);
        return p;
    }
}
