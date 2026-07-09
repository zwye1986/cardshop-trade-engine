package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * V1 商户回调签名测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「20.13.12 回调设计」的空字段保留和 SM3
 * 签名规则。回调 payload 的 key 不能因为 Java null 或业务字段缺失而被省略。
 */
class V1MerchantNotifyPayloadSignerTest {

    @Test
    void signsOrderStatusPayloadAndKeepsRequiredKeys() {
        V1MerchantNotifyPayloadSigner signer = new V1MerchantNotifyPayloadSigner(
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "AES-KEY", true, 7L)));

        Map<String, String> payload = signer.signOrderStatus(order(), "12", Instant.parse("2026-07-08T03:20:00Z"));

        assertThat(payload)
                .containsEntry("orderId", "ORDER-1")
                .containsEntry("outOrderSn", "OUT-1")
                .containsEntry("totalPrice", "5200")
                .containsEntry("type", "")
                .containsEntry("totalSettlePrice", "5200")
                .containsEntry("timestamp", "1783480800")
                .containsEntry("orderStatus", "12")
                .containsEntry("account", "13800138000");
        assertThat(payload.get("nonceStr")).hasSize(32);
        assertThat(payload.get("sign")).hasSize(64);
    }

    @Test
    void signsWriteOffPayloadWithEmptyFieldsPresent() {
        V1MerchantNotifyPayloadSigner signer = new V1MerchantNotifyPayloadSigner(
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "AES-KEY", true, 7L)));

        Map<String, String> payload = signer.signWriteOff(order(), Instant.parse("2026-07-08T03:20:00Z"));

        assertThat(payload)
                .containsEntry("orderId", "ORDER-1")
                .containsEntry("outOrderSn", "OUT-1")
                .containsEntry("account", "13800138000")
                .containsEntry("useTime", "")
                .containsEntry("stateType", "1")
                .containsEntry("useState", "2")
                .containsEntry("invalidTime", "1783480800");
        assertThat(payload.get("nonceStr")).hasSize(32);
        assertThat(payload.get("sign")).hasSize(64);
    }

    private static RechargeFulfillmentOrder order() {
        return new RechargeFulfillmentOrder(
                "ORDER-1",
                "OUT-1",
                "A36",
                "13800138000",
                "PARENT-1",
                "SKU-1",
                "FNK_CGS_BX",
                5200L,
                "https://merchant.example/notify",
                12,
                "DB_PERSISTED",
                "PASSED",
                true,
                true);
    }
}
