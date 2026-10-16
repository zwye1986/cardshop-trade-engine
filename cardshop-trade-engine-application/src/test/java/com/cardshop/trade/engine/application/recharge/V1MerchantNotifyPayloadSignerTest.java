package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredential;
import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort;
import com.cardshop.trade.engine.application.v1.crypto.V12MessageSignatures;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * V1 商户回调签名测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「3.1」「9.3 回调设计」的商户当前密钥和 HMAC-SM3
 * 签名规则。回调 payload 的 key 不能因为 Java null 或业务字段缺失而被省略。
 */
class V1MerchantNotifyPayloadSignerTest {

    @Test
    void signsOrderStatusPayloadAndKeepsRequiredKeys() {
        V1MerchantNotifyPayloadSigner signer = new V1MerchantNotifyPayloadSigner(
                store());

        Map<String, String> payload = signer.signOrderStatus(order(), "12", Instant.parse("2026-07-08T03:20:00Z"));

        assertThat(payload)
                .containsEntry("order_id", "ORDER-1")
                .containsEntry("out_order_sn", "OUT-1")
                .containsEntry("total_price", "5200")
                .containsEntry("type", "1")
                .containsEntry("total_settle_price", "5200")
                .containsEntry("timestamp", "1783480800")
                .containsEntry("order_status", "12")
                .containsEntry("account", "13800138000");
        assertThat(payload.get("nonce_str")).hasSize(32);
        assertThat(payload.get("sign")).hasSize(64);
        assertThat(payload).containsKey("keyId");
    }

    @Test
    void signsWriteOffPayloadWithEmptyFieldsPresent() {
        V1MerchantNotifyPayloadSigner signer = new V1MerchantNotifyPayloadSigner(
                store());

        Map<String, String> payload = signer.signWriteOff(order(), Instant.parse("2026-07-08T03:20:00Z"));

        assertThat(payload)
                .containsEntry("order_id", "ORDER-1")
                .containsEntry("out_order_sn", "OUT-1")
                .containsEntry("account", "13800138000")
                .containsEntry("use_time", "")
                .containsEntry("state_type", "1")
                .containsEntry("use_state", "2")
                .containsEntry("invalid_time", "2026-07-08 11:20:00");
        assertThat(payload.get("nonce_str")).hasSize(32);
        assertThat(payload.get("sign")).hasSize(64);
        assertThat(payload).containsKey("keyId");
    }

    private static com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort store() {
        var store = org.mockito.Mockito.mock(com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort.class);
        org.mockito.Mockito.when(store.findCurrent("A36")).thenReturn(Optional.of(
            new com.cardshop.trade.engine.application.v1.crypto.V12SigningCredential("bound-key","A36",
                "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")));
        return store;
    }
    @Test void rejectsMerchantWithoutCurrentKey() {
        var signer = new V1MerchantNotifyPayloadSigner(
                com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort.unavailable());
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> signer.signOrderStatus(order(),"12",Instant.now()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test void laterCallbacksForSameOrderUseRotatedMerchantCurrentKey() {
        var store = org.mockito.Mockito.mock(V12SigningCredentialStorePort.class);
        String key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
        org.mockito.Mockito.when(store.findCurrent("A36")).thenReturn(
                Optional.of(new V12SigningCredential("first-key", "A36", key)),
                Optional.of(new V12SigningCredential("rotated-key", "A36", key)));
        var signer = new V1MerchantNotifyPayloadSigner(store);
        Instant now = Instant.parse("2026-07-08T03:20:00Z");

        var first = signer.signOrderStatus(order(), "12", now);
        var later = signer.signWriteOff(order(), now);

        assertThat(first).containsEntry("keyId", "first-key");
        assertThat(later).containsEntry("keyId", "rotated-key");
        assertThat(V12MessageSignatures.verify(V12MessageSignatures.Operation.ORDER_CALLBACK, first, key)).isTrue();
        assertThat(V12MessageSignatures.verify(V12MessageSignatures.Operation.WRITEOFF_CALLBACK, later, key)).isTrue();
    }

    @Test void rejectsCredentialFromDifferentMerchant() {
        var store = org.mockito.Mockito.mock(V12SigningCredentialStorePort.class);
        org.mockito.Mockito.when(store.findCurrent("A36")).thenReturn(Optional.of(
                new V12SigningCredential("foreign-key", "OTHER", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")));
        var signer = new V1MerchantNotifyPayloadSigner(store);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> signer.signOrderStatus(order(), "12", Instant.now()))
                .isInstanceOf(IllegalStateException.class);
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
