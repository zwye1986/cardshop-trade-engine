package com.cardshop.trade.engine.simulator.a39;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class V1Sm3SignerTest {

    private final V1Sm3Signer signer = new V1Sm3Signer();

    @Test
    void sm3DigestMatchesKnownVector() {
        assertThat(signer.digestHex("abc"))
                .isEqualTo("66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0");
    }

    @Test
    void createOrderSignatureUsesLegacyFieldSetAndAsciiOrdering() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("product_id", "73301280");
        payload.put("out_order_sn", "OUT-1");
        payload.put("account", "18186222336");
        payload.put("timestamp", "1783224958");
        payload.put("nonce_str", "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR");
        payload.put("notify_url", "http://notify");
        payload.put("num", 1);
        payload.put("order_price", 5200);
        payload.put("product_type", 1);

        V1Sm3Signer.Signature signature = signer.signCreateOrder(payload, "AES-KEY");

        assertThat(signature.source()).isEqualTo(
                "account=18186222336,aesKey=AES-KEY,nonce_str=JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR,"
                        + "notify_url=http://notify,num=1,out_order_sn=OUT-1,product_id=73301280,timestamp=1783224958");
        assertThat(signature.sign()).isEqualTo("95e8617e6b33203297bb4240dbfab24cea064e240fda70906bf9d6fde085b46c");
    }

    @Test
    void createOrderSignatureKeepsBlankNotifyUrlForLoadMode() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("product_id", "80100022");
        payload.put("out_order_sn", "PERF-A39-1");
        payload.put("account", "18186222336");
        payload.put("timestamp", "1783224958");
        payload.put("nonce_str", "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR");
        payload.put("notify_url", "");
        payload.put("num", 1);
        payload.put("order_price", 0);
        payload.put("product_type", 1);

        V1Sm3Signer.Signature signature = signer.signCreateOrder(payload, "AES-KEY");

        assertThat(signature.source()).isEqualTo(
                "account=18186222336,aesKey=AES-KEY,nonce_str=JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR,"
                        + "notify_url=,num=1,out_order_sn=PERF-A39-1,product_id=80100022,timestamp=1783224958");
    }

    @Test
    void querySignatureUsesLegacyQueryFieldSet() {
        Map<String, Object> payload = Map.of(
                "out_order_sn", "OUT-1",
                "timestamp", "1783224958",
                "nonce_str", "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR"
        );

        V1Sm3Signer.Signature signature = signer.signQueryOrder(payload, "AES-KEY");

        assertThat(signature.source()).isEqualTo(
                "aesKey=AES-KEY,nonce_str=JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR,out_order_sn=OUT-1,timestamp=1783224958");
        assertThat(signature.sign()).isEqualTo("f95c93b7d002df22b3173c3f706bb462c470161750483b4d0400e3a5a2136f70");
    }
}
