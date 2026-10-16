package com.cardshop.trade.engine.simulator.a39;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class V12SignerTest {

    private final V12Signer signer = new V12Signer();

    @Test
    void createOrderSignatureUsesV12FieldSetAndAsciiOrdering() {
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

        V12Signer.Signature signature = signer.signCreateOrder(payload, TestKeys.KEY);

        assertThat(signature.source()).isEqualTo(
                "account=18186222336,nonce_str=JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR,"
                        + "notify_url=http://notify,num=1,out_order_sn=OUT-1,product_id=73301280,timestamp=1783224958");
        assertThat(signature.sign()).isEqualTo("f9d4718955501516946fb3bb4facfe2ec87429f6d67c9488415b32ec59f6bd88");
    }

    @Test
    void createOrderSignatureOmitsBlankNotifyUrlForLoadMode() {
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

        V12Signer.Signature signature = signer.signCreateOrder(payload, TestKeys.KEY);

        assertThat(signature.source()).isEqualTo(
                "account=18186222336,nonce_str=JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR,"
                        + "num=1,out_order_sn=PERF-A39-1,product_id=80100022,timestamp=1783224958");
    }

    @Test
    void querySignatureUsesV12QueryFieldSet() {
        Map<String, Object> payload = Map.of(
                "out_order_sn", "OUT-1",
                "timestamp", "1783224958",
                "nonce_str", "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR"
        );

        V12Signer.Signature signature = signer.signQueryOrder(payload, TestKeys.KEY);

        assertThat(signature.source()).isEqualTo(
                "nonce_str=JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR,out_order_sn=OUT-1,timestamp=1783224958");
        assertThat(signature.sign()).isEqualTo("d6699a015e9586ca76d7224c1a0451a66743962faa61bc592b51c6c84d4a7cc8");
    }
}
