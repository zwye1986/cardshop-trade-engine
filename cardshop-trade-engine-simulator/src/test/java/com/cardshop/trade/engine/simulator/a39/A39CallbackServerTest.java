package com.cardshop.trade.engine.simulator.a39;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class A39CallbackServerTest {

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final V1Sm3Signer signer = new V1Sm3Signer();

    @Test
    void acceptsSignedCallbackAndRejectsTamperedCallback() throws Exception {
        try (A39CallbackServer server = A39CallbackServer.start("127.0.0.1", 0, "AES-KEY", signer)) {
            String successBody = signedCallbackBody("12");
            HttpResponse<String> success = post(server.callbackUrl("/callback/order"), successBody);

            assertThat(success.statusCode()).isEqualTo(200);
            assertThat(success.body()).isEqualTo("{\"code\":200,\"msg\":\"接收成功\"}");
            assertThat(server.receivedCallbacks()).hasSize(1);

            String tamperedBody = successBody.replace("\"order_status\":\"12\"", "\"order_status\":\"6\"");
            HttpResponse<String> failed = post(server.callbackUrl("/callback/order"), tamperedBody);

            assertThat(failed.statusCode()).isEqualTo(200);
            assertThat(failed.body()).isEqualTo("{\"code\":500,\"msg\":\"接收失败，请重试\"}");
            assertThat(server.receivedCallbacks()).hasSize(2);
            assertThat(server.receivedCallbacks().get(1).signatureValid()).isFalse();
        }
    }

    private String signedCallbackBody(String orderStatus) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("order_id", "ORDER-1");
        payload.put("out_order_sn", "OUT-1");
        payload.put("total_price", 5200);
        payload.put("type", "1");
        payload.put("total_settle_price", 5200);
        payload.put("timestamp", "1783224958");
        payload.put("nonce_str", "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR");
        payload.put("order_status", orderStatus);
        payload.put("account", "18186222336");
        payload.put("sign", signer.signCallback(payload, "AES-KEY").sign());
        return """
                {"order_id":"%s","out_order_sn":"%s","total_price":5200,"type":"1","total_settle_price":5200,"timestamp":"1783224958","nonce_str":"JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR","order_status":"%s","account":"18186222336","sign":"%s"}
                """.formatted(payload.get("order_id"), payload.get("out_order_sn"), orderStatus, payload.get("sign"));
    }

    private HttpResponse<String> post(String url, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
