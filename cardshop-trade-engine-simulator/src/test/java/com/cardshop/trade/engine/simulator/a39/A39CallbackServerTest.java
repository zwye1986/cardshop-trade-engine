package com.cardshop.trade.engine.simulator.a39;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class A39CallbackServerTest {

    private static final String ROTATED_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final V12Signer signer = new V12Signer();
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void acceptsSignedCallbackAndRejectsTamperedCallback() throws Exception {
        try (A39CallbackServer server = server()) {
            server.registerCredentials(new Credentials("token", "test-key", TestKeys.KEY));
            String body = signedCallbackBody("test-key", TestKeys.KEY, "OUT-1", "18186222336");
            assertThat(post(server, body).body()).contains("\"code\":500");
            server.registerOrder("OUT-1", "18186222336");
            HttpResponse<String> success = post(server, body);
            assertThat(success.statusCode()).isEqualTo(200);
            assertThat(success.body()).isEqualTo("{\"code\":200,\"msg\":\"接收成功\"}");
            assertThat(post(server, body.replace("\"order_status\":\"12\"", "\"order_status\":\"6\"")).body())
                    .contains("\"code\":500");
            assertThat(server.receivedCallbacks()).hasSize(3);
            assertThat(server.receivedCallbacks().get(2).signatureValid()).isFalse();
        }
    }

    @Test
    void acceptsSameKnownOrderUnderCurrentAndRetiredMerchantKeysWithoutRebinding() throws Exception {
        try (A39CallbackServer server = server()) {
            server.registerOrder("OUT-1", "18186222336");
            server.registerCredentials(new Credentials("old-token", "old-key", TestKeys.KEY));
            server.registerCredentials(new Credentials("new-token", "current-key", ROTATED_KEY));
            String original = signedCallbackBody("old-key", TestKeys.KEY, "OUT-1", "18186222336");
            assertThat(post(server, original).body()).contains("\"code\":200");
            assertThat(post(server, signedCallbackBody("current-key", ROTATED_KEY, "OUT-1", "18186222336")).body())
                    .contains("\"code\":200");
            // 正常重试保留原报文与keyId；不要求旧token有效。
            assertThat(post(server, original).body()).contains("\"code\":200");
        }
    }

    @Test
    void tokenRefreshKeepsSigningKeyAndDoesNotRestoreRevokedKey() throws Exception {
        try (A39CallbackServer server = server()) {
            server.registerOrder("OUT-1", "18186222336");
            server.registerCredentials(new Credentials("old-token", "test-key", TestKeys.KEY));
            server.registerCredentials(new Credentials("refreshed-token", "test-key", TestKeys.KEY));
            String body = signedCallbackBody("test-key", TestKeys.KEY, "OUT-1", "18186222336");
            assertThat(post(server, body).body()).contains("\"code\":200");
            server.revokeKey("test-key");
            server.registerCredentials(new Credentials("another-token", "test-key", TestKeys.KEY));
            assertThat(post(server, body).body()).contains("\"code\":500");
            assertThatThrownBy(() -> server.registerCredentials(new Credentials("token", "test-key", ROTATED_KEY)))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void rejectsUnknownRevokedAndCrossMerchantKeysWithoutTryingCurrentKey() throws Exception {
        try (A39CallbackServer merchant = server(); A39CallbackServer anotherMerchant = server()) {
            merchant.registerOrder("OUT-1", "18186222336");
            merchant.registerCredentials(new Credentials("token", "current-key", TestKeys.KEY));
            anotherMerchant.registerCredentials(new Credentials("other-token", "other-key", TestKeys.KEY));
            assertThat(post(merchant, signedCallbackBody("unknown-key", TestKeys.KEY, "OUT-1", "18186222336")).body())
                    .contains("\"code\":500");
            assertThat(post(merchant, signedCallbackBody("other-key", TestKeys.KEY, "OUT-1", "18186222336")).body())
                    .contains("\"code\":500");
            merchant.revokeKey("current-key");
            assertThat(post(merchant, signedCallbackBody("current-key", TestKeys.KEY, "OUT-1", "18186222336")).body())
                    .contains("\"code\":500");
        }
    }

    @Test
    void rejectsSignedNoticeForWrongTrustedOrderAccount() throws Exception {
        try (A39CallbackServer server = server()) {
            server.registerCredentials(new Credentials("token", "test-key", TestKeys.KEY));
            server.registerOrder("OUT-1", "18186222336");
            assertThat(post(server, signedCallbackBody("test-key", TestKeys.KEY, "OUT-1", "another-account")).body())
                    .contains("\"code\":500");
            assertThatThrownBy(() -> server.registerOrder("OUT-1", "another-account"))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void rejectsDuplicateKeyIdEvenWhenLastValueAndSignatureAreValid() throws Exception {
        try (A39CallbackServer server = server()) {
            server.registerCredentials(new Credentials("token", "test-key", TestKeys.KEY));
            server.registerOrder("OUT-1", "18186222336");
            String validBody = signedCallbackBody("test-key", TestKeys.KEY, "OUT-1", "18186222336");
            assertThat(post(server, validBody).body()).contains("\"code\":200");
            String duplicateBody = validBody.replace("\"keyId\":\"test-key\"",
                    "\"keyId\":\"unknown-key\",\"keyId\":\"test-key\"");

            assertThat(post(server, duplicateBody).body()).contains("\"code\":500");
            assertThat(server.receivedCallbacks().get(1).signatureValid()).isFalse();
        }
    }

    @Test
    void rejectsTrailingJsonAfterOtherwiseValidSignedPayload() throws Exception {
        try (A39CallbackServer server = server()) {
            server.registerCredentials(new Credentials("token", "test-key", TestKeys.KEY));
            server.registerOrder("OUT-1", "18186222336");
            String validBody = signedCallbackBody("test-key", TestKeys.KEY, "OUT-1", "18186222336");
            assertThat(post(server, validBody).body()).contains("\"code\":200");

            assertThat(post(server, validBody + " {\"keyId\":\"ignored\"}").body())
                    .contains("\"code\":500");
            assertThat(server.receivedCallbacks().get(1).signatureValid()).isFalse();
        }
    }

    private A39CallbackServer server() throws Exception {
        return A39CallbackServer.start("127.0.0.1", 0, signer);
    }

    private String signedCallbackBody(String keyId, String signKey, String orderSn, String account) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("keyId", keyId);
        payload.put("order_id", "ORDER-1");
        payload.put("out_order_sn", orderSn);
        payload.put("total_price", 5200);
        payload.put("type", "1");
        payload.put("total_settle_price", 5200);
        payload.put("timestamp", "1783224958");
        payload.put("nonce_str", "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR");
        payload.put("order_status", "12");
        payload.put("account", account);
        payload.put("sign", signer.signCallback(payload, signKey).sign());
        return json.writeValueAsString(payload);
    }

    private HttpResponse<String> post(A39CallbackServer server, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(server.callbackUrl("/callback/order")))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
