package com.cardshop.trade.engine.infrastructure.notify;

import com.cardshop.trade.engine.application.recharge.MerchantNotifyRequest;
import com.cardshop.trade.engine.application.v1.crypto.V12MessageSignatures;
import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredential;
import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** 发送边界重新检查撤销状态，不能因为旧快照已签名而绕过；参见详细设计3.1。 */
class V12CallbackSendGuardTest {
    private static final String KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    @Test
    @SuppressWarnings("unchecked")
    void expiredRequestCredentialCanSendButRevocationBlocksNextRetry() throws Exception {
        var store = mock(V12SigningCredentialStorePort.class);
        var credential = new V12SigningCredential("old-key", "app", "A36", KEY, Instant.EPOCH);
        when(store.findForOrder("A36", "ORDER-1")).thenReturn(Optional.of(credential), Optional.empty());
        var http = mock(HttpClient.class);
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.body()).thenReturn("{\"code\":200}");
        when(http.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        var adapter = new HttpMerchantNotifyClientAdapter(http, new ObjectMapper(), Duration.ofSeconds(1), () -> store);
        var request = request(payload("old-key"));
        assertThat(adapter.send(request).success()).isTrue();
        assertThat(adapter.send(request).success()).isFalse();
        verify(http, times(1)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    void validSignatureFromDifferentKeyOrOrderCannotBeSent() throws Exception {
        var store = mock(V12SigningCredentialStorePort.class);
        when(store.findForOrder("A36", "ORDER-1")).thenReturn(Optional.of(
                new V12SigningCredential("bound-key", "app", "A36", KEY, Instant.EPOCH)));
        var http = mock(HttpClient.class);
        var adapter = new HttpMerchantNotifyClientAdapter(http, new ObjectMapper(), Duration.ofSeconds(1), () -> store);
        assertThat(adapter.send(request(payload("another-key"))).success()).isFalse();
        var wrongOrder = payload("bound-key");
        wrongOrder.put("out_order_sn", "OUT-OTHER");
        sign(wrongOrder);
        assertThat(adapter.send(request(wrongOrder)).success()).isFalse();
        verifyNoInteractions(http);
    }

    private static MerchantNotifyRequest request(Map<String, Object> payload) throws Exception {
        return new MerchantNotifyRequest("task", "https://merchant.example/callback",
                new ObjectMapper().writeValueAsString(payload), "A36", "ORDER-1", "OUT-1");
    }

    private static Map<String, Object> payload(String keyId) {
        Map<String, Object> payload = new LinkedHashMap<>(Map.of(
                "keyId", keyId, "order_id", "ORDER-1", "out_order_sn", "OUT-1", "account", "",
                "nonce_str", "nonce", "timestamp", "1783480800", "order_status", "12",
                "total_price", "5200", "total_settle_price", "5200", "type", "1"));
        sign(payload);
        return payload;
    }

    private static void sign(Map<String, Object> payload) {
        payload.put("sign", V12MessageSignatures.sign(V12MessageSignatures.Operation.ORDER_CALLBACK, payload, KEY));
    }
}
