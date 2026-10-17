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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** 历史签名按可信商户与报文keyId检查，订单身份独立校验；参见详细设计3.1、9.3。 */
class V12CallbackSendGuardTest {
    private static final String KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    @Test
    @SuppressWarnings("unchecked")
    void retiredKeyCanSendOriginalPayloadButRevocationBlocksNextRetry() throws Exception {
        var store = mock(V12SigningCredentialStorePort.class);
        var credential = new V12SigningCredential("old-key", "A36", KEY);
        when(store.findForCallback("A36", "old-key")).thenReturn(Optional.of(credential), Optional.empty());
        var http = mock(HttpClient.class);
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.body()).thenReturn("{\"code\":200}");
        when(http.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        var adapter = adapter(http, store);
        var request = request(payload("old-key"));

        assertThat(adapter.send(request).success()).isTrue();
        assertThat(adapter.send(request).success()).isFalse();

        var sent = org.mockito.ArgumentCaptor.forClass(HttpRequest.class);
        verify(http, times(1)).send(sent.capture(), any(HttpResponse.BodyHandler.class));
        assertThat(sentBody(sent.getValue())).isEqualTo(request.payload());
        verify(store, never()).findCurrent(any());
        verify(store, times(2)).findForCallback("A36", "old-key");
    }

    @Test
    @SuppressWarnings("unchecked")
    void sameOrderCanUseAnotherRegisteredMerchantKey() throws Exception {
        var store = mock(V12SigningCredentialStorePort.class);
        when(store.findForCallback("A36", "another-key"))
                .thenReturn(Optional.of(new V12SigningCredential("another-key", "A36", KEY)));
        var http = mock(HttpClient.class);
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.body()).thenReturn("{\"code\":200}");
        when(http.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);

        assertThat(adapter(http, store).send(request(payload("another-key"))).success()).isTrue();
        verify(store, never()).findCurrent(any());
    }

    @Test
    void unknownKeyCannotFallBackToMerchantCurrentKey() throws Exception {
        var store = mock(V12SigningCredentialStorePort.class);
        when(store.findForCallback("A36", "unknown-key")).thenReturn(Optional.empty());
        when(store.findCurrent("A36")).thenReturn(Optional.of(new V12SigningCredential("current-key", "A36", KEY)));
        var http = mock(HttpClient.class);

        assertThat(adapter(http, store).send(request(payload("unknown-key"))).success()).isFalse();

        verifyNoInteractions(http);
        verify(store, never()).findCurrent(any());
    }

    @Test
    void validSignatureCannotOverrideTrustedTaskOrderIdentity() throws Exception {
        var store = mock(V12SigningCredentialStorePort.class);
        when(store.findForCallback("A36", "known-key"))
                .thenReturn(Optional.of(new V12SigningCredential("known-key", "A36", KEY)));
        var http = mock(HttpClient.class);
        var adapter = adapter(http, store);
        for (String field : new String[] {"order_id", "out_order_sn"}) {
            var wrongOrder = payload("known-key");
            wrongOrder.put(field, "OTHER-ORDER");
            sign(wrongOrder);
            assertThat(adapter.send(request(wrongOrder)).success()).isFalse();
        }
        verifyNoInteractions(http);
    }

    @Test
    void crossMerchantOrMismatchedCredentialCannotBeSent() throws Exception {
        var store = mock(V12SigningCredentialStorePort.class);
        when(store.findForCallback("A36", "known-key")).thenReturn(
                Optional.of(new V12SigningCredential("known-key", "OTHER-MERCHANT", KEY)),
                Optional.of(new V12SigningCredential("different-key", "A36", KEY)));
        var http = mock(HttpClient.class);
        var adapter = adapter(http, store);

        assertThat(adapter.send(request(payload("known-key"))).success()).isFalse();
        assertThat(adapter.send(request(payload("known-key"))).success()).isFalse();

        verifyNoInteractions(http);
    }

    @Test
    void tamperedCallbackIsRejectedBeforeHttpSend() throws Exception {
        var store = mock(V12SigningCredentialStorePort.class);
        when(store.findForCallback("A36", "known-key"))
                .thenReturn(Optional.of(new V12SigningCredential("known-key", "A36", KEY)));
        var http = mock(HttpClient.class);
        var payload = payload("known-key");
        payload.put("total_price", "9999");

        assertThat(adapter(http, store).send(request(payload)).success()).isFalse();

        verifyNoInteractions(http);
    }

    @Test
    void missingOrNonStringKeyIdIsRejectedWithoutLookupOrHttpSend() throws Exception {
        var store = mock(V12SigningCredentialStorePort.class);
        var http = mock(HttpClient.class);
        var adapter = adapter(http, store);
        for (Object keyId : new Object[] {null, "", 123}) {
            var invalid = payload("known-key");
            invalid.put("keyId", keyId);
            assertThat(adapter.send(request(invalid)).success()).isFalse();
        }
        verifyNoInteractions(store, http);
    }

    @Test
    void transportFailureDoesNotLeakUrlOrExceptionPayloadIntoLogsAndRetryRecord() throws Exception {
        var store = mock(V12SigningCredentialStorePort.class);
        when(store.findForCallback("A36", "known-key"))
                .thenReturn(Optional.of(new V12SigningCredential("known-key", "A36", KEY)));
        var http = mock(HttpClient.class);
        when(http.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenThrow(new java.io.IOException("private-payload-marker"));
        var original = request(payload("known-key"));
        var request = new MerchantNotifyRequest(original.taskId(), "https://merchant.example/callback?token=private-url-marker",
                original.payload(), original.orgCode(), original.orderId(), original.outOrderSn());
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(HttpMerchantNotifyClientAdapter.class);
        var capture = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        capture.start(); logger.addAppender(capture);
        try {
            var result = adapter(http, store).send(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).doesNotContain("private-payload-marker", "private-url-marker");
            assertThat(capture.list).hasSize(1);
            assertThat(capture.list.getFirst().getFormattedMessage()).doesNotContain("private-url-marker");
            assertThat(capture.list.getFirst().getThrowableProxy().getMessage()).doesNotContain("private-payload-marker");
        } finally { logger.detachAppender(capture); capture.stop(); }
    }

    private static HttpMerchantNotifyClientAdapter adapter(HttpClient http, V12SigningCredentialStorePort store) {
        return new HttpMerchantNotifyClientAdapter(http, new ObjectMapper(), Duration.ofSeconds(1), () -> store);
    }

    /** 读取真实BodyPublisher内容，确认历史任务逐字发送且未被重签或重排字段。 */
    private static String sentBody(HttpRequest request) throws Exception {
        var body = new java.util.concurrent.CompletableFuture<String>();
        var bytes = new java.io.ByteArrayOutputStream();
        request.bodyPublisher().orElseThrow().subscribe(new java.util.concurrent.Flow.Subscriber<java.nio.ByteBuffer>() {
            @Override public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }
            @Override public void onNext(java.nio.ByteBuffer buffer) {
                byte[] chunk = new byte[buffer.remaining()];
                buffer.get(chunk);
                bytes.writeBytes(chunk);
            }
            @Override public void onError(Throwable failure) {
                body.completeExceptionally(failure);
            }
            @Override public void onComplete() {
                body.complete(bytes.toString(java.nio.charset.StandardCharsets.UTF_8));
            }
        });
        return body.get(1, java.util.concurrent.TimeUnit.SECONDS);
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
