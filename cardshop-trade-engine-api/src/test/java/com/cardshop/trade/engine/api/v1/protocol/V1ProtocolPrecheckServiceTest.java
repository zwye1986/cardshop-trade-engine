package com.cardshop.trade.engine.api.v1.protocol;

import com.cardshop.trade.engine.api.v1.codec.V1RequestBodyCodec;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeCreateOrderRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderInvalidRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderQueryRequest;
import com.cardshop.trade.engine.api.v1.security.V1SignatureVerifyResult;
import com.cardshop.trade.engine.api.v1.security.V12SignatureVerifier;
import com.cardshop.trade.engine.api.v1.security.V1TimestampSettings;
import com.cardshop.trade.engine.api.v1.security.V1TimestampValidator;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class V1ProtocolPrecheckServiceTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-07-05T04:15:58Z"), ZoneOffset.UTC);

    private final V1ProtocolPrecheckService service = new V1ProtocolPrecheckService(
            new V1RequestBodyCodec(),
            new V1TimestampValidator(
                    new V1TimestampSettings(Duration.ofSeconds(300)),
                    FIXED_CLOCK
            ),
            new V12SignatureVerifier(),
            new V1ProtocolSettings(false)
    );

    private static V1ProtocolPrecheckService serviceWith(
            V12SignatureVerifier signatureVerifier,
            V1ProtocolSettings settings) {
        return new V1ProtocolPrecheckService(
                new V1RequestBodyCodec(),
                new V1TimestampValidator(
                        new V1TimestampSettings(Duration.ofSeconds(300)),
                        FIXED_CLOCK
                ),
                signatureVerifier,
                settings
        );
    }

    private static final String BAD_SIGN_CREATE_BODY = """
            {
              "product_id": "73301280",
              "out_order_sn": "OUT-1",
              "account": "18186222336",
              "timestamp": "1783224958",
              "nonce_str": "nonce-32",
              "notify_url": "http://notify",
              "num": 1,
              "order_price": 5200,
              "product_type": 1,
              "sign": "bad-sign"
            }
            """;

    @Test
    void acceptsPlainCreateRequestWhenTimestampAndSignatureAreValid() {
        String rawBody = """
                {
                  "product_id": "73301280",
                  "out_order_sn": "OUT-1",
                  "account": "18186222336",
                  "timestamp": "1783224958",
                  "nonce_str": "nonce-32",
                  "notify_url": "http://notify",
                  "num": 1,
                  "order_price": 5200,
                  "product_type": 1,
                  "sign": "8ebc766e26f42d7e212a542839d6e23d62909a701700b1c6db159862fc940cc4"
                }
                """;

        V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> result = service.precheckCreate(
                rawBody,
                new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY", com.cardshop.trade.engine.api.v1.V12TestCredentials.CREDENTIAL)
        );

        assertThat(result.valid()).isTrue();
        assertThat(result.reason()).isEqualTo("OK");
        assertThat(result.payload()).isNotNull();
        assertThat(result.payload().outOrderSn()).isEqualTo("OUT-1");
    }

    @Test
    void rejectsCreateRequestWhenTimestampIsOutOfRange() {
        String rawBody = """
                {
                  "product_id": "73301280",
                  "out_order_sn": "OUT-1",
                  "account": "18186222336",
                  "timestamp": "1783224557",
                  "nonce_str": "nonce-32",
                  "notify_url": "http://notify",
                  "num": 1,
                  "order_price": 5200,
                  "product_type": 1,
                  "sign": "8ebc766e26f42d7e212a542839d6e23d62909a701700b1c6db159862fc940cc4"
                }
                """;

        V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> result = service.precheckCreate(
                rawBody,
                new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY", com.cardshop.trade.engine.api.v1.V12TestCredentials.CREDENTIAL)
        );

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo("TIMESTAMP_OUT_OF_RANGE");
        assertThat(result.payload()).isNull();
    }

    @Test
    void rejectsCreateRequestWhenSignatureIsInvalid() {
        V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> result = service.precheckCreate(
                BAD_SIGN_CREATE_BODY,
                new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY", com.cardshop.trade.engine.api.v1.V12TestCredentials.CREDENTIAL)
        );

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo("SIGN_FAILED");
        assertThat(result.payload()).isNull();
    }

    @Test
    void rejectsCreateSignatureEvenWhenFormerBypassHeaderMatches() {
        V1ProtocolPrecheckService loadTestService = serviceWith(
                new V12SignatureVerifier(),
                new V1ProtocolSettings(false, "load-secret")
        );

        V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> result = loadTestService.precheckCreate(
                BAD_SIGN_CREATE_BODY,
                new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY", com.cardshop.trade.engine.api.v1.V12TestCredentials.CREDENTIAL),
                "load-secret"
        );

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo("SIGN_FAILED");
        assertThat(result.payload()).isNull();
    }

    @Test
    void doesNotBypassCreateSignatureWhenLoadTestTokenBlank() {
        V1ProtocolPrecheckService loadTestService = serviceWith(
                new V12SignatureVerifier(),
                new V1ProtocolSettings(false, "")
        );

        V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> result = loadTestService.precheckCreate(
                BAD_SIGN_CREATE_BODY,
                new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY", com.cardshop.trade.engine.api.v1.V12TestCredentials.CREDENTIAL),
                "load-secret"
        );

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo("SIGN_FAILED");
        assertThat(result.payload()).isNull();
    }

    @Test
    void doesNotBypassCreateSignatureWhenHeaderMismatches() {
        V1ProtocolPrecheckService loadTestService = serviceWith(
                new V12SignatureVerifier(),
                new V1ProtocolSettings(false, "load-secret")
        );

        V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> result = loadTestService.precheckCreate(
                BAD_SIGN_CREATE_BODY,
                new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY", com.cardshop.trade.engine.api.v1.V12TestCredentials.CREDENTIAL),
                "wrong"
        );

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo("SIGN_FAILED");
        assertThat(result.payload()).isNull();
    }

    @Test
    void stillVerifiesAndRejectsWithFormerBypassHeader() {
        CountingVerifier countingVerifier = new CountingVerifier();
        V1ProtocolPrecheckService loadTestService = serviceWith(
                countingVerifier,
                new V1ProtocolSettings(false, "load-secret")
        );

        V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> result = loadTestService.precheckCreate(
                BAD_SIGN_CREATE_BODY,
                new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY", com.cardshop.trade.engine.api.v1.V12TestCredentials.CREDENTIAL),
                "load-secret"
        );

        assertThat(result.valid()).isFalse();
        assertThat(countingVerifier.createCalls).isEqualTo(1);
    }

    @Test
    void acceptsPlainQueryRequestWhenTimestampAndSignatureAreValid() {
        String sign = signQueryLike("OUT-1", "nonce-32", "1783224958");
        String rawBody = """
                {
                  "out_order_sn": "OUT-1",
                  "timestamp": "1783224958",
                  "nonce_str": "nonce-32",
                  "sign": "%s"
                }
                """.formatted(sign);

        V1ProtocolPrecheckResult<V1RechargeOrderQueryRequest> result = service.precheckQuery(
                rawBody,
                new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY", com.cardshop.trade.engine.api.v1.V12TestCredentials.CREDENTIAL)
        );

        assertThat(result.valid()).isTrue();
        assertThat(result.reason()).isEqualTo("OK");
        assertThat(result.payload()).isNotNull();
        assertThat(result.payload().outOrderSn()).isEqualTo("OUT-1");
    }

    @Test
    void acceptsPlainInvalidRequestWhenTimestampAndSignatureAreValid() {
        String sign = signQueryLike("OUT-1", "nonce-32", "1783224958");
        String rawBody = """
                {
                  "out_order_sn": "OUT-1",
                  "reason": "customer request",
                  "timestamp": "1783224958",
                  "nonce_str": "nonce-32",
                  "sign": "%s"
                }
                """.formatted(sign);

        V1ProtocolPrecheckResult<V1RechargeOrderInvalidRequest> result = service.precheckInvalid(
                rawBody,
                new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY", com.cardshop.trade.engine.api.v1.V12TestCredentials.CREDENTIAL)
        );

        assertThat(result.valid()).isTrue();
        assertThat(result.reason()).isEqualTo("OK");
        assertThat(result.payload()).isNotNull();
        assertThat(result.payload().reason()).isEqualTo("customer request");
    }

    private static String signQueryLike(String outOrderSn, String nonceStr, String timestamp) {
        String source = "nonce_str=%s,out_order_sn=%s,timestamp=%s"
                .formatted(nonceStr, outOrderSn, timestamp);
        return com.cardshop.trade.engine.api.v1.V12TestCredentials.signSource(source);
    }

    /**
     * 计数创建订单验签调用次数，用于断言旁路仍执行 SM3。
     */
    private static final class CountingVerifier extends V12SignatureVerifier {
        private int createCalls;

        @Override
        public V1SignatureVerifyResult verifyCreate(V1RechargeCreateOrderRequest request, String aesKey) {
            createCalls++;
            return super.verifyCreate(request, aesKey);
        }
    }
}
