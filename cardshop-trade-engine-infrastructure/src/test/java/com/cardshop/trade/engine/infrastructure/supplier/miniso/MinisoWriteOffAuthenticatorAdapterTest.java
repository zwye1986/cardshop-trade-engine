package com.cardshop.trade.engine.infrastructure.supplier.miniso;

import static org.assertj.core.api.Assertions.*;

import com.cardshop.trade.engine.application.recharge.MinisoWriteOffException;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** 固定旧卡管向量及拒绝边界；参见详细设计9.5。 */
class MinisoWriteOffAuthenticatorAdapterTest {
    private static final String KEY = "miniso-callback-local-test-key";
    private static final String BODY =
            "{\"couponCode\":\"TEST-MINISO-0001\",\"useTime\":\"2026-10-17 09:29:00\"}";

    @Test
    void verifiesIndependentLegacyVectorAndCanonicalizesWhitespace() {
        var auth = new MinisoWriteOffAuthenticatorAdapter(KEY);
        var headers = headers("62088F92CF0CB4452F9A95098E60E350");
        var notice =
                auth.authenticate(
                        " { \"useTime\":\"2026-10-17 09:29:00\","
                                + " \"couponCode\":\"TEST-MINISO-0001\" }",
                        headers);
        assertThat(notice.couponCode()).isEqualTo("TEST-MINISO-0001");
        assertThat(notice.eventId())
                .isEqualTo(
                        "miniso-redeemed-ff08b00be72babc1a123564e3e203072731fd9811650618cec05951824fc3465");
        assertThat(notice.toString()).doesNotContain("TEST-MINISO");
        assertThat(auth.toString()).doesNotContain(KEY);
    }

    @Test
    void rejectsTamperingWrongKeyAndDuplicateHeaders() {
        var auth = new MinisoWriteOffAuthenticatorAdapter(KEY);
        assertThatThrownBy(
                        () ->
                                auth.authenticate(
                                        BODY.replace("0001", "0002"),
                                        headers("62088F92CF0CB4452F9A95098E60E350")))
                .isInstanceOf(MinisoWriteOffException.class)
                .hasMessage("SIGNATURE_FAILED");
        assertThatThrownBy(
                        () ->
                                new MinisoWriteOffAuthenticatorAdapter("wrong")
                                        .authenticate(
                                                BODY, headers("62088F92CF0CB4452F9A95098E60E350")))
                .hasMessage("SIGNATURE_FAILED");
        var duplicate = headers("62088F92CF0CB4452F9A95098E60E350");
        duplicate.put("SIGN", List.of("62088F92CF0CB4452F9A95098E60E350"));
        assertThatThrownBy(() -> auth.authenticate(BODY, duplicate)).hasMessage("INVALID");
    }

    @Test
    void rejectsDuplicateKeysNullTypesUnknownFieldsAndImpossibleDates() {
        var auth = new MinisoWriteOffAuthenticatorAdapter(KEY);
        for (var body :
                List.of(
                        BODY.replace("0001", "0001\",\"couponCode\":\"OTHER"),
                        BODY.replace("\"TEST-MINISO-0001\"", "null"),
                        BODY.replace("\"TEST-MINISO-0001\"", "123"),
                        BODY.replace("09:29:00", "29:29:00"),
                        BODY.replace("2026-10-17", "2026-02-30"),
                        BODY.replace("}", ",\"extra\":1}"))) {
            assertThatThrownBy(
                            () ->
                                    auth.authenticate(
                                            body, headers("62088F92CF0CB4452F9A95098E60E350")))
                    .hasMessage("INVALID");
        }
    }

    @Test
    void signsUtf8EscapesAndDoesNotRejectHistoricalDelivery() throws Exception {
        var body = "{\"couponCode\":\"测试\\\"A\\\\B\",\"useTime\":\"2026-10-17 09:29:00\"}";
        var source =
                KEY
                        + "nonce0123456789abcdef0123456789abcdeftenantMINISOtime1792200600000versiontripartite1.0"
                        + body;
        var sign =
                HexFormat.of()
                        .withUpperCase()
                        .formatHex(
                                MessageDigest.getInstance("MD5")
                                        .digest(source.getBytes(StandardCharsets.UTF_8)));
        assertThat(
                        new MinisoWriteOffAuthenticatorAdapter(KEY)
                                .authenticate(body, headers(sign))
                                .couponCode())
                .isEqualTo("测试\"A\\B");
    }

    private static Map<String, List<String>> headers(String sign) {
        var h = new HashMap<String, List<String>>();
        h.put("nonce", List.of("0123456789abcdef0123456789abcdef"));
        h.put("time", List.of("1792200600000"));
        h.put("sign", List.of(sign));
        return h;
    }
}
