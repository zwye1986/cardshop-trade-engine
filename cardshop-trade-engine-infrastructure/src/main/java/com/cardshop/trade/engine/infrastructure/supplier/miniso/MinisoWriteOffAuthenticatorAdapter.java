package com.cardshop.trade.engine.infrastructure.supplier.miniso;

import static com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason.*;

import com.cardshop.trade.engine.application.recharge.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.LocalDateTime;
import java.time.format.*;
import java.util.*;

/** 名创回调独立key2验签，复刻TreeMap两字段JSON，不签原始HTTP字节。 参见docs/交行直充业务系统详细设计.md「9.5」；不依赖全局Jackson配置，不输出签名原文。 */
public final class MinisoWriteOffAuthenticatorAdapter implements MinisoWriteOffAuthenticator {
    private static final int MAX_BODY_BYTES = 4096;
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                                    .build())
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
                    .withResolverStyle(ResolverStyle.STRICT);
    private final String key2;

    public MinisoWriteOffAuthenticatorAdapter(String key2) {
        if (key2 == null || key2.isBlank())
            throw new IllegalArgumentException("MINISO callback key2 required");
        this.key2 = key2;
    }

    @Override
    public MinisoWriteOffNotification authenticate(String raw, Map<String, List<String>> headers) {
        try {
            if (raw == null || raw.getBytes(StandardCharsets.UTF_8).length > MAX_BODY_BYTES)
                throw invalid();
            String nonce = header(headers, "nonce", true), time = header(headers, "time", true);
            String sign = header(headers, "sign", true);
            if (!nonce.matches("[a-z0-9]{32}")
                    || !time.matches("[0-9]{13}")
                    || !sign.matches("[A-F0-9]{32}")) throw invalid();
            for (var field : Map.of("tenant", "MINISO", "version", "tripartite1.0").entrySet()) {
                String supplied = header(headers, field.getKey(), false);
                if (supplied != null && !supplied.equals(field.getValue())) throw invalid();
            }
            JsonNode body = JSON.readTree(raw);
            if (body == null
                    || !body.isObject()
                    || body.size() != 2
                    || !body.has("couponCode")
                    || !body.has("useTime")
                    || !body.get("couponCode").isTextual()
                    || !body.get("useTime").isTextual()) throw invalid();
            String coupon = body.get("couponCode").textValue(),
                    used = body.get("useTime").textValue();
            if (coupon.isBlank()
                    || coupon.codePointCount(0, coupon.length()) > 128
                    || !coupon.equals(coupon.strip())
                    || !StandardCharsets.UTF_8.newEncoder().canEncode(coupon)
                    || coupon.chars().anyMatch(Character::isISOControl)
                    || used.length() != 19) throw invalid();
            LocalDateTime useTime = LocalDateTime.parse(used, TIME);
            var canonical = new TreeMap<String, String>();
            canonical.put("couponCode", coupon);
            canonical.put("useTime", used);
            String source =
                    key2
                            + "nonce"
                            + nonce
                            + "tenantMINISOtime"
                            + time
                            + "versiontripartite1.0"
                            + JSON.writeValueAsString(canonical);
            byte[] expected =
                    MessageDigest.getInstance("MD5")
                            .digest(source.getBytes(StandardCharsets.UTF_8));
            if (!MessageDigest.isEqual(expected, HexFormat.of().parseHex(sign)))
                throw new MinisoWriteOffException(SIGNATURE_FAILED);
            return new MinisoWriteOffNotification(coupon, useTime);
        } catch (JsonProcessingException | DateTimeParseException ex) {
            // 立即抛安全分类异常，不携带可能含正文/券码的cause，由入口统一采样记录。
            throw invalid();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("MD5 unavailable", ex);
        }
    }

    private static String header(Map<String, List<String>> headers, String name, boolean required) {
        List<String> values = new ArrayList<>();
        headers.forEach(
                (key, value) -> {
                    if (name.equalsIgnoreCase(key) && value != null) values.addAll(value);
                });
        if (values.isEmpty() && !required) return null;
        if (values.size() != 1 || values.getFirst() == null) throw invalid();
        return values.getFirst();
    }

    private static MinisoWriteOffException invalid() {
        return new MinisoWriteOffException(INVALID);
    }

    @Override
    public String toString() {
        return "MinisoWriteOffAuthenticatorAdapter[key2=REDACTED]";
    }
}
