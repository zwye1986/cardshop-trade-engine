package com.cardshop.trade.engine.domain.service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * 直充受理请求幂等哈希规范化器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「14.2 Application 对象字段」：
 * 哈希只表达一次商户业务受理请求的身份，不纳入 {@code timestamp}、{@code nonce_str}、{@code sign}
 * 和来源 IP 等重放或传输字段，避免同一业务请求因重试参数变化被误判为新单。
 */
public final class RequestHashCanonicalizer {

    private static final String VERSION = "v1";
    private static final String ALGORITHM = "SHA-256";

    /**
     * 生成交行 V1 直充创建订单请求的稳定幂等哈希。
     *
     * <p>字段名使用新系统内部语义名，并按 ASCII 升序输出 {@code key=value\n}，便于排障时复现。
     */
    public RequestHash canonicalizeRechargeCreate(RechargeCreateFields fields) {
        Objects.requireNonNull(fields, "fields must not be null");
        TreeMap<String, String> canonicalFields = new TreeMap<>();
        canonicalFields.put("account", nullToEmpty(fields.account()));
        canonicalFields.put("notifyUrl", nullToEmpty(fields.notifyUrl()));
        canonicalFields.put("num", fields.num() == null ? "" : String.valueOf(fields.num()));
        canonicalFields.put("orgCode", nullToEmpty(fields.orgCode()));
        canonicalFields.put("outOrderSn", nullToEmpty(fields.outOrderSn()));
        canonicalFields.put("productId", nullToEmpty(fields.productId()));

        String canonicalText = canonicalText(canonicalFields);
        return new RequestHash(VERSION, ALGORITHM, canonicalText, sha256Hex(canonicalText));
    }

    private static String canonicalText(Map<String, String> fields) {
        StringBuilder builder = new StringBuilder();
        fields.forEach((key, value) -> builder.append(key).append('=').append(value).append('\n'));
        return builder.toString();
    }

    private static String sha256Hex(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance(ALGORITHM);
            byte[] bytes = digest.digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(bytes.length * 2);
            for (byte value : bytes) {
                hex.append(Character.forDigit((value >> 4) & 0x0F, 16));
                hex.append(Character.forDigit(value & 0x0F, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ALGORITHM + " is not available", ex);
        }
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    /**
     * 创建直充订单时用于识别商户业务请求的字段集合。
     */
    public record RechargeCreateFields(
            String orgCode,
            String productId,
            String outOrderSn,
            String account,
            String notifyUrl,
            Integer num) {
    }

    /**
     * 请求幂等哈希结果，保留规范化文本是为了线上定位时能复算和比对。
     */
    public record RequestHash(String version, String algorithm, String canonicalText, String value) {
    }
}
