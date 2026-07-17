package com.cardshop.trade.engine.simulator.a39;

import org.bouncycastle.crypto.digests.SM3Digest;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * 交行 V1 协议 SM3 签名器。
 *
 * <p>签名规则对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」和接口文档
 * 「1.2 签名规则」：移除 {@code sign}，加入商户 {@code aesKey}，按字段名 ASCII 升序拼接
 * {@code key=value}，再计算 SM3 hex。下单签名字段必须保持旧系统 {@code CardApiUtil}
 * 兼容口径，不能把新服务 DTO 为运行需要携带的 {@code order_price/product_type} 纳入签名。
 */
public final class V1Sm3Signer {

    private static final String SIGN_FIELD = "sign";
    private static final String AES_KEY_FIELD = "aesKey";
    private static final List<String> CREATE_ORDER_SIGN_FIELDS = List.of(
            "account",
            "nonce_str",
            "notify_url",
            "num",
            "out_order_sn",
            "product_id",
            "timestamp"
    );
    private static final List<String> QUERY_ORDER_SIGN_FIELDS = List.of(
            "nonce_str",
            "out_order_sn",
            "timestamp"
    );

    public Signature signCreateOrder(Map<String, ?> payload, String aesKey) {
        return signSelected(payload, aesKey, CREATE_ORDER_SIGN_FIELDS, true);
    }

    public Signature signQueryOrder(Map<String, ?> payload, String aesKey) {
        return signSelected(payload, aesKey, QUERY_ORDER_SIGN_FIELDS, false);
    }

    public Signature signCallback(Map<String, ?> payload, String aesKey) {
        return signAll(payload, aesKey, true);
    }

    public boolean verifyCallback(Map<String, ?> payload, String aesKey) {
        Object actual = payload.get(SIGN_FIELD);
        if (actual == null) {
            return false;
        }
        String expected = signCallback(payload, aesKey).sign();
        return expected.equalsIgnoreCase(String.valueOf(actual));
    }

    public String digestHex(String source) {
        byte[] input = source.getBytes(StandardCharsets.UTF_8);
        SM3Digest digest = new SM3Digest();
        digest.update(input, 0, input.length);
        byte[] output = new byte[digest.getDigestSize()];
        digest.doFinal(output, 0);
        return hex(output);
    }

    private Signature signSelected(Map<String, ?> payload, String aesKey, List<String> fieldNames, boolean keepEmptyValues) {
        Objects.requireNonNull(payload, "payload must not be null");
        TreeMap<String, String> params = new TreeMap<>();
        for (String fieldName : fieldNames) {
            Object value = payload.get(fieldName);
            if (keepEmptyValues ? payload.containsKey(fieldName) : hasText(value)) {
                params.put(fieldName, value == null ? "null" : String.valueOf(value));
            }
        }
        params.put(AES_KEY_FIELD, requireText(aesKey, "aesKey"));
        return sign(params);
    }

    private Signature signAll(Map<String, ?> payload, String aesKey, boolean keepEmptyValues) {
        Objects.requireNonNull(payload, "payload must not be null");
        TreeMap<String, String> params = new TreeMap<>();
        payload.forEach((key, value) -> {
            if (!SIGN_FIELD.equals(key) && (keepEmptyValues || hasText(value))) {
                params.put(key, value == null ? "" : String.valueOf(value));
            }
        });
        params.put(AES_KEY_FIELD, requireText(aesKey, "aesKey"));
        return sign(params);
    }

    private Signature sign(TreeMap<String, String> params) {
        String source = source(params);
        return new Signature(source, digestHex(source));
    }

    private static String source(TreeMap<String, String> params) {
        StringBuilder builder = new StringBuilder(128);
        params.forEach((key, value) -> builder.append(key).append('=').append(value).append(','));
        if (!builder.isEmpty()) {
            builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static boolean hasText(Object value) {
        return value != null && !String.valueOf(value).isBlank();
    }

    private static String hex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            builder.append(Character.forDigit((value >> 4) & 0x0F, 16));
            builder.append(Character.forDigit(value & 0x0F, 16));
        }
        return builder.toString().toLowerCase(Locale.ROOT);
    }

    public record Signature(String source, String sign) {
    }
}
