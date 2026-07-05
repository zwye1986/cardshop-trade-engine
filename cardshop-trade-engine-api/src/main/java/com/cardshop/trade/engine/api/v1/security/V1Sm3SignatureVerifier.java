package com.cardshop.trade.engine.api.v1.security;

import com.cardshop.trade.engine.api.v1.dto.V1RechargeCreateOrderRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderInvalidRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderQueryRequest;
import org.bouncycastle.crypto.digests.SM3Digest;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * `/v1/cardapi` SM3 签名校验器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.3 SM4 解密与 SM3 验签」。
 * 拼接规则兼容旧系统 {@code CardApiUtil.sortMapByAscii}：字段名 ASCII 升序，格式为
 * {@code key=value,key=value}，再对拼接串计算 SM3 hex。签名比较大小写兼容。
 */
@Component
public class V1Sm3SignatureVerifier {

    /**
     * 校验创建直充订单签名。
     */
    public V1SignatureVerifyResult verifyCreate(V1RechargeCreateOrderRequest request, String aesKey) {
        Objects.requireNonNull(request, "request must not be null");
        TreeMap<String, Object> params = new TreeMap<>();
        params.put("account", request.account());
        params.put("aesKey", aesKey);
        params.put("nonce_str", request.nonceStr());
        params.put("notify_url", request.notifyUrl());
        params.put("num", request.num());
        params.put("out_order_sn", request.outOrderSn());
        params.put("product_id", request.productId());
        params.put("timestamp", request.timestamp());
        return verify(params, request.sign());
    }

    /**
     * 校验直充查单签名。
     */
    public V1SignatureVerifyResult verifyQuery(V1RechargeOrderQueryRequest request, String aesKey) {
        Objects.requireNonNull(request, "request must not be null");
        TreeMap<String, Object> params = queryLikeParams(request.outOrderSn(), request.nonceStr(), request.timestamp(), aesKey);
        return verify(params, request.sign());
    }

    /**
     * 校验作废订单签名。
     */
    public V1SignatureVerifyResult verifyInvalid(V1RechargeOrderInvalidRequest request, String aesKey) {
        Objects.requireNonNull(request, "request must not be null");
        TreeMap<String, Object> params = queryLikeParams(request.outOrderSn(), request.nonceStr(), request.timestamp(), aesKey);
        return verify(params, request.sign());
    }

    public String digestHex(String source) {
        byte[] input = source.getBytes(StandardCharsets.UTF_8);
        SM3Digest digest = new SM3Digest();
        digest.update(input, 0, input.length);
        byte[] output = new byte[digest.getDigestSize()];
        digest.doFinal(output, 0);
        return hex(output);
    }

    private V1SignatureVerifyResult verify(TreeMap<String, Object> params, String actualSign) {
        String source = source(params);
        String expected = digestHex(source);
        boolean valid = actualSign != null && expected.equalsIgnoreCase(actualSign);
        return new V1SignatureVerifyResult(valid, source, expected);
    }

    private static TreeMap<String, Object> queryLikeParams(String outOrderSn, String nonceStr, String timestamp, String aesKey) {
        TreeMap<String, Object> params = new TreeMap<>();
        params.put("aesKey", aesKey);
        params.put("nonce_str", nonceStr);
        params.put("out_order_sn", outOrderSn);
        params.put("timestamp", timestamp);
        return params;
    }

    private static String source(Map<String, Object> params) {
        StringBuilder builder = new StringBuilder(128);
        params.forEach((key, value) -> builder.append(key).append('=').append(value).append(','));
        if (!builder.isEmpty()) {
            builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    private static String hex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            builder.append(Character.forDigit((value >> 4) & 0x0F, 16));
            builder.append(Character.forDigit(value & 0x0F, 16));
        }
        return builder.toString();
    }
}
