package com.cardshop.trade.engine.api.v1.security;

import com.cardshop.trade.engine.api.v1.dto.V1RechargeCreateOrderRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderInvalidRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderQueryRequest;
import com.cardshop.trade.engine.application.v1.crypto.V12MessageSignatures;
import static com.cardshop.trade.engine.application.v1.crypto.V12MessageSignatures.Operation.*;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.TreeMap;

/**
 * `/v1/cardapi` V1.2 HMAC-SM3签名校验器，使用可信token配对的signKey。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * 对应详细设计3.1及调用方源码包：字段白名单、请求空值省略、64位小写认证码和常量时间比较。
 */
@Component
public class V12SignatureVerifier {

    /**
     * 校验创建直充订单签名。
     */
    public V1SignatureVerifyResult verifyCreate(V1RechargeCreateOrderRequest request, String signKey) {
        Objects.requireNonNull(request, "request must not be null");
        TreeMap<String, Object> params = new TreeMap<>();
        params.put("account", request.account());

        params.put("nonce_str", request.nonceStr());
        params.put("notify_url", request.notifyUrl());
        params.put("num", request.num());
        params.put("out_order_sn", request.outOrderSn());
        params.put("product_id", request.productId());
        params.put("timestamp", request.timestamp());
        return verify(params, request.sign(), signKey, CREATE);
    }

    /**
     * 校验直充查单签名。
     */
    public V1SignatureVerifyResult verifyQuery(V1RechargeOrderQueryRequest request, String signKey) {
        Objects.requireNonNull(request, "request must not be null");
        TreeMap<String, Object> params = queryLikeParams(request.outOrderSn(), request.nonceStr(), request.timestamp());
        return verify(params, request.sign(), signKey, QUERY);
    }

    /**
     * 校验作废订单签名。
     */
    public V1SignatureVerifyResult verifyInvalid(V1RechargeOrderInvalidRequest request, String signKey) {
        Objects.requireNonNull(request, "request must not be null");
        TreeMap<String, Object> params = queryLikeParams(request.outOrderSn(), request.nonceStr(), request.timestamp());
        return verify(params, request.sign(), signKey, INVALID);
    }

    private V1SignatureVerifyResult verify(TreeMap<String,Object> params, String actualSign, String signKey,
                                           V12MessageSignatures.Operation operation) {
        if (signKey == null) return new V1SignatureVerifyResult(false, "", "");
        params.put("sign", actualSign);
        boolean valid = V12MessageSignatures.verify(operation, params, signKey);
        // 不向调用者暴露含PII的stringA或计算出的认证码，避免异常或日志泄露。
        return new V1SignatureVerifyResult(valid, "", "");
    }

    private static TreeMap<String, Object> queryLikeParams(String outOrderSn, String nonceStr, String timestamp) {
        TreeMap<String, Object> params = new TreeMap<>();

        params.put("nonce_str", nonceStr);
        params.put("out_order_sn", outOrderSn);
        params.put("timestamp", timestamp);
        return params;
    }

}
