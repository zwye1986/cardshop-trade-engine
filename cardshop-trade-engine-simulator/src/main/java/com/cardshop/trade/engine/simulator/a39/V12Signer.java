package com.cardshop.trade.engine.simulator.a39;

import example.v12.crypto.HmacSm3SignUtils;
import example.v12.crypto.HmacSm3SignUtils.Operation;
import java.util.Map;

/** 模拟器使用对外源码验证V1.2，参见详细设计3.1；aesKey只留给SM4。 */
public final class V12Signer {
    public Signature signCreateOrder(Map<String, ?> payload, String signKey) {
        return sign(Operation.CREATE, payload, signKey);
    }
    public Signature signQueryOrder(Map<String, ?> payload, String signKey) {
        return sign(Operation.QUERY, payload, signKey);
    }
    public Signature signCallback(Map<String, ?> payload, String signKey) {
        return sign(callbackOperation(payload), payload, signKey);
    }
    public boolean verifyCallback(Map<String, ?> payload, String keyId, String signKey) {
        return HmacSm3SignUtils.verifyCallback(callbackOperation(payload), payload,
                keyId, signKey);
    }
    private static Operation callbackOperation(Map<String, ?> payload) {
        return payload.containsKey("order_status") ? Operation.ORDER_CALLBACK : Operation.WRITEOFF_CALLBACK;
    }
    private static Signature sign(Operation operation, Map<String, ?> payload, String key) {
        return new Signature(HmacSm3SignUtils.canonicalize(operation, payload),
                HmacSm3SignUtils.sign(operation, payload, key));
    }
    public record Signature(String source, String sign) { }
}
