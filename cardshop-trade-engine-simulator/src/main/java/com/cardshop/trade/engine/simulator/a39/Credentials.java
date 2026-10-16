package com.cardshop.trade.engine.simulator.a39;

import example.v12.crypto.HmacSm3SignUtils;
import java.util.Map;

/** 完整保存签发结果，禁止把token截断为密钥；参见详细设计3.1。 */
public record Credentials(String token, String keyId, String signKey) {
    public Credentials {
        if (token == null || token.isBlank() || keyId == null || keyId.isBlank()) {
            throw new IllegalArgumentException("V1.2 token/keyId missing");
        }
        HmacSm3SignUtils.sign(HmacSm3SignUtils.Operation.QUERY, Map.of(), signKey);
    }
    @Override public String toString() { return "Credentials[redacted]"; }
}
