package com.cardshop.trade.engine.simulator.a39;

import example.v12.crypto.HmacSm3SignUtils;
import java.util.Map;

/**
 * 保存token/signKey，禁止把token截断为密钥；参见详细设计「交行开放平台通知认证修订 2026-10-19」。
 * keyId仅兼容旧直连测试凭证；最新getToken不返回该字段，业务请求签名也不依赖它。
 */
public record Credentials(String token, String keyId, String signKey) {
    public Credentials {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("V1.2 token missing");
        }
        HmacSm3SignUtils.sign(HmacSm3SignUtils.Operation.QUERY, Map.of(), signKey);
    }
    @Override public String toString() { return "Credentials[redacted]"; }
}
