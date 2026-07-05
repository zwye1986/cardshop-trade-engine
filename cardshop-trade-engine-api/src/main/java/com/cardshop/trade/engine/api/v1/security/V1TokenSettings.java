package com.cardshop.trade.engine.api.v1.security;

import java.time.Duration;

/**
 * `/v1/cardapi` Token 校验所需的最小配置视图。
 */
public record V1TokenSettings(String jwtSecret, String tokenClaimKey, Duration clockSkew) {

    public V1TokenSettings {
        jwtSecret = jwtSecret == null ? "" : jwtSecret;
        tokenClaimKey = tokenClaimKey == null || tokenClaimKey.isBlank() ? "order_token_key:" : tokenClaimKey.trim();
        clockSkew = clockSkew == null ? Duration.ofSeconds(30) : clockSkew;
    }
}
