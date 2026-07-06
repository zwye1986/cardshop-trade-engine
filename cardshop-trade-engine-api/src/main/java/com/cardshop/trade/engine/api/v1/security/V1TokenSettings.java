package com.cardshop.trade.engine.api.v1.security;

import java.time.Duration;

/**
 * `/v1/cardapi` Token 校验所需的最小配置视图。
 */
public record V1TokenSettings(
        String jwtSecret,
        String tokenClaimKey,
        Duration clockSkew,
        String tokenRedisKeyPrefix,
        Duration tokenTtl) {

    public V1TokenSettings(String jwtSecret, String tokenClaimKey, Duration clockSkew) {
        this(jwtSecret, tokenClaimKey, clockSkew, "token:app_id:", Duration.ofHours(3));
    }

    public V1TokenSettings {
        jwtSecret = jwtSecret == null ? "" : jwtSecret;
        tokenClaimKey = tokenClaimKey == null || tokenClaimKey.isBlank() ? "order_token_key:" : tokenClaimKey.trim();
        clockSkew = clockSkew == null ? Duration.ofSeconds(30) : clockSkew;
        tokenRedisKeyPrefix = tokenRedisKeyPrefix == null || tokenRedisKeyPrefix.isBlank()
                ? "token:app_id:"
                : tokenRedisKeyPrefix.trim();
        tokenTtl = tokenTtl == null || tokenTtl.isNegative() || tokenTtl.isZero()
                ? Duration.ofHours(3)
                : tokenTtl;
    }
}
