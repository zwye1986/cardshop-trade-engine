package com.cardshop.trade.engine.api.v1.security;

import java.time.Duration;

/**
 * V1 timestamp 校验配置。
 */
public record V1TimestampSettings(Duration allowedSkew) {

    public V1TimestampSettings {
        if (allowedSkew == null || allowedSkew.isNegative() || allowedSkew.isZero()) {
            allowedSkew = Duration.ofSeconds(300);
        }
    }
}
