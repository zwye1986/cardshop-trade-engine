package com.cardshop.trade.engine.api.v1.security;

import java.time.Instant;

/**
 * V1 timestamp 校验结果。
 */
public record V1TimestampVerifyResult(boolean valid, String reason, Instant timestamp) {

    public static V1TimestampVerifyResult valid(Instant timestamp) {
        return new V1TimestampVerifyResult(true, "OK", timestamp);
    }

    public static V1TimestampVerifyResult invalid(String reason) {
        return new V1TimestampVerifyResult(false, reason, null);
    }
}
