package com.cardshop.trade.engine.application.v1.crypto;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * V1.2独立签名凭证；对应docs/交行直充业务系统详细设计.md「3.1」及「V1.2 密钥存储实施约束」。
 * 请求过期不意味着历史回调密钥到期；持久化层另行管理撤销。严禁把本对象序列化到日志或订单消息。
 */
public record V12SigningCredential(String keyId, String appId, String orgCode,
                                   String signKey, Instant requestExpiresAt) {
    private static final int KEY_LENGTH = 32;
    private static final SecureRandom RANDOM = new SecureRandom();

    public V12SigningCredential {
        Objects.requireNonNull(keyId); Objects.requireNonNull(appId); Objects.requireNonNull(orgCode);
        Objects.requireNonNull(signKey); Objects.requireNonNull(requestExpiresAt);
        if (keyId.isBlank() || appId.isBlank() || orgCode.isBlank()) {
            throw new IllegalArgumentException("credential identity must not be blank");
        }
        byte[] bytes = Base64.getUrlDecoder().decode(signKey);
        try {
            if (bytes.length != KEY_LENGTH || !Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(signKey)) {
                throw new IllegalArgumentException("signKey must encode exactly 32 bytes without padding");
            }
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }

    /** 每次签发使用独立随机密钥和keyId，不接收token/aesKey/app_secret作为随机种子。 */
    public static V12SigningCredential generate(String appId, String orgCode, Instant expiresAt) {
        byte[] bytes = new byte[KEY_LENGTH];
        RANDOM.nextBytes(bytes);
        try {
            return new V12SigningCredential(UUID.randomUUID().toString(), appId, orgCode,
                    Base64.getUrlEncoder().withoutPadding().encodeToString(bytes), expiresAt);
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }

    public boolean validForRequestAt(Instant instant) {
        return instant.isBefore(requestExpiresAt);
    }

    @Override public String toString() {
        return "V12SigningCredential[keyId=" + keyId + ", signKey=REDACTED]";
    }
}
