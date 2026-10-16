package com.cardshop.trade.engine.application.v1.crypto;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * V1.2独立签名凭证；对应docs/交行直充业务系统详细设计.md「3.1」及「V1.2 密钥存储实施约束」。
 * 商户级密钥没有token到期时间或订单归属；持久化层分别管理当前版本、历史版本和撤销。
 * 严禁把本对象序列化到日志或订单消息。
 */
public record V12SigningCredential(String keyId, String orgCode, String signKey) {
    private static final int KEY_LENGTH = 32;
    private static final SecureRandom RANDOM = new SecureRandom();

    public V12SigningCredential {
        Objects.requireNonNull(keyId); Objects.requireNonNull(orgCode); Objects.requireNonNull(signKey);
        if (keyId.isBlank() || orgCode.isBlank()) {
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

    /** 仅首次初始化或专门轮换调用；普通token刷新必须复用当前版本，见详细设计3.1。 */
    public static V12SigningCredential generate(String orgCode) {
        byte[] bytes = new byte[KEY_LENGTH];
        RANDOM.nextBytes(bytes);
        try {
            return new V12SigningCredential(UUID.randomUUID().toString(), orgCode,
                    Base64.getUrlEncoder().withoutPadding().encodeToString(bytes));
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }

    @Override public String toString() {
        return "V12SigningCredential[keyId=" + keyId + ", signKey=REDACTED]";
    }
}
