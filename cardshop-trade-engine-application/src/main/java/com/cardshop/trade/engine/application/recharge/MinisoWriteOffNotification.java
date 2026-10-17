package com.cardshop.trade.engine.application.recharge;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.Objects;

/**
 * 已验签的核销事件；字段对应0021 COMMENT，参见docs/交行直充业务系统详细设计.md「9.5」。
 *
 * @param couponCode 名创券码
 * @param useTime 核销时间
 */
public record MinisoWriteOffNotification(String couponCode, LocalDateTime useTime) {
    public MinisoWriteOffNotification {
        Objects.requireNonNull(couponCode);
        Objects.requireNonNull(useTime);
    }

    public String eventId() {
        return "miniso-redeemed-" + digest("MINISO:REDEEMED:" + couponCode);
    }

    public static String digest(String value) {
        try {
            return HexFormat.of()
                    .formatHex(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 unavailable", ex);
        }
    }

    @Override
    public String toString() {
        return "MinisoWriteOffNotification[eventId=" + eventId() + "]";
    }
}
