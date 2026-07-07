package com.cardshop.trade.engine.api.v1.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * `/v1/cardapi` timestamp 偏差校验器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.2 V1 请求 DTO 和字段来源」
 * 与「20.13.3 SM4 解密与 SM3 验签」。兼容 10 位秒级时间戳和 13 位毫秒级时间戳。
 */
@Component
public class V1TimestampValidator {

    private static final Logger log = LoggerFactory.getLogger(V1TimestampValidator.class);

    private final V1TimestampSettings settings;
    private final Clock clock;

    @Autowired
    public V1TimestampValidator(V1TimestampSettings settings) {
        this(settings, Clock.systemUTC());
    }

    public V1TimestampValidator(V1TimestampSettings settings, Clock clock) {
        this.settings = settings;
        this.clock = clock;
    }

    /**
     * 校验调用方时间戳是否在允许偏差范围内。
     */
    public V1TimestampVerifyResult validate(String timestampText) {
        Instant timestamp = parse(timestampText);
        if (timestamp == null) {
            return V1TimestampVerifyResult.invalid("TIMESTAMP_INVALID");
        }
        Instant now = clock.instant();
        Duration distance = timestamp.isAfter(now)
                ? Duration.between(now, timestamp)
                : Duration.between(timestamp, now);
        if (distance.compareTo(settings.allowedSkew()) > 0) {
            return V1TimestampVerifyResult.invalid("TIMESTAMP_OUT_OF_RANGE");
        }
        return V1TimestampVerifyResult.valid(timestamp);
    }

    private static Instant parse(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        String trimmed = value.trim();
        if (!trimmed.matches("\\d{10}|\\d{13}")) {
            return null;
        }
        try {
            if (trimmed.length() == 10) {
                return Instant.ofEpochSecond(Long.parseLong(trimmed));
            }
            return Instant.ofEpochMilli(Long.parseLong(trimmed));
        } catch (NumberFormatException ex) {
            log.error("V1 timestamp解析失败 timestampLength={}", trimmed.length(), ex);
            return null;
        }
    }
}
