package com.cardshop.trade.engine.application.v1;

import java.time.Duration;
import java.util.Objects;

/**
 * V1 商户安全资料进程内短 TTL 缓存参数。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」和
 * 「11. 配置中心、部署与健康检查」。用于降低下单 prepare 路径对 {@code mc_ext_depart}
 * 的每单直读，避免与异步落库争抢 Hikari。
 */
public record V1MerchantProfileCacheSettings(
        boolean enabled,
        Duration ttl,
        Duration negativeTtl,
        int maximumSize) {

    public V1MerchantProfileCacheSettings {
        Objects.requireNonNull(ttl, "ttl must not be null");
        Objects.requireNonNull(negativeTtl, "negativeTtl must not be null");
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("ttl must be positive");
        }
        if (negativeTtl.isNegative() || negativeTtl.isZero()) {
            throw new IllegalArgumentException("negativeTtl must be positive");
        }
        if (maximumSize <= 0) {
            throw new IllegalArgumentException("maximumSize must be positive");
        }
    }
}
