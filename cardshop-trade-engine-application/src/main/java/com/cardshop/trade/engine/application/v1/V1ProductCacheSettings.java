package com.cardshop.trade.engine.application.v1;

import java.time.Duration;

/**
 * V1 父商品选品缓存的逻辑过期与单飞刷新参数。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * 业务允许改价后最多 30 分钟继续按旧价成交，因此软过期、硬过期和物理 TTL 必须严格递增；
 * 任一参数非法都应在应用启动时失败，不能静默退化成每单读取 MySQL。
 */
public record V1ProductCacheSettings(
        Duration refreshAfter,
        Duration maxStaleAge,
        Duration physicalTtl,
        Duration refreshLockTtl,
        Duration refreshWaitTimeout,
        Duration negativeCacheTtl) {

    public V1ProductCacheSettings {
        requirePositive(refreshAfter, "refreshAfter");
        requirePositive(maxStaleAge, "maxStaleAge");
        requirePositive(physicalTtl, "physicalTtl");
        requirePositive(refreshLockTtl, "refreshLockTtl");
        requirePositive(refreshWaitTimeout, "refreshWaitTimeout");
        requirePositive(negativeCacheTtl, "negativeCacheTtl");
        if (refreshAfter.compareTo(maxStaleAge) >= 0) {
            throw new IllegalArgumentException("refreshAfter must be less than maxStaleAge");
        }
        if (maxStaleAge.compareTo(physicalTtl) >= 0) {
            throw new IllegalArgumentException("maxStaleAge must be less than physicalTtl");
        }
    }

    private static void requirePositive(Duration value, String field) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
    }
}
