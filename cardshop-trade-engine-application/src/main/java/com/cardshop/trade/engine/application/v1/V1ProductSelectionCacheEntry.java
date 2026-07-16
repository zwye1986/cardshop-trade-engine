package com.cardshop.trade.engine.application.v1;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Redis 中父商品选品快照的 application 层表示。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * 时间字段使用绝对时间，避免各实例基于本地写入时刻重新计算导致同一快照年龄不一致。
 *
 * @param selection 完整选品或负缓存业务失败结果
 * @param cacheKind 正常快照或负缓存
 * @param loadedAt MySQL 事实源读取完成时间
 * @param refreshAfter 进入软过期的绝对时间
 * @param expireAt 达到硬过期、不得继续按旧价成交的绝对时间
 */
public record V1ProductSelectionCacheEntry(
        V1ProductSelection selection,
        CacheKind cacheKind,
        Instant loadedAt,
        Instant refreshAfter,
        Instant expireAt) {

    public V1ProductSelectionCacheEntry {
        Objects.requireNonNull(selection, "selection must not be null");
        Objects.requireNonNull(cacheKind, "cacheKind must not be null");
        Objects.requireNonNull(loadedAt, "loadedAt must not be null");
        Objects.requireNonNull(refreshAfter, "refreshAfter must not be null");
        Objects.requireNonNull(expireAt, "expireAt must not be null");
        if (refreshAfter.isBefore(loadedAt)) {
            throw new IllegalArgumentException("refreshAfter must not be before loadedAt");
        }
        if (expireAt.isBefore(refreshAfter)) {
            throw new IllegalArgumentException("expireAt must not be before refreshAfter");
        }
    }

    public boolean negative() {
        return cacheKind == CacheKind.NEGATIVE;
    }

    public Duration age(Instant now) {
        if (now == null || now.isBefore(loadedAt)) {
            return Duration.ZERO;
        }
        return Duration.between(loadedAt, now);
    }

    public enum CacheKind {
        SNAPSHOT,
        NEGATIVE
    }
}
