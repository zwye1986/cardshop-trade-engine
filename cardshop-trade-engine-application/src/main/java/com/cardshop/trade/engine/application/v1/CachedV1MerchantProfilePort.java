package com.cardshop.trade.engine.application.v1;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * V1 商户安全资料短 TTL 本地缓存装饰器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。对
 * {@link #findByAppIdAndOrgCode(String, String)} 与 {@link #findByOrgCode(String)} 做进程内缓存，
 * 降低压测/高峰下每单直打 {@code mc_ext_depart} 与异步落库争抢 Hikari 的概率。
 * {@link #findByAppIdAndAppSecret(String, String)} 不缓存，避免把密钥拼进缓存键。
 *
 * <p>缓存条目可包含 {@code aesKey}，仅存于本进程内存；禁止写入 Redis，也禁止写入日志。
 * 观测端口通过 {@link Supplier} 延迟解析，避免基础设施 Bean 早于 Micrometer 指标 Bean 创建时永久落到 noop。
 */
public final class CachedV1MerchantProfilePort implements V1MerchantProfilePort {

    static final String LOOKUP_APP_ORG = "app_org";
    static final String LOOKUP_ORG = "org";
    static final String RESULT_HIT = "hit";
    static final String RESULT_NEGATIVE = "negative";
    static final String RESULT_MISS = "miss";

    private final V1MerchantProfilePort delegate;
    private final V1MerchantProfileCacheSettings settings;
    private final Clock clock;
    private final Supplier<V1MerchantProfileCacheObserver> observerSupplier;
    private final ConcurrentHashMap<String, CacheEntry> byAppOrg = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CacheEntry> byOrg = new ConcurrentHashMap<>();

    public CachedV1MerchantProfilePort(
            V1MerchantProfilePort delegate,
            V1MerchantProfileCacheSettings settings,
            Clock clock) {
        this(delegate, settings, clock, V1MerchantProfileCacheObserver.noop());
    }

    public CachedV1MerchantProfilePort(
            V1MerchantProfilePort delegate,
            V1MerchantProfileCacheSettings settings,
            Clock clock,
            V1MerchantProfileCacheObserver observer) {
        this(delegate, settings, clock, () -> observer == null
                ? V1MerchantProfileCacheObserver.noop()
                : observer);
    }

    public CachedV1MerchantProfilePort(
            V1MerchantProfilePort delegate,
            V1MerchantProfileCacheSettings settings,
            Clock clock,
            Supplier<V1MerchantProfileCacheObserver> observerSupplier) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        Objects.requireNonNull(observerSupplier, "observerSupplier must not be null");
        this.observerSupplier = () -> {
            V1MerchantProfileCacheObserver observer = observerSupplier.get();
            return observer == null ? V1MerchantProfileCacheObserver.noop() : observer;
        };
    }

    @Override
    public Optional<V1MerchantProfile> findByAppIdAndOrgCode(String appId, String orgCode) {
        if (!settings.enabled()) {
            return delegate.findByAppIdAndOrgCode(appId, orgCode);
        }
        String key = appOrgKey(appId, orgCode);
        Optional<V1MerchantProfile> cached = readFresh(byAppOrg, key);
        if (cached != null) {
            recordLookup(cached.isPresent() ? RESULT_HIT : RESULT_NEGATIVE, LOOKUP_APP_ORG);
            return cached;
        }
        Optional<V1MerchantProfile> loaded = delegate.findByAppIdAndOrgCode(appId, orgCode);
        store(byAppOrg, key, loaded);
        recordLookup(RESULT_MISS, LOOKUP_APP_ORG);
        return loaded;
    }

    @Override
    public Optional<V1MerchantProfile> findByOrgCode(String orgCode) {
        if (!settings.enabled()) {
            return delegate.findByOrgCode(orgCode);
        }
        String key = orgKey(orgCode);
        Optional<V1MerchantProfile> cached = readFresh(byOrg, key);
        if (cached != null) {
            recordLookup(cached.isPresent() ? RESULT_HIT : RESULT_NEGATIVE, LOOKUP_ORG);
            return cached;
        }
        Optional<V1MerchantProfile> loaded = delegate.findByOrgCode(orgCode);
        store(byOrg, key, loaded);
        recordLookup(RESULT_MISS, LOOKUP_ORG);
        return loaded;
    }

    @Override
    public Optional<V1MerchantProfile> findByAppIdAndAppSecret(String appId, String appSecret) {
        // 发 token 路径频率低，且密钥不能进入缓存键；始终直读下游。
        return delegate.findByAppIdAndAppSecret(appId, appSecret);
    }

    private void recordLookup(String result, String lookupType) {
        observerSupplier.get().recordLookup(result, lookupType);
    }

    private Optional<V1MerchantProfile> readFresh(ConcurrentHashMap<String, CacheEntry> map, String key) {
        CacheEntry entry = map.get(key);
        if (entry == null) {
            return null;
        }
        if (clock.millis() >= entry.expireAtEpochMs()) {
            map.remove(key, entry);
            return null;
        }
        return entry.value();
    }

    private void store(ConcurrentHashMap<String, CacheEntry> map, String key, Optional<V1MerchantProfile> value) {
        Duration ttl = value.isPresent() ? settings.ttl() : settings.negativeTtl();
        long expireAt = clock.millis() + Math.max(1L, ttl.toMillis());
        enforceMaximumSize(map);
        map.put(key, new CacheEntry(value, expireAt));
    }

    private void enforceMaximumSize(ConcurrentHashMap<String, CacheEntry> map) {
        if (map.size() < settings.maximumSize()) {
            return;
        }
        long now = clock.millis();
        map.entrySet().removeIf(entry -> now >= entry.getValue().expireAtEpochMs());
        if (map.size() >= settings.maximumSize()) {
            // 仍超限时清空该分片，避免无界增长；短 TTL 场景可接受冷启动一次。
            map.clear();
        }
    }

    private static String appOrgKey(String appId, String orgCode) {
        return "appOrg|" + nullToEmpty(appId) + "|" + nullToEmpty(orgCode);
    }

    private static String orgKey(String orgCode) {
        return "org|" + nullToEmpty(orgCode);
    }

    private static String nullToEmpty(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value.trim();
    }

    private record CacheEntry(Optional<V1MerchantProfile> value, long expireAtEpochMs) {
    }
}
