package com.cardshop.trade.engine.application.v1;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * V1 商户安全资料短 TTL 本地缓存测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」商户安全资料读取约定：
 * 下单 prepare 路径缓存 {@code findByAppIdAndOrgCode}/{@code findByOrgCode}，发 token 的
 * {@code findByAppIdAndAppSecret} 不缓存。
 */
class CachedV1MerchantProfilePortTest {

    private static final Instant NOW = Instant.parse("2026-07-17T07:00:00Z");

    @Test
    void findByAppIdAndOrgCodeCachesHitWithinTtl() {
        CountingMerchantProfilePort delegate = new CountingMerchantProfilePort(
                Optional.of(profile("app-A39", "A39")));
        MutableClock clock = new MutableClock(NOW);
        RecordingObserver observer = new RecordingObserver();
        CachedV1MerchantProfilePort port = new CachedV1MerchantProfilePort(
                delegate,
                new V1MerchantProfileCacheSettings(true, Duration.ofSeconds(60), Duration.ofSeconds(10), 1024),
                clock,
                observer);

        Optional<V1MerchantProfile> first = port.findByAppIdAndOrgCode("app-A39", "A39");
        Optional<V1MerchantProfile> second = port.findByAppIdAndOrgCode("app-A39", "A39");

        assertThat(first).contains(profile("app-A39", "A39"));
        assertThat(second).contains(profile("app-A39", "A39"));
        assertThat(delegate.appOrgCalls.get()).isEqualTo(1);
        assertThat(observer.results).containsExactly("miss:app_org", "hit:app_org");
    }

    @Test
    void findByAppIdAndOrgCodeReloadsAfterTtlExpires() {
        CountingMerchantProfilePort delegate = new CountingMerchantProfilePort(
                Optional.of(profile("app-A39", "A39")));
        MutableClock clock = new MutableClock(NOW);
        CachedV1MerchantProfilePort port = new CachedV1MerchantProfilePort(
                delegate,
                new V1MerchantProfileCacheSettings(true, Duration.ofSeconds(60), Duration.ofSeconds(10), 1024),
                clock);

        port.findByAppIdAndOrgCode("app-A39", "A39");
        clock.advance(Duration.ofSeconds(61));
        port.findByAppIdAndOrgCode("app-A39", "A39");

        assertThat(delegate.appOrgCalls.get()).isEqualTo(2);
    }

    @Test
    void findByAppIdAndOrgCodeCachesNegativeLookupWithNegativeTtl() {
        CountingMerchantProfilePort delegate = new CountingMerchantProfilePort(Optional.empty());
        MutableClock clock = new MutableClock(NOW);
        RecordingObserver observer = new RecordingObserver();
        CachedV1MerchantProfilePort port = new CachedV1MerchantProfilePort(
                delegate,
                new V1MerchantProfileCacheSettings(true, Duration.ofSeconds(60), Duration.ofSeconds(10), 1024),
                clock,
                observer);

        assertThat(port.findByAppIdAndOrgCode("missing", "A39")).isEmpty();
        assertThat(port.findByAppIdAndOrgCode("missing", "A39")).isEmpty();
        assertThat(delegate.appOrgCalls.get()).isEqualTo(1);
        assertThat(observer.results).containsExactly("miss:app_org", "negative:app_org");

        clock.advance(Duration.ofSeconds(11));
        assertThat(port.findByAppIdAndOrgCode("missing", "A39")).isEmpty();
        assertThat(delegate.appOrgCalls.get()).isEqualTo(2);
    }

    @Test
    void findByOrgCodeUsesSeparateCacheKey() {
        CountingMerchantProfilePort delegate = new CountingMerchantProfilePort(
                Optional.of(profile("app-A39", "A39")));
        MutableClock clock = new MutableClock(NOW);
        RecordingObserver observer = new RecordingObserver();
        CachedV1MerchantProfilePort port = new CachedV1MerchantProfilePort(
                delegate,
                new V1MerchantProfileCacheSettings(true, Duration.ofSeconds(60), Duration.ofSeconds(10), 1024),
                clock,
                observer);

        port.findByOrgCode("A39");
        port.findByOrgCode("A39");
        port.findByAppIdAndOrgCode("app-A39", "A39");

        assertThat(delegate.orgCalls.get()).isEqualTo(1);
        assertThat(delegate.appOrgCalls.get()).isEqualTo(1);
        assertThat(observer.results).containsExactly("miss:org", "hit:org", "miss:app_org");
    }

    @Test
    void findByAppIdAndAppSecretNeverCaches() {
        CountingMerchantProfilePort delegate = new CountingMerchantProfilePort(
                Optional.of(profile("app-A39", "A39")));
        MutableClock clock = new MutableClock(NOW);
        CachedV1MerchantProfilePort port = new CachedV1MerchantProfilePort(
                delegate,
                new V1MerchantProfileCacheSettings(true, Duration.ofSeconds(60), Duration.ofSeconds(10), 1024),
                clock);

        port.findByAppIdAndAppSecret("app-A39", "secret");
        port.findByAppIdAndAppSecret("app-A39", "secret");

        assertThat(delegate.appSecretCalls.get()).isEqualTo(2);
    }

    @Test
    void disabledCacheAlwaysDelegates() {
        CountingMerchantProfilePort delegate = new CountingMerchantProfilePort(
                Optional.of(profile("app-A39", "A39")));
        MutableClock clock = new MutableClock(NOW);
        CachedV1MerchantProfilePort port = new CachedV1MerchantProfilePort(
                delegate,
                new V1MerchantProfileCacheSettings(false, Duration.ofSeconds(60), Duration.ofSeconds(10), 1024),
                clock);

        port.findByAppIdAndOrgCode("app-A39", "A39");
        port.findByAppIdAndOrgCode("app-A39", "A39");

        assertThat(delegate.appOrgCalls.get()).isEqualTo(2);
    }

    private static V1MerchantProfile profile(String appId, String orgCode) {
        return new V1MerchantProfile(appId, orgCode, "AES-KEY", true, 54L);
    }

    private static final class CountingMerchantProfilePort implements V1MerchantProfilePort {

        private final Optional<V1MerchantProfile> profile;
        private final AtomicInteger appOrgCalls = new AtomicInteger();
        private final AtomicInteger orgCalls = new AtomicInteger();
        private final AtomicInteger appSecretCalls = new AtomicInteger();

        private CountingMerchantProfilePort(Optional<V1MerchantProfile> profile) {
            this.profile = profile;
        }

        @Override
        public Optional<V1MerchantProfile> findByAppIdAndOrgCode(String appId, String orgCode) {
            appOrgCalls.incrementAndGet();
            return profile;
        }

        @Override
        public Optional<V1MerchantProfile> findByOrgCode(String orgCode) {
            orgCalls.incrementAndGet();
            return profile;
        }

        @Override
        public Optional<V1MerchantProfile> findByAppIdAndAppSecret(String appId, String appSecret) {
            appSecretCalls.incrementAndGet();
            return profile;
        }
    }

    private static final class RecordingObserver implements V1MerchantProfileCacheObserver {

        private final List<String> results = new ArrayList<>();

        @Override
        public void recordLookup(String result, String lookupType) {
            results.add(result + ":" + lookupType);
        }
    }

    private static final class MutableClock extends Clock {

        private Instant current;

        private MutableClock(Instant current) {
            this.current = current;
        }

        private void advance(Duration duration) {
            current = current.plus(duration);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return current;
        }
    }
}
