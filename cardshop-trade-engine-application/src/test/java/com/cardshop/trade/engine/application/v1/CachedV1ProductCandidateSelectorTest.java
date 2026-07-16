package com.cardshop.trade.engine.application.v1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.cardshop.trade.engine.application.v1.V1ProductSelectionCacheEntry.CacheKind;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Redis 优先 V1 商品选品状态机测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。测试重点证明
 * 新鲜缓存不访问 MySQL、软过期允许旧价、硬过期必须刷新，以及同一父商品的锁竞争者不会击穿 MySQL。
 */
class CachedV1ProductCandidateSelectorTest {

    private static final Instant NOW = Instant.parse("2026-07-15T03:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final V1ProductCacheSettings SETTINGS = new V1ProductCacheSettings(
            Duration.ofMinutes(25),
            Duration.ofMinutes(30),
            Duration.ofMinutes(60),
            Duration.ofSeconds(5),
            Duration.ofSeconds(1),
            Duration.ofMinutes(1));

    @Test
    void freshSnapshotDoesNotLoadMysql() {
        FakeCachePort cache = new FakeCachePort(snapshotEntry(oldSelection(), Duration.ofMinutes(10)));
        AtomicInteger mysqlCalls = new AtomicInteger();
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            mysqlCalls.incrementAndGet();
            return newSelection();
        });

        V1ProductSelection selected = selector.select("A39", "80100022", "18100000000");

        assertThat(selected).isEqualTo(oldSelection());
        assertThat(mysqlCalls).hasValue(0);
    }

    @Test
    void softStaleLockLoserUsesOldSnapshot() {
        FakeCachePort cache = new FakeCachePort(snapshotEntry(oldSelection(), Duration.ofMinutes(26)));
        cache.lockToken = Optional.empty();
        AtomicInteger mysqlCalls = new AtomicInteger();
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            mysqlCalls.incrementAndGet();
            return newSelection();
        });

        V1ProductSelection selected = selector.select("A39", "80100022", "18100000000");

        assertThat(selected).isEqualTo(oldSelection());
        assertThat(mysqlCalls).hasValue(0);
        assertThat(cache.awaitCalls).isZero();
    }

    @Test
    void softStaleLockOwnerRefreshesAndUsesNewPrice() {
        FakeCachePort cache = new FakeCachePort(snapshotEntry(oldSelection(), Duration.ofMinutes(26)));
        AtomicInteger mysqlCalls = new AtomicInteger();
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            mysqlCalls.incrementAndGet();
            return newSelection();
        });

        V1ProductSelection selected = selector.select("A39", "80100022", "18100000000");

        assertThat(selected).isEqualTo(newSelection());
        assertThat(mysqlCalls).hasValue(1);
        assertThat(cache.snapshotWrites).isEqualTo(1);
    }

    @Test
    void softStaleRefreshFailureUsesOldSnapshotWithinThirtyMinutes() {
        FakeCachePort cache = new FakeCachePort(snapshotEntry(oldSelection(), Duration.ofMinutes(26)));
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            throw new IllegalStateException("mysql unavailable");
        });

        assertThat(selector.select("A39", "80100022", "18100000000"))
                .isEqualTo(oldSelection());
        assertThat(cache.releaseCalls).isEqualTo(1);
    }

    @Test
    void softStaleRefreshFailureCrossingThirtyMinutesFailsClosed() {
        MutableClock clock = new MutableClock(NOW);
        V1ProductSelectionCacheEntry almostExpired = new V1ProductSelectionCacheEntry(
                oldSelection(),
                CacheKind.SNAPSHOT,
                NOW.minus(SETTINGS.maxStaleAge()).plusSeconds(1),
                NOW.minus(SETTINGS.maxStaleAge()).plusSeconds(1).plus(SETTINGS.refreshAfter()),
                NOW.plusSeconds(1));
        FakeCachePort cache = new FakeCachePort(almostExpired);
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            clock.advance(Duration.ofSeconds(2));
            throw new IllegalStateException("mysql unavailable");
        }, SETTINGS, clock, V1ProductCacheObserver.noop());

        assertThatThrownBy(() -> selector.select("A39", "80100022", "18100000000"))
                .isInstanceOf(V1ProductCacheUnavailableException.class)
                .hasMessageContaining("硬过期");
    }

    @Test
    void hardStaleLockLoserWaitsForNewerSnapshot() {
        V1ProductSelectionCacheEntry oldEntry = snapshotEntry(oldSelection(), Duration.ofMinutes(30));
        FakeCachePort cache = new FakeCachePort(oldEntry);
        cache.lockToken = Optional.empty();
        cache.awaitedEntry = Optional.of(new V1ProductSelectionCacheEntry(
                newSelection(),
                CacheKind.SNAPSHOT,
                NOW,
                NOW.plus(SETTINGS.refreshAfter()),
                NOW.plus(SETTINGS.maxStaleAge())));
        AtomicInteger mysqlCalls = new AtomicInteger();
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            mysqlCalls.incrementAndGet();
            return newSelection();
        });

        V1ProductSelection selected = selector.select("A39", "80100022", "18100000000");

        assertThat(selected).isEqualTo(newSelection());
        assertThat(mysqlCalls).hasValue(0);
        assertThat(cache.awaitCalls).isEqualTo(1);
        assertThat(cache.observedLoadedAt).isEqualTo(oldEntry.loadedAt());
    }

    @Test
    void hardStaleRefreshFailureThrowsSystemBusy() {
        FakeCachePort cache = new FakeCachePort(snapshotEntry(oldSelection(), Duration.ofMinutes(30)));
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            throw new IllegalStateException("mysql unavailable");
        });

        assertThatThrownBy(() -> selector.select("A39", "80100022", "18100000000"))
                .isInstanceOf(V1ProductCacheUnavailableException.class)
                .hasMessageContaining("硬过期")
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void hardStaleLockLoserWaitTimeoutThrowsSystemBusy() {
        FakeCachePort cache = new FakeCachePort(snapshotEntry(oldSelection(), Duration.ofMinutes(30)));
        cache.lockToken = Optional.empty();
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> newSelection());

        assertThatThrownBy(() -> selector.select("A39", "80100022", "18100000000"))
                .isInstanceOf(V1ProductCacheUnavailableException.class)
                .hasMessageContaining("等待刷新超时");
        assertThat(cache.awaitCalls).isEqualTo(1);
    }

    @Test
    void missingSelectionWritesNegativeEntry() {
        FakeCachePort cache = new FakeCachePort(null);
        V1ProductSelection denied = V1ProductSelection.failed(
                "80100022", "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        AtomicInteger mysqlCalls = new AtomicInteger();
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            mysqlCalls.incrementAndGet();
            return denied;
        });

        assertThat(selector.select("A39", "80100022", "18100000000")).isEqualTo(denied);
        assertThat(mysqlCalls).hasValue(1);
        assertThat(cache.negativeWrites).isEqualTo(1);
    }

    @Test
    void negativeEntryAvoidsMysqlUntilItsExpiry() {
        V1ProductSelection denied = V1ProductSelection.failed(
                "80100022", "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        FakeCachePort cache = new FakeCachePort(new V1ProductSelectionCacheEntry(
                denied,
                CacheKind.NEGATIVE,
                NOW.minusSeconds(30),
                NOW.plusSeconds(30),
                NOW.plusSeconds(30)));
        AtomicInteger mysqlCalls = new AtomicInteger();
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            mysqlCalls.incrementAndGet();
            return newSelection();
        });

        assertThat(selector.select("A39", "80100022", "18100000000")).isEqualTo(denied);
        assertThat(mysqlCalls).hasValue(0);
    }

    @Test
    void expiredNegativeEntryReloadsMysql() {
        V1ProductSelection denied = V1ProductSelection.failed(
                "80100022", "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        FakeCachePort cache = new FakeCachePort(new V1ProductSelectionCacheEntry(
                denied,
                CacheKind.NEGATIVE,
                NOW.minus(SETTINGS.negativeCacheTtl()),
                NOW,
                NOW));
        AtomicInteger mysqlCalls = new AtomicInteger();
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            mysqlCalls.incrementAndGet();
            return newSelection();
        });

        assertThat(selector.select("A39", "80100022", "18100000000")).isEqualTo(newSelection());
        assertThat(mysqlCalls).hasValue(1);
    }

    @Test
    void sourceFailureRecordsMysqlFailed() {
        FakeCachePort cache = new FakeCachePort(null);
        RecordingObserver observer = new RecordingObserver();
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            throw new IllegalStateException("mysql unavailable");
        }, SETTINGS, CLOCK, observer);

        assertThatThrownBy(() -> selector.select("A39", "80100022", "18100000000"))
                .isInstanceOf(V1ProductCacheUnavailableException.class);
        assertThat(observer.refreshResults).contains("mysql_failed").doesNotContain("redis_failed");
    }

    @Test
    void snapshotWriteFailureRecordsRedisFailedInsteadOfMysqlFailed() {
        FakeCachePort cache = new FakeCachePort(null);
        cache.snapshotWriteFailure = new IllegalStateException("redis unavailable");
        RecordingObserver observer = new RecordingObserver();
        CachedV1ProductCandidateSelector selector = selector(
                cache, ignored -> newSelection(), SETTINGS, CLOCK, observer);

        assertThatThrownBy(() -> selector.select("A39", "80100022", "18100000000"))
                .isInstanceOf(V1ProductCacheUnavailableException.class);
        assertThat(observer.refreshResults).contains("redis_failed").doesNotContain("mysql_failed");
    }

    @Test
    void negativeWriteFailureReturnsBusinessFailureAndRecordsRedisFailed() {
        V1ProductSelection denied = V1ProductSelection.failed(
                "80100022", "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        FakeCachePort cache = new FakeCachePort(null);
        cache.negativeWriteFailure = new IllegalStateException("redis unavailable");
        RecordingObserver observer = new RecordingObserver();
        CachedV1ProductCandidateSelector selector = selector(
                cache, ignored -> denied, SETTINGS, CLOCK, observer);

        assertThat(selector.select("A39", "80100022", "18100000000")).isEqualTo(denied);
        assertThat(observer.refreshResults).contains("redis_failed").doesNotContain("mysql_failed");
    }

    @Test
    void slowSourceLongerThanInitialLockTtlStillHasSingleMysqlLoader() throws Exception {
        V1ProductCacheSettings shortLockSettings = new V1ProductCacheSettings(
                Duration.ofMinutes(25),
                Duration.ofMinutes(30),
                Duration.ofMinutes(60),
                Duration.ofMillis(90),
                Duration.ofSeconds(1),
                Duration.ofMinutes(1));
        ExpiringLockCachePort cache = new ExpiringLockCachePort(
                snapshotEntry(oldSelection(), Duration.ofMinutes(26)), shortLockSettings.refreshLockTtl());
        AtomicInteger mysqlCalls = new AtomicInteger();
        CountDownLatch firstLoadStarted = new CountDownLatch(1);
        CachedV1ProductCandidateSelector selector = selector(cache, ignored -> {
            mysqlCalls.incrementAndGet();
            firstLoadStarted.countDown();
            try {
                Thread.sleep(250);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("source interrupted", ex);
            }
            return newSelection();
        }, shortLockSettings, CLOCK, V1ProductCacheObserver.noop());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<V1ProductSelection> first = executor.submit(
                    () -> selector.select("A39", "80100022", "18100000000"));
            assertThat(firstLoadStarted.await(1, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(140);

            V1ProductSelection competing = selector.select("A39", "80100022", "18100000001");

            assertThat(competing).isEqualTo(oldSelection());
            assertThat(first.get(2, TimeUnit.SECONDS)).isEqualTo(newSelection());
            assertThat(mysqlCalls).hasValue(1);
            assertThat(cache.renewCalls).isPositive();
        } finally {
            executor.shutdownNow();
        }
    }

    private static CachedV1ProductCandidateSelector selector(
            FakeCachePort cache,
            java.util.function.Function<String, V1ProductSelection> source) {
        return selector(cache, source, SETTINGS, CLOCK, V1ProductCacheObserver.noop());
    }

    private static CachedV1ProductCandidateSelector selector(
            V1ProductSelectionCachePort cache,
            java.util.function.Function<String, V1ProductSelection> source,
            V1ProductCacheSettings settings,
            Clock clock,
            V1ProductCacheObserver observer) {
        return new CachedV1ProductCandidateSelector(
                cache,
                (orgCode, productId, account) -> source.apply(productId),
                settings,
                clock,
                observer);
    }

    private static V1ProductSelectionCacheEntry snapshotEntry(
            V1ProductSelection selection,
            Duration age) {
        Instant loadedAt = NOW.minus(age);
        return new V1ProductSelectionCacheEntry(
                selection,
                CacheKind.SNAPSHOT,
                loadedAt,
                loadedAt.plus(SETTINGS.refreshAfter()),
                loadedAt.plus(SETTINGS.maxStaleAge()));
    }

    private static V1ProductSelection oldSelection() {
        return V1ProductSelection.passed(
                "80100022", "80100023", "MINISO", "名创直充100", "codekey",
                1, 10000L, 10000L, 9000L, 1L);
    }

    private static V1ProductSelection newSelection() {
        return V1ProductSelection.passed(
                "80100022", "80100023", "MINISO", "名创直充100", "codekey",
                1, 12000L, 10000L, 9000L, 1L);
    }

    private static final class FakeCachePort implements V1ProductSelectionCachePort {

        private V1ProductSelectionCacheEntry entry;
        private Optional<String> lockToken = Optional.of("lock-token");
        private Optional<V1ProductSelectionCacheEntry> awaitedEntry = Optional.empty();
        private int snapshotWrites;
        private int negativeWrites;
        private int releaseCalls;
        private int awaitCalls;
        private Instant observedLoadedAt;
        private RuntimeException snapshotWriteFailure;
        private RuntimeException negativeWriteFailure;

        private FakeCachePort(V1ProductSelectionCacheEntry entry) {
            this.entry = entry;
        }

        @Override
        public Optional<V1ProductSelectionCacheEntry> find(String orgCode, String productId) {
            return Optional.ofNullable(entry);
        }

        @Override
        public Optional<String> tryAcquireRefreshLock(String orgCode, String productId) {
            return lockToken;
        }

        @Override
        public boolean writeSnapshotIfLockOwned(
                String orgCode,
                String productId,
                String token,
                V1ProductSelection selection,
                Instant loadedAt) {
            if (snapshotWriteFailure != null) {
                throw snapshotWriteFailure;
            }
            snapshotWrites++;
            entry = new V1ProductSelectionCacheEntry(
                    selection,
                    CacheKind.SNAPSHOT,
                    loadedAt,
                    loadedAt.plus(SETTINGS.refreshAfter()),
                    loadedAt.plus(SETTINGS.maxStaleAge()));
            return true;
        }

        @Override
        public boolean writeNegativeIfLockOwned(
                String orgCode,
                String productId,
                String token,
                V1ProductSelection selection,
                Instant loadedAt) {
            if (negativeWriteFailure != null) {
                throw negativeWriteFailure;
            }
            negativeWrites++;
            entry = new V1ProductSelectionCacheEntry(
                    selection,
                    CacheKind.NEGATIVE,
                    loadedAt,
                    loadedAt.plus(SETTINGS.negativeCacheTtl()),
                    loadedAt.plus(SETTINGS.negativeCacheTtl()));
            return true;
        }

        @Override
        public void releaseRefreshLock(String orgCode, String productId, String lockToken) {
            releaseCalls++;
        }

        @Override
        public Optional<V1ProductSelectionCacheEntry> awaitNewerEntry(
                String orgCode,
                String productId,
                Instant observedLoadedAt) {
            awaitCalls++;
            this.observedLoadedAt = observedLoadedAt;
            return awaitedEntry;
        }
    }

    private static final class RecordingObserver implements V1ProductCacheObserver {

        private final List<String> refreshResults = new ArrayList<>();

        @Override
        public void recordCacheResult(String result, Duration age) {
        }

        @Override
        public synchronized void recordRefresh(String result, Duration duration) {
            refreshResults.add(result);
        }

        @Override
        public void recordMysqlLoad() {
        }
    }

    private static final class MutableClock extends Clock {

        private Instant current;

        private MutableClock(Instant current) {
            this.current = current;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public synchronized Instant instant() {
            return current;
        }

        private synchronized void advance(Duration duration) {
            current = current.plus(duration);
        }
    }

    private static final class ExpiringLockCachePort implements V1ProductSelectionCachePort {

        private final Duration lockTtl;
        private V1ProductSelectionCacheEntry entry;
        private String token;
        private long expiresAtNanos;
        private int renewCalls;

        private ExpiringLockCachePort(V1ProductSelectionCacheEntry entry, Duration lockTtl) {
            this.entry = entry;
            this.lockTtl = lockTtl;
        }

        @Override
        public synchronized Optional<V1ProductSelectionCacheEntry> find(String orgCode, String productId) {
            return Optional.ofNullable(entry);
        }

        @Override
        public synchronized Optional<String> tryAcquireRefreshLock(String orgCode, String productId) {
            long now = System.nanoTime();
            if (token != null && now < expiresAtNanos) {
                return Optional.empty();
            }
            token = "token-" + now;
            expiresAtNanos = now + lockTtl.toNanos();
            return Optional.of(token);
        }

        @Override
        public synchronized boolean renewRefreshLock(String orgCode, String productId, String lockToken) {
            long now = System.nanoTime();
            if (!java.util.Objects.equals(token, lockToken) || now >= expiresAtNanos) {
                return false;
            }
            renewCalls++;
            expiresAtNanos = now + lockTtl.toNanos();
            return true;
        }

        @Override
        public synchronized boolean writeSnapshotIfLockOwned(
                String orgCode,
                String productId,
                String lockToken,
                V1ProductSelection selection,
                Instant loadedAt) {
            if (!java.util.Objects.equals(token, lockToken) || System.nanoTime() >= expiresAtNanos) {
                return false;
            }
            entry = new V1ProductSelectionCacheEntry(
                    selection,
                    CacheKind.SNAPSHOT,
                    loadedAt,
                    loadedAt.plus(SETTINGS.refreshAfter()),
                    loadedAt.plus(SETTINGS.maxStaleAge()));
            token = null;
            return true;
        }

        @Override
        public boolean writeNegativeIfLockOwned(
                String orgCode,
                String productId,
                String lockToken,
                V1ProductSelection selection,
                Instant loadedAt) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void releaseRefreshLock(String orgCode, String productId, String lockToken) {
            if (java.util.Objects.equals(token, lockToken)) {
                token = null;
            }
        }

        @Override
        public Optional<V1ProductSelectionCacheEntry> awaitNewerEntry(
                String orgCode, String productId, Instant observedLoadedAt) {
            return Optional.empty();
        }
    }
}
