package com.cardshop.trade.engine.application.v1;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis 优先、MySQL 单飞回源的 V1 商品候选选择器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。正常下单只读取
 * Redis 父商品选品快照；进入软过期后仅锁持有者读取 MySQL，未持锁请求仍可在已确认的 30 分钟窗口内
 * 使用旧价；达到硬过期后必须等待或完成可信刷新，否则在 Redis 接单和余额预扣前返回系统繁忙。
 */
public class CachedV1ProductCandidateSelector implements V1ProductCandidateSelector {

    private static final Logger log = LoggerFactory.getLogger(CachedV1ProductCandidateSelector.class);
    private static final Duration DEGRADED_LOG_INTERVAL = Duration.ofSeconds(30);
    private static final String LOG_EVENT_DEGRADED = "v1-product-cache.degraded";
    private static final LogStormGuard DEGRADED_LOG_GUARD = LogStormGuard.atMostOncePer(DEGRADED_LOG_INTERVAL);

    private final V1ProductSelectionCachePort cachePort;
    private final V1ProductCandidateSelector sourceSelector;
    private final V1ProductCacheSettings settings;
    private final Clock clock;
    private final V1ProductCacheObserver observer;

    public CachedV1ProductCandidateSelector(
            V1ProductSelectionCachePort cachePort,
            V1ProductCandidateSelector sourceSelector,
            V1ProductCacheSettings settings,
            Clock clock,
            V1ProductCacheObserver observer) {
        this.cachePort = Objects.requireNonNull(cachePort, "cachePort must not be null");
        this.sourceSelector = Objects.requireNonNull(sourceSelector, "sourceSelector must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.observer = observer == null ? V1ProductCacheObserver.noop() : observer;
    }

    @Override
    public V1ProductSelection select(String orgCode, String productId, String account) {
        Instant now = clock.instant();
        Optional<V1ProductSelectionCacheEntry> cached;
        try {
            cached = cachePort.find(orgCode, productId);
        } catch (RuntimeException ex) {
            observer.recordCacheResult("miss", Duration.ZERO);
            throw new V1ProductCacheUnavailableException("V1商品选品缓存读取失败", ex);
        }

        if (cached.isEmpty()) {
            observer.recordCacheResult("miss", Duration.ZERO);
            return refreshHardOrWait(orgCode, productId, account, null);
        }

        V1ProductSelectionCacheEntry entry = cached.get();
        Duration age = entry.age(now);
        if (entry.negative() && now.isBefore(entry.expireAt())) {
            observer.recordCacheResult("negative", age);
            return entry.selection();
        }
        if (!entry.negative() && now.isBefore(entry.refreshAfter())) {
            observer.recordCacheResult("fresh", age);
            return entry.selection();
        }
        if (!entry.negative() && now.isBefore(entry.expireAt())) {
            observer.recordCacheResult("soft_stale", age);
            return refreshSoftOrUseOld(orgCode, productId, account, entry);
        }

        observer.recordCacheResult("hard_stale", age);
        return refreshHardOrWait(orgCode, productId, account, entry);
    }

    private V1ProductSelection refreshSoftOrUseOld(
            String orgCode,
            String productId,
            String account,
            V1ProductSelectionCacheEntry oldEntry) {
        Optional<String> token = acquireLock(orgCode, productId, false);
        if (token.isEmpty()) {
            observer.recordRefresh("lock_busy", Duration.ZERO);
            if (clock.instant().isBefore(oldEntry.expireAt())) {
                return oldEntry.selection();
            }
            return refreshHardOrWait(orgCode, productId, account, oldEntry);
        }
        try {
            return refreshAsOwner(orgCode, productId, account, token.get());
        } catch (RuntimeException ex) {
            if (clock.instant().isBefore(oldEntry.expireAt())) {
                logDegraded("V1商品选品软过期刷新失败，继续使用30分钟窗口内旧快照", orgCode, productId, ex);
                return oldEntry.selection();
            }
            throw new V1ProductCacheUnavailableException("V1商品选品刷新失败且旧快照已硬过期", ex);
        } finally {
            releaseLock(orgCode, productId, token.get());
        }
    }

    private V1ProductSelection refreshHardOrWait(
            String orgCode,
            String productId,
            String account,
            V1ProductSelectionCacheEntry observedEntry) {
        Optional<String> token = acquireLock(orgCode, productId, true);
        if (token.isPresent()) {
            try {
                return refreshAsOwner(orgCode, productId, account, token.get());
            } catch (V1ProductCacheUnavailableException ex) {
                throw ex;
            } catch (RuntimeException ex) {
                throw new V1ProductCacheUnavailableException("V1商品选品缓存硬过期刷新失败", ex);
            } finally {
                releaseLock(orgCode, productId, token.get());
            }
        }

        observer.recordRefresh("lock_busy", Duration.ZERO);
        Instant observedLoadedAt = observedEntry == null ? Instant.EPOCH : observedEntry.loadedAt();
        Optional<V1ProductSelectionCacheEntry> refreshed;
        try {
            refreshed = cachePort.awaitNewerEntry(orgCode, productId, observedLoadedAt);
        } catch (RuntimeException ex) {
            observer.recordRefresh("redis_failed", Duration.ZERO);
            throw new V1ProductCacheUnavailableException("V1商品选品硬过期等待刷新失败", ex);
        }
        if (refreshed.isEmpty()) {
            observer.recordRefresh("wait_timeout", settings.refreshWaitTimeout());
            throw new V1ProductCacheUnavailableException("V1商品选品缓存硬过期等待刷新超时");
        }
        return refreshed.get().selection();
    }

    private V1ProductSelection refreshAsOwner(
            String orgCode,
            String productId,
            String account,
            String lockToken) {
        long startedNanos = System.nanoTime();
        RefreshLockLease lease = RefreshLockLease.start(
                cachePort,
                orgCode,
                productId,
                lockToken,
                settings.refreshLockTtl());
        try {
            V1ProductSelection loaded;
            observer.recordMysqlLoad();
            try {
                loaded = sourceSelector.select(orgCode, productId, account);
            } catch (RuntimeException ex) {
                observer.recordRefresh("mysql_failed", elapsed(startedNanos));
                throw ex;
            }

            RuntimeException leaseFailure = lease.failure();
            if (leaseFailure != null) {
                observer.recordRefresh("redis_failed", elapsed(startedNanos));
                throw new V1ProductCacheUnavailableException("V1商品选品刷新锁续租失败", leaseFailure);
            }

            Instant loadedAt = clock.instant();
            if (!loaded.productSnapshotAvailable()) {
                /*
                 * 参见详细设计「6. Redis 受理与缓存层」：负缓存只是保护 MySQL 的优化，写入失败时仍返回
                 * MySQL 已确认的业务失败，不能把“商品无权限”误报成系统异常或可售商品。
                 */
                try {
                    boolean cached = cachePort.writeNegativeIfLockOwned(
                            orgCode, productId, lockToken, loaded, loadedAt);
                    observer.recordRefresh(cached ? "success" : "redis_failed", elapsed(startedNanos));
                } catch (RuntimeException ex) {
                    observer.recordRefresh("redis_failed", elapsed(startedNanos));
                    logDegraded("V1商品负缓存写入失败，继续返回MySQL已确认的业务失败", orgCode, productId, ex);
                }
                return loaded;
            }

            boolean written;
            try {
                written = cachePort.writeSnapshotIfLockOwned(
                        orgCode, productId, lockToken, loaded, loadedAt);
            } catch (RuntimeException ex) {
                observer.recordRefresh("redis_failed", elapsed(startedNanos));
                throw new V1ProductCacheUnavailableException("V1商品选品Redis原子刷新失败", ex);
            }
            if (!written) {
                observer.recordRefresh("redis_failed", elapsed(startedNanos));
                throw new V1ProductCacheUnavailableException("V1商品选品刷新锁已失效或原子写入未完成");
            }
            observer.recordRefresh("success", elapsed(startedNanos));
            return loaded;
        } finally {
            lease.close();
        }
    }

    private Optional<String> acquireLock(String orgCode, String productId, boolean hardExpiry) {
        try {
            return cachePort.tryAcquireRefreshLock(orgCode, productId);
        } catch (RuntimeException ex) {
            observer.recordRefresh("redis_failed", Duration.ZERO);
            if (hardExpiry) {
                throw new V1ProductCacheUnavailableException("V1商品选品刷新锁获取失败", ex);
            }
            logDegraded("V1商品选品软过期刷新锁获取失败", orgCode, productId, ex);
            return Optional.empty();
        }
    }

    private void releaseLock(String orgCode, String productId, String token) {
        try {
            cachePort.releaseRefreshLock(orgCode, productId, token);
        } catch (RuntimeException ex) {
            /*
             * 该路径位于每请求刷新竞争链路，Redis 故障时可能高频触发，因此按 30 秒采样 warn；锁本身有
             * 5 秒 TTL，释放失败不会形成永久锁，但必须保留异常栈供排障。参见详细设计「12. 可观测性」。
             */
            logDegraded("V1商品选品刷新锁释放失败，等待锁TTL自动释放", orgCode, productId, ex);
        }
    }

    private static Duration elapsed(long startedNanos) {
        return Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNanos));
    }

    private static void logDegraded(String message, String orgCode, String productId, RuntimeException ex) {
        if (DEGRADED_LOG_GUARD.shouldLog(LOG_EVENT_DEGRADED).shouldLog()) {
            log.warn("{} orgCode={}, productId={}, sampleIntervalSeconds={}",
                    message,
                    orgCode,
                    productId,
                    DEGRADED_LOG_INTERVAL.toSeconds(),
                    ex);
        }
    }

    /**
     * MySQL 阻塞查询期间的父商品刷新锁租约。
     *
     * <p>参见详细设计「6. Redis 受理与缓存层」。续租只在 token 仍匹配时生效；任何续租异常都会让
     * 当前刷新者 fail closed，最终写入仍由 Redis fencing Lua 再校验一次所有权。
     */
    private static final class RefreshLockLease implements AutoCloseable {

        private static final Duration MIN_RENEW_INTERVAL = Duration.ofMillis(1);

        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        private final Thread watchdog;

        private RefreshLockLease(
                V1ProductSelectionCachePort cachePort,
                String orgCode,
                String productId,
                String lockToken,
                Duration lockTtl) {
            Duration renewInterval = lockTtl.dividedBy(3);
            if (renewInterval.compareTo(MIN_RENEW_INTERVAL) < 0) {
                renewInterval = MIN_RENEW_INTERVAL;
            }
            Duration interval = renewInterval;
            watchdog = Thread.ofVirtual()
                    .name("v1-product-cache-lock-renew")
                    .start(() -> renewLoop(cachePort, orgCode, productId, lockToken, interval));
        }

        static RefreshLockLease start(
                V1ProductSelectionCachePort cachePort,
                String orgCode,
                String productId,
                String lockToken,
                Duration lockTtl) {
            return new RefreshLockLease(cachePort, orgCode, productId, lockToken, lockTtl);
        }

        RuntimeException failure() {
            return failure.get();
        }

        private void renewLoop(
                V1ProductSelectionCachePort cachePort,
                String orgCode,
                String productId,
                String lockToken,
                Duration interval) {
            while (!closed.get()) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (closed.get()) {
                    return;
                }
                try {
                    if (!cachePort.renewRefreshLock(orgCode, productId, lockToken)) {
                        failure.compareAndSet(null, new IllegalStateException("refresh lock is no longer owned"));
                        return;
                    }
                } catch (RuntimeException ex) {
                    failure.compareAndSet(null, ex);
                    return;
                }
            }
        }

        @Override
        public void close() {
            closed.set(true);
            watchdog.interrupt();
            try {
                watchdog.join();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
