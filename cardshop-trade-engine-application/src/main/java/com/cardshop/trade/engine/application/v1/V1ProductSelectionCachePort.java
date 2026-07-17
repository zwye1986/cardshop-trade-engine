package com.cardshop.trade.engine.application.v1;

import java.time.Instant;
import java.util.Optional;

/**
 * V1 父商品选品快照缓存端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * 端口不暴露 Redis 类型；实现必须保证刷新锁带所有权 token，且父商品快照和实际子商品配置
 * 在确认锁仍归当前刷新者所有后原子完成。
 */
public interface V1ProductSelectionCachePort {

    default boolean isAvailable() {
        return true;
    }

    Optional<V1ProductSelectionCacheEntry> find(String orgCode, String productId);

    Optional<String> tryAcquireRefreshLock(String orgCode, String productId);

    /**
     * 仅当刷新锁仍由当前 token 持有时延长租约。
     *
     * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。默认返回 false，
     * 使不支持续租的测试或替代实现 fail closed，不能在慢 MySQL 查询期间静默失去单飞保护。
     */
    default boolean renewRefreshLock(String orgCode, String productId, String lockToken) {
        return false;
    }

    boolean writeSnapshotIfLockOwned(
            String orgCode,
            String productId,
            String lockToken,
            V1ProductSelection selection,
            Instant loadedAt);

    boolean writeNegativeIfLockOwned(
            String orgCode,
            String productId,
            String lockToken,
            V1ProductSelection selection,
            Instant loadedAt);

    void releaseRefreshLock(String orgCode, String productId, String lockToken);

    Optional<V1ProductSelectionCacheEntry> awaitNewerEntry(
            String orgCode,
            String productId,
            Instant observedLoadedAt);

    static V1ProductSelectionCachePort unavailable() {
        return new V1ProductSelectionCachePort() {
            @Override
            public boolean isAvailable() {
                return false;
            }

            @Override
            public Optional<V1ProductSelectionCacheEntry> find(String orgCode, String productId) {
                throw unavailableException();
            }

            @Override
            public Optional<String> tryAcquireRefreshLock(String orgCode, String productId) {
                throw unavailableException();
            }

            @Override
            public boolean renewRefreshLock(String orgCode, String productId, String lockToken) {
                throw unavailableException();
            }

            @Override
            public boolean writeSnapshotIfLockOwned(
                    String orgCode,
                    String productId,
                    String lockToken,
                    V1ProductSelection selection,
                    Instant loadedAt) {
                throw unavailableException();
            }

            @Override
            public boolean writeNegativeIfLockOwned(
                    String orgCode,
                    String productId,
                    String lockToken,
                    V1ProductSelection selection,
                    Instant loadedAt) {
                throw unavailableException();
            }

            @Override
            public void releaseRefreshLock(String orgCode, String productId, String lockToken) {
                throw unavailableException();
            }

            @Override
            public Optional<V1ProductSelectionCacheEntry> awaitNewerEntry(
                    String orgCode,
                    String productId,
                    Instant observedLoadedAt) {
                throw unavailableException();
            }

            private IllegalStateException unavailableException() {
                return new IllegalStateException("V1 product selection cache is unavailable");
            }
        };
    }
}
