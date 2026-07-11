package com.cardshop.trade.engine.infrastructure.observability;

/**
 * 交易引擎业务监控快照提供者。
 *
 * <p>参见 {@code docs/集成测试方案.md} 第 9 节的监控告警执行口径。Provider 可以读取 Redis/MySQL，
 * 但必须自行做低频采样或缓存，避免 Prometheus scrape 直接放大共享旧库查询压力。
 */
@FunctionalInterface
public interface TradeEngineObservabilitySnapshotProvider {

    TradeEngineObservabilitySnapshot currentSnapshot();

    static TradeEngineObservabilitySnapshotProvider empty() {
        return TradeEngineObservabilitySnapshot::empty;
    }
}
