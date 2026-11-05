package com.cardshop.trade.engine.api.v1.usecase;

import java.time.Duration;
import java.util.function.BiConsumer;

/** 作废HTTP等待配置及响应指标边界，应用层不依赖Reactor/Micrometer；参见详细设计9.3.4、9.3.8。 */
public record V1InvalidationSettings(Duration waitTimeout, BiConsumer<String, Duration> responseObserver) {
    public V1InvalidationSettings {
        if (waitTimeout.isZero() || waitTimeout.isNegative()) throw new IllegalArgumentException("invalidation wait timeout must be positive");
        java.util.Objects.requireNonNull(responseObserver);
    }
}
