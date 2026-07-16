package com.cardshop.trade.engine.application.v1;

/**
 * V1 商品缓存达到硬过期后无法完成可信刷新。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。该异常必须在
 * Redis 接单和余额预扣前映射为系统繁忙，不能继续使用超过 30 分钟的旧价格。
 */
public class V1ProductCacheUnavailableException extends RuntimeException {

    public V1ProductCacheUnavailableException(String message) {
        super(message);
    }

    public V1ProductCacheUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
