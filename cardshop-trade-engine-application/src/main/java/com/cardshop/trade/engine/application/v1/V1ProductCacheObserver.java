package com.cardshop.trade.engine.application.v1;

import java.time.Duration;

/**
 * V1 商品缓存低基数观测端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「12. 可观测性」。实现禁止把商户号、
 * 商品号、账号或订单号写入指标标签。
 */
public interface V1ProductCacheObserver {

    void recordCacheResult(String result, Duration age);

    void recordRefresh(String result, Duration duration);

    void recordMysqlLoad();

    static V1ProductCacheObserver noop() {
        return new V1ProductCacheObserver() {
            @Override
            public void recordCacheResult(String result, Duration age) {
            }

            @Override
            public void recordRefresh(String result, Duration duration) {
            }

            @Override
            public void recordMysqlLoad() {
            }
        };
    }
}
