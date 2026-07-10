package com.cardshop.trade.engine.api.v1.usecase;

import java.time.Duration;

/**
 * V1 Redis 接单用例运行参数。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「11. 配置中心、部署与健康检查」和
 * 「6. Redis 受理与缓存层」。当前对象只承载接口层构造接单命令所需的最小时间参数。
 *
 * @param firstPersistRetryDelay Redis pending 首次补偿扫描延迟
 */
public record V1RechargeAcceptSettings(Duration firstPersistRetryDelay) {

    public V1RechargeAcceptSettings {
        if (firstPersistRetryDelay == null || firstPersistRetryDelay.isNegative() || firstPersistRetryDelay.isZero()) {
            throw new IllegalArgumentException("firstPersistRetryDelay must be positive");
        }
    }
}
