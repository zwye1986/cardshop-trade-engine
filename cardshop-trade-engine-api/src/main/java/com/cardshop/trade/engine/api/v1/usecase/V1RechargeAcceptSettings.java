package com.cardshop.trade.engine.api.v1.usecase;

import java.time.Duration;

/**
 * V1 Redis 接单用例运行参数。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「7.4 核心配置项」和
 * 「20.13.6 Redis Lua 下单结果」。当前对象只承载接口层构造接单命令所需的最小时间参数。
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
