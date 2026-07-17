package com.cardshop.trade.engine.application.v1;

/**
 * V1 商户配置启动预热结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。启动日志和测试只记录
 * 商户号、数量和结果，不携带 appSecret、aesKey 等敏感字段。
 */
public record V1MerchantConfigPreheatResult(
        int requested,
        int succeeded,
        int failed) {
}
