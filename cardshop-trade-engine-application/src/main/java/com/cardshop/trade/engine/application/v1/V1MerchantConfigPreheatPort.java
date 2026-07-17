package com.cardshop.trade.engine.application.v1;

/**
 * V1 商户配置 Redis 预热端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。该端口只负责把
 * 已由旧库读取并校验过的商户画像写入 {@code merchant:config}，不得重新查询旧库或输出密钥字段。
 */
@FunctionalInterface
public interface V1MerchantConfigPreheatPort {

    /**
     * 写入单个商户的 Redis 门禁配置。
     */
    void preheat(V1MerchantProfile profile);
}
