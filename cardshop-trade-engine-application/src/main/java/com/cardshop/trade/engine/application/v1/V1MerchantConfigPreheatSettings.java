package com.cardshop.trade.engine.application.v1;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * V1 商户配置启动预热参数。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。启动预热必须显式配置
 * 商户白名单，默认关闭，避免生产应用启动时意外扫描全量旧库或写入非灰度商户的 Redis 配置。
 */
public record V1MerchantConfigPreheatSettings(
        boolean enabled,
        List<String> orgCodes,
        boolean failOnError) {

    public V1MerchantConfigPreheatSettings {
        orgCodes = orgCodes == null ? List.of() : List.copyOf(orgCodes);
    }

    /**
     * 返回按配置顺序去重后的有效商户号。
     */
    public List<String> normalizedOrgCodes() {
        LinkedHashSet<String> normalized = new LinkedHashSet<>();
        for (String orgCode : orgCodes) {
            if (orgCode == null || orgCode.isBlank()) {
                continue;
            }
            normalized.add(orgCode.strip());
        }
        return new ArrayList<>(normalized);
    }
}
