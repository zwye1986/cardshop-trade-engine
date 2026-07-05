package com.cardshop.trade.engine.api.v1.routing;

import java.time.Duration;
import java.util.List;

/**
 * `/v1/cardapi` 路由所需的最小配置视图。
 *
 * <p>API 模块只依赖这个不可变配置快照，避免反向依赖 infrastructure 中的 Nacos 绑定对象。
 */
public record V1RoutingSettings(
        boolean enabled,
        String legacyBaseUrl,
        Duration proxyTimeout,
        List<String> grayOrgCodes,
        List<String> grayAppIds,
        List<String> supportedPaths) {

    public V1RoutingSettings {
        if (proxyTimeout == null || proxyTimeout.isZero() || proxyTimeout.isNegative()) {
            proxyTimeout = Duration.ofSeconds(3);
        }
        grayOrgCodes = normalized(grayOrgCodes);
        grayAppIds = normalized(grayAppIds);
        supportedPaths = normalized(supportedPaths);
    }

    private static List<String> normalized(List<String> values) {
        if (values == null) {
            return List.of();
        }
        return values.stream()
                .filter(value -> value != null && !value.isBlank())
                .map(String::trim)
                .distinct()
                .toList();
    }
}
