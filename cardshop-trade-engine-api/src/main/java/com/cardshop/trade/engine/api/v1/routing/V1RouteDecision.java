package com.cardshop.trade.engine.api.v1.routing;

/**
 * `/v1/cardapi` 路由判定结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * reason 只记录规则命中原因，不包含 token、密钥、请求明文等敏感数据。
 */
public record V1RouteDecision(V1RouteTarget target, String reason) {

    public static V1RouteDecision newRoute(String reason) {
        return new V1RouteDecision(V1RouteTarget.NEW, reason);
    }

    public static V1RouteDecision legacy(String reason) {
        return new V1RouteDecision(V1RouteTarget.LEGACY, reason);
    }

    public boolean isLegacy() {
        return target == V1RouteTarget.LEGACY;
    }
}
