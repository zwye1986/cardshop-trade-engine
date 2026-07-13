package com.cardshop.trade.engine.api.v1.routing;

import com.cardshop.trade.engine.api.v1.security.AuthenticatedMerchant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * `/v1/cardapi` 路由判定服务。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」：
 * 当前 dev/test 阶段只接入 A39 交行机构，暂时不按灰度名单切流。路由开启且路径受支持时直接进入新链路；
 * 路由关闭或未知路径才回退旧系统代理，避免未实现路径被新服务误接管。
 */
@Component
public class V1RouteDecisionService {

    private static final String REASON_SUPPORTED_PATH = "supported-path";

    private final V1RoutingSettings settings;

    @Autowired
    public V1RouteDecisionService(V1RoutingSettings settings) {
        this.settings = settings;
    }

    /**
     * Token 申请接口在当前 A39 接入口径下不再读取 appId 做灰度判定。
     *
     * <p>请求体为空、缺 appId 或凭证错误时，应由新链路 token 用例返回旧协议业务错误，而不是绕到旧系统代理。
     */
    public V1RouteDecision decideAuth(String path, String rawBody) {
        V1RouteDecision guardDecision = guard(path);
        if (guardDecision != null) {
            return guardDecision;
        }

        return V1RouteDecision.newRoute(REASON_SUPPORTED_PATH);
    }

    /**
     * 下单、查单、作废接口先完成 Token 兼容解析；当前不再按 appId/orgCode 灰度过滤。
     *
     * <p>merchant 为空代表认证失败或缺少 token，仍进入新链路用例，由用例按旧协议返回鉴权失败。
     */
    public V1RouteDecision decideOrder(String path, AuthenticatedMerchant merchant) {
        V1RouteDecision guardDecision = guard(path);
        if (guardDecision != null) {
            return guardDecision;
        }
        return V1RouteDecision.newRoute(REASON_SUPPORTED_PATH);
    }

    public V1RoutingSettings settings() {
        return settings;
    }

    private V1RouteDecision guard(String path) {
        if (!settings.enabled()) {
            return V1RouteDecision.legacy("routing-disabled");
        }
        if (!isSupportedPath(path)) {
            return V1RouteDecision.legacy("unsupported-path");
        }
        return null;
    }

    private boolean isSupportedPath(String path) {
        return path != null && settings.supportedPaths().contains(path.trim());
    }
}
