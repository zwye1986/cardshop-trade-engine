package com.cardshop.trade.engine.api.v1.routing;

import com.cardshop.trade.engine.api.v1.security.AuthenticatedMerchant;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Map;

/**
 * `/v1/cardapi` 灰度路由判定服务。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.1.1 `/v1/cardapi` 路由兼容设计」：
 * 默认保守回退旧系统，只有路径支持且明确命中灰度 appId 或 orgCode 时才进入新链路。
 */
@Component
public class V1RouteDecisionService {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final V1RoutingSettings settings;
    private final ObjectMapper objectMapper;

    @Autowired
    public V1RouteDecisionService(V1RoutingSettings settings) {
        this(settings, new ObjectMapper());
    }

    V1RouteDecisionService(V1RoutingSettings settings, ObjectMapper objectMapper) {
        this.settings = settings;
        this.objectMapper = objectMapper;
    }

    /**
     * Token 申请接口只能从请求体解析 appId；解析失败时回退旧系统保持兼容。
     */
    public V1RouteDecision decideAuth(String path, String rawBody) {
        V1RouteDecision guardDecision = guard(path);
        if (guardDecision != null) {
            return guardDecision;
        }

        String appId = readAppId(rawBody);
        if (!StringUtils.hasText(appId)) {
            return V1RouteDecision.legacy("auth-app-id-missing");
        }
        if (contains(settings.grayAppIds(), appId)) {
            return V1RouteDecision.newRoute("gray-app");
        }
        return V1RouteDecision.legacy("not-gray-app");
    }

    /**
     * 下单、查单、作废接口先完成 Token 兼容解析，再按 appId/orgCode 灰度。
     */
    public V1RouteDecision decideOrder(String path, AuthenticatedMerchant merchant) {
        V1RouteDecision guardDecision = guard(path);
        if (guardDecision != null) {
            return guardDecision;
        }
        if (merchant == null) {
            return V1RouteDecision.legacy("merchant-missing");
        }
        if (contains(settings.grayOrgCodes(), merchant.orgCode())) {
            return V1RouteDecision.newRoute("gray-org");
        }
        if (contains(settings.grayAppIds(), merchant.appId())) {
            return V1RouteDecision.newRoute("gray-app");
        }
        return V1RouteDecision.legacy("not-gray-merchant");
    }

    public V1RoutingSettings settings() {
        return settings;
    }

    private V1RouteDecision guard(String path) {
        if (!settings.enabled()) {
            return V1RouteDecision.legacy("routing-disabled");
        }
        if (!contains(settings.supportedPaths(), path)) {
            return V1RouteDecision.legacy("unsupported-path");
        }
        return null;
    }

    private String readAppId(String rawBody) {
        if (!StringUtils.hasText(rawBody)) {
            return null;
        }
        try {
            Map<String, Object> body = objectMapper.readValue(rawBody, MAP_TYPE);
            Object snake = body.get("app_id");
            Object camel = body.get("appId");
            Object value = snake == null ? camel : snake;
            return value == null ? null : String.valueOf(value).trim();
        } catch (Exception ignored) {
            return null;
        }
    }

    private static boolean contains(Iterable<String> values, String candidate) {
        if (!StringUtils.hasText(candidate)) {
            return false;
        }
        for (String value : values) {
            if (candidate.trim().equals(value)) {
                return true;
            }
        }
        return false;
    }
}
