package com.cardshop.trade.engine.api.v1.routing;

import com.cardshop.trade.engine.api.v1.security.AuthenticatedMerchant;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class V1RouteDecisionServiceTest {

    @Test
    void disabledRoutingAlwaysFallsBackToLegacy() {
        V1RoutingSettings settings = defaultSettings(false, List.of(), List.of("app-gray"));

        V1RouteDecisionService service = new V1RouteDecisionService(settings);

        V1RouteDecision decision = service.decideAuth(
                "/v1/cardapi/auth/getToken",
                "{\"app_id\":\"app-gray\",\"app_secret\":\"secret\"}"
        );

        assertThat(decision.target()).isEqualTo(V1RouteTarget.LEGACY);
        assertThat(decision.reason()).contains("disabled");
    }

    @Test
    void unsupportedPathFallsBackToLegacy() {
        V1RoutingSettings settings = defaultSettings(true, List.of(), List.of("app-gray"));

        V1RouteDecisionService service = new V1RouteDecisionService(settings);

        V1RouteDecision decision = service.decideAuth(
                "/v1/cardapi/unknown",
                "{\"app_id\":\"app-gray\"}"
        );

        assertThat(decision.target()).isEqualTo(V1RouteTarget.LEGACY);
        assertThat(decision.reason()).contains("unsupported");
    }

    @Test
    void authRequestForGrayAppUsesNewRoute() {
        V1RoutingSettings settings = defaultSettings(true, List.of(), List.of("app-gray", "app-next"));

        V1RouteDecisionService service = new V1RouteDecisionService(settings);

        V1RouteDecision decision = service.decideAuth(
                "/v1/cardapi/auth/getToken",
                "{\"app_id\":\"app-next\",\"app_secret\":\"secret\"}"
        );

        assertThat(decision.target()).isEqualTo(V1RouteTarget.NEW);
        assertThat(decision.reason()).contains("gray-app");
    }

    @Test
    void nonGrayAuthRequestFallsBackToLegacy() {
        V1RoutingSettings settings = defaultSettings(true, List.of(), List.of("app-gray"));

        V1RouteDecisionService service = new V1RouteDecisionService(settings);

        V1RouteDecision decision = service.decideAuth(
                "/v1/cardapi/auth/getToken",
                "{\"app_id\":\"app-prod\",\"app_secret\":\"secret\"}"
        );

        assertThat(decision.target()).isEqualTo(V1RouteTarget.LEGACY);
        assertThat(decision.reason()).contains("not-gray");
    }

    @Test
    void orderRequestForGrayOrgUsesNewRoute() {
        V1RoutingSettings settings = defaultSettings(true, List.of("A36", "A99"), List.of());

        V1RouteDecisionService service = new V1RouteDecisionService(settings);

        V1RouteDecision decision = service.decideOrder(
                "/v1/cardapi/order/createRechargeOrder",
                new AuthenticatedMerchant("app-prod", "A36")
        );

        assertThat(decision.target()).isEqualTo(V1RouteTarget.NEW);
        assertThat(decision.reason()).contains("gray-org");
    }

    private static V1RoutingSettings defaultSettings(boolean enabled, List<String> grayOrgCodes, List<String> grayAppIds) {
        return new V1RoutingSettings(
                enabled,
                "http://legacy.example.com",
                Duration.ofSeconds(3),
                grayOrgCodes,
                grayAppIds,
                List.of(
                        "/v1/cardapi/auth/getToken",
                        "/v1/cardapi/order/createRechargeOrder",
                        "/v1/cardapi/order/getRechargeOrder",
                        "/v1/cardapi/order/invalid"
                )
        );
    }
}
