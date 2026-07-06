package com.cardshop.trade.engine.api.v1.controller;

import com.cardshop.trade.engine.api.v1.routing.LegacyCardApiClient;
import com.cardshop.trade.engine.api.v1.routing.V1RouteDecisionService;
import com.cardshop.trade.engine.api.v1.routing.V1RoutingSettings;
import com.cardshop.trade.engine.api.v1.security.AuthenticatedMerchant;
import com.cardshop.trade.engine.api.v1.security.V1TokenAuthenticator;
import com.cardshop.trade.engine.api.v1.security.V1TokenSettings;
import com.cardshop.trade.engine.application.v1.CreateRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.IssueV1TokenUseCase;
import com.cardshop.trade.engine.application.v1.QueryRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V1 WebFlux 入口阻塞边界测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.1.1 `/v1/cardapi` 路由兼容设计」
 * 和「20.13.5 Token 生成接口」。商户资料查询、Redis token 写读仍通过同步端口兼容旧
 * MyBatis/Redis 适配器，因此 controller 必须把这些调用隔离到 boundedElastic，避免 Netty
 * 事件循环线程直接等待阻塞 IO。
 */
class V1ControllerBlockingBoundaryTest {

    @Test
    void authTokenIssueRunsOnBoundedElasticWhenNewRouteIsSelected() {
        AtomicReference<String> threadName = new AtomicReference<>();
        IssueV1TokenUseCase useCase = rawBody -> {
            threadName.set(Thread.currentThread().getName());
            return new V1CardApiResult("200", "操作成功", null);
        };
        V1AuthController controller = new V1AuthController(
                routeDecisionService("/v1/cardapi/auth/getToken"),
                legacyClient(),
                useCase);

        controller.getToken(Mono.just("""
                {"app_id":"app-gray","app_secret":"secret-1"}
                """), request("/v1/cardapi/auth/getToken")).block();

        assertThat(threadName.get()).contains("boundedElastic");
    }

    @Test
    void orderTokenAuthenticationRunsOnBoundedElasticBeforeRouting() {
        AtomicReference<String> threadName = new AtomicReference<>();
        V1TokenAuthenticator authenticator = new V1TokenAuthenticator(tokenSettings(), fixedClock(), Optional.empty()) {
            @Override
            public Optional<AuthenticatedMerchant> authenticate(String authorization) {
                threadName.set(Thread.currentThread().getName());
                return Optional.of(new AuthenticatedMerchant("app-gray", "A36"));
            }
        };
        QueryRechargeOrderUseCase queryUseCase = command -> new V1CardApiResult("200", "操作成功", null);
        V1RechargeOrderController controller = new V1RechargeOrderController(
                routeDecisionService("/v1/cardapi/order/getRechargeOrder"),
                authenticator,
                legacyClient(),
                createUseCase(),
                queryUseCase,
                command -> new V1CardApiResult("200", "操作成功", null));

        controller.getRechargeOrder("Bearer token", Mono.just("{}"),
                request("/v1/cardapi/order/getRechargeOrder")).block();

        assertThat(threadName.get()).contains("boundedElastic");
    }

    private static V1RouteDecisionService routeDecisionService(String supportedPath) {
        return new V1RouteDecisionService(new V1RoutingSettings(
                true,
                "http://legacy.example",
                Duration.ofSeconds(3),
                List.of("A36"),
                List.of("app-gray"),
                List.of(supportedPath)));
    }

    private static LegacyCardApiClient legacyClient() {
        return (request, rawBody) -> Mono.just(ResponseEntity.ok("legacy"));
    }

    private static CreateRechargeOrderUseCase createUseCase() {
        return command -> CompletableFuture.completedFuture(new V1CardApiResult("200", "操作成功", null));
    }

    private static ServerHttpRequest request(String path) {
        return MockServerHttpRequest.post(path).build();
    }

    private static V1TokenSettings tokenSettings() {
        return new V1TokenSettings("test-secret", "order_token_key:", Duration.ofSeconds(30));
    }

    private static Clock fixedClock() {
        return Clock.fixed(Instant.parse("2026-07-06T02:00:00Z"), ZoneOffset.UTC);
    }
}
