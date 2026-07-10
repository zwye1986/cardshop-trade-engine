package com.cardshop.trade.engine.api.v1.controller;

import com.cardshop.trade.engine.api.v1.dto.V1CardApiResponse;
import com.cardshop.trade.engine.api.v1.routing.LegacyCardApiClient;
import com.cardshop.trade.engine.api.v1.routing.V1RouteDecision;
import com.cardshop.trade.engine.api.v1.routing.V1RouteDecisionService;
import com.cardshop.trade.engine.api.v1.security.AuthenticatedMerchant;
import com.cardshop.trade.engine.api.v1.security.V1TokenAuthenticator;
import com.cardshop.trade.engine.application.v1.CreateRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.QueryRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.application.v1.V1MerchantCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * 原卡管兼容的 `/v1/cardapi/order` 入口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「4. Application 用例编排层」、
 * 「3. API/V1 接入层」和「9.2 作废设计」。入口层负责 token 兼容解析、灰度判定和旧系统代理；
 * 其中 token 兼容解析可能读取共享 Redis key {@code token:app_id:{appId}}，必须和 Netty
 * 事件循环隔离，避免灰度流量放大后造成 worker 线程饥饿。
 */
@RestController
@RequestMapping("/v1/cardapi/order")
public class V1RechargeOrderController {

    private static final Logger log = LoggerFactory.getLogger(V1RechargeOrderController.class);

    private final V1RouteDecisionService routeDecisionService;
    private final V1TokenAuthenticator tokenAuthenticator;
    private final LegacyCardApiClient legacyCardApiClient;
    private final CreateRechargeOrderUseCase createRechargeOrderUseCase;
    private final QueryRechargeOrderUseCase queryRechargeOrderUseCase;
    private final InvalidRechargeOrderUseCase invalidRechargeOrderUseCase;

    public V1RechargeOrderController(
            V1RouteDecisionService routeDecisionService,
            V1TokenAuthenticator tokenAuthenticator,
            LegacyCardApiClient legacyCardApiClient,
            CreateRechargeOrderUseCase createRechargeOrderUseCase,
            QueryRechargeOrderUseCase queryRechargeOrderUseCase,
            InvalidRechargeOrderUseCase invalidRechargeOrderUseCase) {
        this.routeDecisionService = routeDecisionService;
        this.tokenAuthenticator = tokenAuthenticator;
        this.legacyCardApiClient = legacyCardApiClient;
        this.createRechargeOrderUseCase = createRechargeOrderUseCase;
        this.queryRechargeOrderUseCase = queryRechargeOrderUseCase;
        this.invalidRechargeOrderUseCase = invalidRechargeOrderUseCase;
    }

    @PostMapping("/createRechargeOrder")
    public Mono<ResponseEntity<?>> createRechargeOrder(
            @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization,
            @RequestBody(required = false) Mono<String> body,
            ServerHttpRequest request) {
        return routeAsyncOrderRequest(authorization, body, request, createRechargeOrderUseCase::createRechargeOrder);
    }

    @PostMapping("/getRechargeOrder")
    public Mono<ResponseEntity<?>> getRechargeOrder(
            @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization,
            @RequestBody(required = false) Mono<String> body,
            ServerHttpRequest request) {
        return routeOrderRequest(authorization, body, request, queryRechargeOrderUseCase::queryRechargeOrder);
    }

    @PostMapping("/invalid")
    public Mono<ResponseEntity<?>> invalidRechargeOrder(
            @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization,
            @RequestBody(required = false) Mono<String> body,
            ServerHttpRequest request) {
        return routeOrderRequest(authorization, body, request, invalidRechargeOrderUseCase::invalidRechargeOrder);
    }

    private Mono<ResponseEntity<?>> routeOrderRequest(
            String authorization,
            Mono<String> body,
            ServerHttpRequest request,
            Function<V1MerchantCommand, V1CardApiResult> newRouteHandler) {
        return authenticateOnBlockingScheduler(authorization)
                .flatMap(merchant -> body.defaultIfEmpty("")
                        .flatMap(rawBody -> routeResolvedOrderRequest(request, newRouteHandler, merchant, rawBody)));
    }

    private static ResponseEntity<V1CardApiResponse<Object>> ok(V1CardApiResult result) {
        return ResponseEntity.ok(V1CardApiResponse.failure(result.code(), result.msg(), result.data()));
    }

    private Mono<ResponseEntity<?>> routeAsyncOrderRequest(
            String authorization,
            Mono<String> body,
            ServerHttpRequest request,
            Function<V1MerchantCommand, CompletionStage<V1CardApiResult>> newRouteHandler) {
        return authenticateOnBlockingScheduler(authorization)
                .flatMap(merchant -> body.defaultIfEmpty("")
                        .flatMap(rawBody -> routeResolvedAsyncOrderRequest(request, newRouteHandler, merchant, rawBody)));
    }

    private Mono<ResponseEntity<?>> routeResolvedOrderRequest(
            ServerHttpRequest request,
            Function<V1MerchantCommand, V1CardApiResult> newRouteHandler,
            Optional<AuthenticatedMerchant> merchant,
            String rawBody) {
        V1RouteDecision decision = routeDecisionService.decideOrder(path(request), merchant.orElse(null));
        logOrderDecision(request, decision, merchant);
        if (decision.isLegacy()) {
            return legacyCardApiClient.proxy(request, rawBody).map(entity -> (ResponseEntity<?>) entity);
        }
        AuthenticatedMerchant principal = merchant.orElse(new AuthenticatedMerchant("", ""));
        return Mono.fromCallable(() -> newRouteHandler.apply(
                        new V1MerchantCommand(principal.appId(), principal.orgCode(), rawBody)))
                .subscribeOn(Schedulers.boundedElastic())
                .map(V1RechargeOrderController::ok);
    }

    private Mono<ResponseEntity<?>> routeResolvedAsyncOrderRequest(
            ServerHttpRequest request,
            Function<V1MerchantCommand, CompletionStage<V1CardApiResult>> newRouteHandler,
            Optional<AuthenticatedMerchant> merchant,
            String rawBody) {
        V1RouteDecision decision = routeDecisionService.decideOrder(path(request), merchant.orElse(null));
        logOrderDecision(request, decision, merchant);
        if (decision.isLegacy()) {
            return legacyCardApiClient.proxy(request, rawBody).map(entity -> (ResponseEntity<?>) entity);
        }
        AuthenticatedMerchant principal = merchant.orElse(new AuthenticatedMerchant("", ""));
        return Mono.fromCompletionStage(newRouteHandler.apply(
                new V1MerchantCommand(principal.appId(), principal.orgCode(), rawBody)))
                .map(V1RechargeOrderController::ok);
    }

    private void logOrderDecision(
            ServerHttpRequest request,
            V1RouteDecision decision,
            Optional<AuthenticatedMerchant> merchant) {
        log.info("V1订单路由判定 path={}, target={}, reason={}, appId={}, orgCode={}",
                path(request),
                decision.target(),
                decision.reason(),
                merchant.map(AuthenticatedMerchant::appId).orElse(""),
                merchant.map(AuthenticatedMerchant::orgCode).orElse(""));
    }

    /**
     * V1 token 校验遵循旧系统规则：先验 JWT，再读取 Redis 中
     * {@code token:app_id:{appId}} 做“最后签发 token”一致性检查，参见详细设计
     * 「3. API/V1 接入层」。当前 Redis 端口仍是同步接口，所以认证必须延迟到订阅时，
     * 并放入 boundedElastic；否则 controller 方法在 Netty event-loop 上被调用时会直接阻塞。
     */
    private Mono<Optional<AuthenticatedMerchant>> authenticateOnBlockingScheduler(String authorization) {
        return Mono.fromCallable(() -> tokenAuthenticator.authenticate(authorization))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private static String path(ServerHttpRequest request) {
        return request.getPath().pathWithinApplication().value();
    }
}
