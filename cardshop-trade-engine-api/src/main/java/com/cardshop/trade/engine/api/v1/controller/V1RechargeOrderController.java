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

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * 原卡管兼容的 `/v1/cardapi/order` 入口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.6 创建直充订单」、
 * 「20.13.10 查单设计」和「20.13.11 作废设计」。D3 阶段负责入口、token 兼容解析、
 * 灰度判定和旧系统代理；创建直充订单在新路由命中后交由应用用例完成协议预检和 Redis 接单。
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
        Optional<AuthenticatedMerchant> merchant = tokenAuthenticator.authenticate(authorization);
        return body.defaultIfEmpty("")
                .flatMap(rawBody -> {
                    V1RouteDecision decision = routeDecisionService.decideOrder(path(request), merchant.orElse(null));
                    log.info("V1订单路由判定 path={}, target={}, reason={}, appId={}, orgCode={}",
                            path(request),
                            decision.target(),
                            decision.reason(),
                            merchant.map(AuthenticatedMerchant::appId).orElse(""),
                            merchant.map(AuthenticatedMerchant::orgCode).orElse(""));
                    if (decision.isLegacy()) {
                        return legacyCardApiClient.proxy(request, rawBody).map(entity -> (ResponseEntity<?>) entity);
                    }
                    AuthenticatedMerchant principal = merchant.orElse(new AuthenticatedMerchant("", ""));
                    V1CardApiResult result = newRouteHandler.apply(
                            new V1MerchantCommand(principal.appId(), principal.orgCode(), rawBody));
                    return Mono.just(ok(result));
                });
    }

    private static ResponseEntity<V1CardApiResponse<Object>> ok(V1CardApiResult result) {
        return ResponseEntity.ok(V1CardApiResponse.failure(result.code(), result.msg(), result.data()));
    }

    private Mono<ResponseEntity<?>> routeAsyncOrderRequest(
            String authorization,
            Mono<String> body,
            ServerHttpRequest request,
            Function<V1MerchantCommand, CompletionStage<V1CardApiResult>> newRouteHandler) {
        Optional<AuthenticatedMerchant> merchant = tokenAuthenticator.authenticate(authorization);
        return body.defaultIfEmpty("")
                .flatMap(rawBody -> {
                    V1RouteDecision decision = routeDecisionService.decideOrder(path(request), merchant.orElse(null));
                    log.info("V1订单路由判定 path={}, target={}, reason={}, appId={}, orgCode={}",
                            path(request),
                            decision.target(),
                            decision.reason(),
                            merchant.map(AuthenticatedMerchant::appId).orElse(""),
                            merchant.map(AuthenticatedMerchant::orgCode).orElse(""));
                    if (decision.isLegacy()) {
                        return legacyCardApiClient.proxy(request, rawBody).map(entity -> (ResponseEntity<?>) entity);
                    }
                    AuthenticatedMerchant principal = merchant.orElse(new AuthenticatedMerchant("", ""));
                    return Mono.fromCompletionStage(newRouteHandler.apply(
                            new V1MerchantCommand(principal.appId(), principal.orgCode(), rawBody)))
                            .map(V1RechargeOrderController::ok);
                });
    }

    private static String path(ServerHttpRequest request) {
        return request.getPath().pathWithinApplication().value();
    }
}
