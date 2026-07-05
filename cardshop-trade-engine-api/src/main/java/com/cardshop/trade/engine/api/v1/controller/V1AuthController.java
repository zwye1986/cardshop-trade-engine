package com.cardshop.trade.engine.api.v1.controller;

import com.cardshop.trade.engine.api.v1.dto.V1CardApiResponse;
import com.cardshop.trade.engine.api.v1.routing.LegacyCardApiClient;
import com.cardshop.trade.engine.api.v1.routing.V1RouteDecision;
import com.cardshop.trade.engine.api.v1.routing.V1RouteDecisionService;
import com.cardshop.trade.engine.application.v1.IssueV1TokenUseCase;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * 原卡管兼容的 `/v1/cardapi/auth` 入口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.5 Token 生成接口」。
 * D3 阶段只完成入口和灰度代理框架，真实 token 生成依赖后续商户密钥读取与 Redis 写入能力。
 */
@RestController
@RequestMapping("/v1/cardapi/auth")
public class V1AuthController {

    private static final Logger log = LoggerFactory.getLogger(V1AuthController.class);

    private final V1RouteDecisionService routeDecisionService;
    private final LegacyCardApiClient legacyCardApiClient;
    private final IssueV1TokenUseCase issueV1TokenUseCase;

    public V1AuthController(
            V1RouteDecisionService routeDecisionService,
            LegacyCardApiClient legacyCardApiClient,
            IssueV1TokenUseCase issueV1TokenUseCase) {
        this.routeDecisionService = routeDecisionService;
        this.legacyCardApiClient = legacyCardApiClient;
        this.issueV1TokenUseCase = issueV1TokenUseCase;
    }

    @PostMapping("/getToken")
    public Mono<ResponseEntity<?>> getToken(@RequestBody(required = false) Mono<String> body,
                                            ServerHttpRequest request) {
        return body.defaultIfEmpty("")
                .flatMap(rawBody -> {
                    V1RouteDecision decision = routeDecisionService.decideAuth(path(request), rawBody);
                    log.info("V1 token路由判定 path={}, target={}, reason={}",
                            path(request), decision.target(), decision.reason());
                    if (decision.isLegacy()) {
                        return legacyCardApiClient.proxy(request, rawBody).map(entity -> (ResponseEntity<?>) entity);
                    }
                    V1CardApiResult result = issueV1TokenUseCase.issueToken(rawBody);
                    return Mono.just(ok(result));
                });
    }

    private static ResponseEntity<V1CardApiResponse<Object>> ok(V1CardApiResult result) {
        return ResponseEntity.ok(V1CardApiResponse.failure(result.code(), result.msg(), result.data()));
    }

    private static String path(ServerHttpRequest request) {
        return request.getPath().pathWithinApplication().value();
    }
}
