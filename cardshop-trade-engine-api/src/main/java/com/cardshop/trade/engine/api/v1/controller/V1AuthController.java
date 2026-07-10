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
import reactor.core.scheduler.Schedulers;

/**
 * 原卡管兼容的 `/v1/cardapi/auth` 入口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * 灰度命中新交易引擎时，真实签发会访问旧商户表和共享 Redis token key；这些依赖当前仍通过同步端口
 * 适配旧 MyBatis/Redis 语义，因此必须在 controller 层显式放到 boundedElastic，避免 WebFlux Netty
 * 事件循环线程直接等待阻塞 IO。
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
                    return issueTokenOnBlockingScheduler(rawBody).map(V1AuthController::ok);
                });
    }

    /**
     * `/auth/getToken` 的签发链路会读取 `mc_ext_depart` 并写入 Redis
     * {@code token:app_id:{appId}}，参见详细设计「3. API/V1 接入层」。
     * 在 V1TokenStorePort 改造成全响应式之前，统一在这里设置阻塞边界，保证调用方即使运行在
     * Netty event-loop 上，也不会把 worker 线程卡在同步 IO 等待上。
     */
    private Mono<V1CardApiResult> issueTokenOnBlockingScheduler(String rawBody) {
        return Mono.fromCallable(() -> issueV1TokenUseCase.issueToken(rawBody))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private static ResponseEntity<V1CardApiResponse<Object>> ok(V1CardApiResult result) {
        return ResponseEntity.ok(V1CardApiResponse.failure(result.code(), result.msg(), result.data()));
    }

    private static String path(ServerHttpRequest request) {
        return request.getPath().pathWithinApplication().value();
    }
}
