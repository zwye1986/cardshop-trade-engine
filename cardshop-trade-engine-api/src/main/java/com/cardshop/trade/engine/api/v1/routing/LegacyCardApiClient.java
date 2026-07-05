package com.cardshop.trade.engine.api.v1.routing;

import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import reactor.core.publisher.Mono;

/**
 * 原卡管 `/v1/cardapi` 代理客户端。
 */
public interface LegacyCardApiClient {

    Mono<ResponseEntity<String>> proxy(ServerHttpRequest request, String rawBody);
}
