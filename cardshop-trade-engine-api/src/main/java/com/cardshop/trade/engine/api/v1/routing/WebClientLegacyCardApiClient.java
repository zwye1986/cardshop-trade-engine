package com.cardshop.trade.engine.api.v1.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.net.URI;

/**
 * 使用 WebClient 转发原卡管 `/v1/cardapi` 请求。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * 代理只负责透传，不解析业务明文；日志只记录路径、HTTP 方法和配置可用性，不输出 token 或请求体。
 */
@Component
public class WebClientLegacyCardApiClient implements LegacyCardApiClient {

    private static final Logger log = LoggerFactory.getLogger(WebClientLegacyCardApiClient.class);
    private static final String LEGACY_BASE_URL_MISSING_BODY =
            "{\"code\":\"505\",\"msg\":\"旧系统代理地址未配置\",\"data\":null}";

    private final WebClient webClient;
    private final V1RoutingSettings settings;

    public WebClientLegacyCardApiClient(WebClient.Builder webClientBuilder, V1RoutingSettings settings) {
        this.webClient = webClientBuilder.build();
        this.settings = settings;
    }

    @Override
    public Mono<ResponseEntity<String>> proxy(ServerHttpRequest request, String rawBody) {
        if (!StringUtils.hasText(settings.legacyBaseUrl())) {
            log.warn("旧系统代理地址未配置 method={}, path={}", request.getMethod(), request.getPath().value());
            return Mono.just(ResponseEntity.status(502)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(LEGACY_BASE_URL_MISSING_BODY));
        }

        URI targetUri = buildTargetUri(request);
        WebClient.RequestBodySpec spec = webClient.method(request.getMethod())
                .uri(targetUri)
                .headers(headers -> copyProxyHeaders(request, headers));

        return (StringUtils.hasText(rawBody)
                ? spec.body(BodyInserters.fromValue(rawBody)).exchangeToMono(response -> response.toEntity(String.class))
                : spec.exchangeToMono(response -> response.toEntity(String.class)))
                .timeout(settings.proxyTimeout())
                .map(entity -> ResponseEntity.status(entity.getStatusCode())
                        .headers(filterResponseHeaders(entity.getHeaders()))
                        .body(entity.getBody()))
                .doOnSubscribe(ignored -> log.debug(
                        "代理旧系统/v1/cardapi method={}, path={}, queryPresent={}",
                        request.getMethod(),
                        request.getPath().value(),
                        StringUtils.hasText(request.getURI().getRawQuery())));
    }

    private URI buildTargetUri(ServerHttpRequest request) {
        String baseUrl = trimTrailingSlash(settings.legacyBaseUrl());
        String path = request.getURI().getRawPath();
        String query = request.getURI().getRawQuery();
        String value = StringUtils.hasText(query) ? baseUrl + path + "?" + query : baseUrl + path;
        return URI.create(value);
    }

    private static void copyProxyHeaders(ServerHttpRequest request, HttpHeaders headers) {
        request.getHeaders().forEach((name, values) -> {
            if (HttpHeaders.HOST.equalsIgnoreCase(name) || HttpHeaders.CONTENT_LENGTH.equalsIgnoreCase(name)) {
                return;
            }
            headers.put(name, values);
        });
    }

    private static HttpHeaders filterResponseHeaders(HttpHeaders source) {
        HttpHeaders headers = new HttpHeaders();
        source.forEach((name, values) -> {
            if (HttpHeaders.TRANSFER_ENCODING.equalsIgnoreCase(name) || HttpHeaders.CONTENT_LENGTH.equalsIgnoreCase(name)) {
                return;
            }
            headers.put(name, values);
        });
        return headers;
    }

    private static String trimTrailingSlash(String value) {
        String trimmed = value.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }
}
