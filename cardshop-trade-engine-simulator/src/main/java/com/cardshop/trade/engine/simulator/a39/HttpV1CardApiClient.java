package com.cardshop.trade.engine.simulator.a39;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 基于 JDK {@link HttpClient} 的 V1 客户端。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。该客户端仅用于
 * dev/test 业务模拟，日志和异常文本不输出 {@code app_secret/aesKey/token}。
 */
public final class HttpV1CardApiClient implements V1CardApiClient {

    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final URI baseUri;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final LegacySm4BodyEncoder sm4BodyEncoder;
    private final Consumer<String> logSink;
    private final Duration requestTimeout;
    private final boolean logHttpTraffic;

    public HttpV1CardApiClient(String baseUrl) {
        this(URI.create(baseUrl), HttpClient.newHttpClient(), new ObjectMapper(), new LegacySm4BodyEncoder(), System.out::println,
                DEFAULT_REQUEST_TIMEOUT, true);
    }

    public HttpV1CardApiClient(String baseUrl, Duration requestTimeout, boolean logHttpTraffic) {
        this(URI.create(baseUrl), HttpClient.newHttpClient(), new ObjectMapper(), new LegacySm4BodyEncoder(), System.out::println,
                requestTimeout, logHttpTraffic);
    }

    HttpV1CardApiClient(URI baseUri, HttpClient httpClient, ObjectMapper objectMapper, LegacySm4BodyEncoder sm4BodyEncoder) {
        this(baseUri, httpClient, objectMapper, sm4BodyEncoder, ignored -> {
        }, DEFAULT_REQUEST_TIMEOUT, true);
    }

    HttpV1CardApiClient(
            URI baseUri,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            LegacySm4BodyEncoder sm4BodyEncoder,
            Consumer<String> logSink) {
        this(baseUri, httpClient, objectMapper, sm4BodyEncoder, logSink, DEFAULT_REQUEST_TIMEOUT, true);
    }

    HttpV1CardApiClient(
            URI baseUri,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            LegacySm4BodyEncoder sm4BodyEncoder,
            Consumer<String> logSink,
            Duration requestTimeout,
            boolean logHttpTraffic) {
        this.baseUri = Objects.requireNonNull(baseUri, "baseUri must not be null");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.sm4BodyEncoder = Objects.requireNonNull(sm4BodyEncoder, "sm4BodyEncoder must not be null");
        this.logSink = Objects.requireNonNull(logSink, "logSink must not be null");
        this.requestTimeout = requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative()
                ? DEFAULT_REQUEST_TIMEOUT
                : requestTimeout;
        this.logHttpTraffic = logHttpTraffic;
    }

    @Override
    public String getToken(String appId, String appSecret) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("app_id", appId);
        payload.put("app_secret", appSecret);
        ApiResponse response = post("/v1/cardapi/auth/getToken", null, payload, false, null);
        Object token = response.data().get("token");
        if (!"200".equals(response.code()) || token == null || String.valueOf(token).isBlank()) {
            throw new IllegalStateException("getToken failed: code=%s, msg=%s".formatted(response.code(), response.msg()));
        }
        return String.valueOf(token);
    }

    @Override
    public ApiResponse createRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) throws Exception {
        return post("/v1/cardapi/order/createRechargeOrder", token, payload, sm4Enabled, aesKey);
    }

    @Override
    public ApiResponse getRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) throws Exception {
        return post("/v1/cardapi/order/getRechargeOrder", token, payload, sm4Enabled, aesKey);
    }

    private ApiResponse post(String path, String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) throws Exception {
        long totalStartNanos = System.nanoTime();
        String json = objectMapper.writeValueAsString(payload);
        String body = sm4Enabled ? sm4BodyEncoder.encode(json, aesKey) : json;
        URI requestUri = uri(path);
        String contentType = sm4Enabled ? "text/plain; charset=UTF-8" : "application/json";
        HttpRequest.Builder builder = HttpRequest.newBuilder(requestUri)
                .timeout(requestTimeout)
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (token != null && !token.isBlank()) {
            builder.header("Authorization", token);
        }
        if (logHttpTraffic) {
            logRequestCurl(path, requestUri, token, contentType, sm4Enabled ? body : objectMapper.writeValueAsString(sanitizedPayload(payload)));
        }
        long prepareNanos = System.nanoTime() - totalStartNanos;
        /*
         * ofInputStream：send 返回时响应头已就绪，随后再读 body，便于拆分 untilHeaders / bodyRead。
         * untilHeaders 仍聚合 DNS/建连/TLS/写请求/等首包，JDK HttpClient 无法再细分。
         * 参见 docs/superpowers/specs/2026-07-15-a39-simulator-load-test-design.md「客户端侧耗时拆分」。
         */
        long untilHeadersStartNanos = System.nanoTime();
        HttpResponse<InputStream> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
        long untilHeadersNanos = System.nanoTime() - untilHeadersStartNanos;
        long bodyReadStartNanos = System.nanoTime();
        String responseBody;
        try (InputStream inputStream = response.body()) {
            responseBody = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
        long bodyReadNanos = System.nanoTime() - bodyReadStartNanos;
        if (logHttpTraffic) {
            logSink.accept("[A39][HTTP][response] path=%s status=%d body=%s".formatted(path, response.statusCode(), responseBody));
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("HTTP request failed: path=%s, status=%d".formatted(path, response.statusCode()));
        }
        long parseStartNanos = System.nanoTime();
        ApiResponse parsed = parseResponse(responseBody);
        long parseNanos = System.nanoTime() - parseStartNanos;
        long totalNanos = System.nanoTime() - totalStartNanos;
        return new ApiResponse(
                parsed.code(),
                parsed.msg(),
                parsed.data(),
                new ClientTiming(prepareNanos, untilHeadersNanos, bodyReadNanos, parseNanos, totalNanos));
    }

    private void logRequestCurl(String path, URI requestUri, String token, String contentType, String body) {
        StringBuilder curl = new StringBuilder(256)
                .append("curl -i -X POST ")
                .append(shellQuote(requestUri.toString()))
                .append(" -H ")
                .append(shellQuote("Content-Type: " + contentType));
        if (token != null && !token.isBlank()) {
            curl.append(" -H ").append(shellQuote("Authorization: ***"));
        }
        curl.append(" --data-raw ").append(shellQuote(body));
        logSink.accept("[A39][HTTP][request] path=%s curl=%s".formatted(path, curl));
    }

    private static Map<String, Object> sanitizedPayload(Map<String, Object> payload) {
        Map<String, Object> sanitized = new LinkedHashMap<>(payload);
        if (sanitized.containsKey("app_secret")) {
            sanitized.put("app_secret", "***");
        }
        return sanitized;
    }

    private static String shellQuote(String value) {
        return "'" + value.replace("'", "'\\''") + "'";
    }

    URI uri(String path) {
        String base = baseUri.toString();
        String normalizedBase = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        return URI.create(normalizedBase + normalizedPath);
    }

    private ApiResponse parseResponse(String body) throws Exception {
        JsonNode root = objectMapper.readTree(body == null || body.isBlank() ? "{}" : body);
        String code = root.has("code") ? root.get("code").asText() : "";
        String msg = root.has("msg") ? root.get("msg").asText() : "";
        Map<String, Object> data = root.has("data") && root.get("data").isObject()
                ? objectMapper.convertValue(root.get("data"), MAP_TYPE)
                : Map.of();
        return new ApiResponse(code, msg, data);
    }
}
