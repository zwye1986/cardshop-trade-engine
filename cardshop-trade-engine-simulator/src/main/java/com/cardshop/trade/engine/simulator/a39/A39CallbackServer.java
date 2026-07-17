package com.cardshop.trade.engine.simulator.a39;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 商户侧回调接收器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。测试工具必须像真实商户一样
 * 返回旧协议 JSON：验签成功返回 {@code code=200}，失败返回非 200 业务码，让平台保留重试语义。
 * 验签按发送方原始字段名计算，兼容接口文档 snake_case 与当前内部回调 signer 的 camelCase。
 */
public final class A39CallbackServer implements AutoCloseable {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };
    private static final String SUCCESS_BODY = "{\"code\":200,\"msg\":\"接收成功\"}";
    private static final String FAILED_BODY = "{\"code\":500,\"msg\":\"接收失败，请重试\"}";

    private final HttpServer server;
    private final String bindHost;
    private final int port;
    private final String aesKey;
    private final V1Sm3Signer signer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<CallbackRecord> receivedCallbacks = Collections.synchronizedList(new ArrayList<>());

    private A39CallbackServer(HttpServer server, String bindHost, int port, String aesKey, V1Sm3Signer signer) {
        this.server = server;
        this.bindHost = bindHost;
        this.port = port;
        this.aesKey = aesKey;
        this.signer = signer;
    }

    public static A39CallbackServer start(String bindHost, int port, String aesKey, V1Sm3Signer signer) throws IOException {
        Objects.requireNonNull(bindHost, "bindHost must not be null");
        Objects.requireNonNull(aesKey, "aesKey must not be null");
        Objects.requireNonNull(signer, "signer must not be null");
        HttpServer server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);
        A39CallbackServer callbackServer = new A39CallbackServer(server, bindHost, server.getAddress().getPort(), aesKey, signer);
        server.createContext("/callback", callbackServer::handle);
        server.start();
        return callbackServer;
    }

    public List<CallbackRecord> receivedCallbacks() {
        synchronized (receivedCallbacks) {
            return List.copyOf(receivedCallbacks);
        }
    }

    public String callbackUrl(String path) {
        String host = "0.0.0.0".equals(bindHost) ? "127.0.0.1" : bindHost;
        return "http://%s:%d%s".formatted(host, port, path);
    }

    private void handle(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            respond(exchange, 405, FAILED_BODY);
            return;
        }
        String rawBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, Object> payload;
        boolean signatureValid = false;
        try {
            payload = objectMapper.readValue(rawBody, MAP_TYPE);
            signatureValid = signer.verifyCallback(payload, aesKey);
        } catch (Exception ex) {
            payload = Map.of();
        }
        receivedCallbacks.add(new CallbackRecord(exchange.getRequestURI().getPath(), rawBody, payload, signatureValid));
        respond(exchange, 200, signatureValid ? SUCCESS_BODY : FAILED_BODY);
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }

    public record CallbackRecord(String path, String rawBody, Map<String, Object> payload, boolean signatureValid) {
    }
}
