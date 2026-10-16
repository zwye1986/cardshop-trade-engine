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
 * 验签严格使用V1.2 snake_case字段，并核对可信订单keyId绑定。
 */
public final class A39CallbackServer implements AutoCloseable {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };
    private static final String SUCCESS_BODY = "{\"code\":200,\"msg\":\"接收成功\"}";
    private static final String FAILED_BODY = "{\"code\":500,\"msg\":\"接收失败，请重试\"}";

    private static final System.Logger LOG = System.getLogger(A39CallbackServer.class.getName());
    private static final int ERROR_SAMPLE_INTERVAL = 100;
    private final java.util.concurrent.atomic.AtomicLong invalidPayloadCount = new java.util.concurrent.atomic.AtomicLong();
    private final HttpServer server;
    private final String bindHost;
    private final int port;
    // 下单前绑定，防止回调早于同步响应；刷新token不得覆盖旧订单绑定，见详细设计3.1。
    private final java.util.concurrent.ConcurrentMap<String, Credentials> bindings = new java.util.concurrent.ConcurrentHashMap<>();
    private final V12Signer signer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<CallbackRecord> receivedCallbacks = Collections.synchronizedList(new ArrayList<>());

    private A39CallbackServer(HttpServer server, String bindHost, int port, V12Signer signer) {
        this.server = server;
        this.bindHost = bindHost;
        this.port = port;
        this.signer = signer;
    }

    public static A39CallbackServer start(String bindHost, int port, V12Signer signer) throws IOException {
        Objects.requireNonNull(bindHost, "bindHost must not be null");
        Objects.requireNonNull(signer, "signer must not be null");
        HttpServer server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);
        A39CallbackServer callbackServer = new A39CallbackServer(server, bindHost, server.getAddress().getPort(), signer);
        server.createContext("/callback", callbackServer::handle);
        server.start();
        return callbackServer;
    }

    public void bindOrder(String outOrderSn, Credentials credential) {
        Credentials existing = bindings.putIfAbsent(outOrderSn, credential);
        if (existing != null && !existing.keyId().equals(credential.keyId())) {
            throw new IllegalStateException("Order already bound to another signing credential");
        }
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
            Object orderSn = payload.get("out_order_sn");
            Credentials credential = orderSn instanceof String ? bindings.get(orderSn) : null;
            signatureValid = credential != null && signer.verifyCallback(payload, (String) orderSn, credential);
        } catch (Exception ex) {
            // 外部坏报文属预期拒绝，每100次采样warn及异常栈，避免压测日志风暴；见详细设计3.1。
            long count = invalidPayloadCount.incrementAndGet();
            if ((count - 1) % ERROR_SAMPLE_INTERVAL == 0) {
                LOG.log(System.Logger.Level.WARNING, "V1.2 callback rejected, count=" + count, ex);
            }
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
