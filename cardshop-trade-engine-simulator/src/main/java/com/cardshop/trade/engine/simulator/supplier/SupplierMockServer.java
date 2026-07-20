package com.cardshop.trade.engine.simulator.supplier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 供应商 HTTP mock 进程：注册下单/查单四条路径，JSON 协议对齐 Gateway。
 *
 * <p>参见 {@code docs/superpowers/specs/2026-07-20-supplier-mock-design.md}「协议与路径」
 * 与「组件划分」中的 {@code SupplierMockServer} 职责。
 */
public final class SupplierMockServer implements AutoCloseable {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private static final String PATH_DISPATCH = "/supplier/dispatch";
    private static final String PATH_DISPATCH_MINISO = "/tripartite/coupon/batchGrandCoupon";
    private static final String PATH_QUERY = "/supplier/query";
    private static final String PATH_QUERY_MINISO = "/tripartite/coupon/queryGrand";

    private static final Set<String> DISPATCH_PATHS = Set.of(PATH_DISPATCH, PATH_DISPATCH_MINISO);
    private static final Set<String> ALL_PATHS = Set.of(
            PATH_DISPATCH, PATH_DISPATCH_MINISO, PATH_QUERY, PATH_QUERY_MINISO);

    private static final String STATUS_FAILED = "FAILED";
    private static final String CODE_INVALID_REQUEST = "E_MOCK_FAIL";
    private static final String MESSAGE_MISSING_ORDER_KEY = "order_id 或 supplier_request_id 不能为空";
    private static final String MESSAGE_INVALID_BODY = "请求体不是合法 JSON";

    private final HttpServer server;
    private final String bindHost;
    private final int port;
    private final SupplierMockOrderStore store;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor;

    private SupplierMockServer(
            HttpServer server, String bindHost, int port, SupplierMockOrderStore store, ExecutorService executor) {
        this.server = server;
        this.bindHost = bindHost;
        this.port = port;
        this.store = store;
        this.executor = executor;
    }

    /**
     * 按生产配置启动 mock 服务。
     */
    public static SupplierMockServer start(SupplierMockOptions options, SupplierMockOrderStore store) throws IOException {
        Objects.requireNonNull(options, "options must not be null");
        Objects.requireNonNull(store, "store must not be null");
        return start(options.bindHost(), options.port(), store);
    }

    /**
     * 按指定绑定地址与端口启动，供单测使用 {@code port=0} 随机端口。
     */
    static SupplierMockServer start(String bindHost, int port, SupplierMockOrderStore store) throws IOException {
        Objects.requireNonNull(bindHost, "bindHost must not be null");
        Objects.requireNonNull(store, "store must not be null");
        HttpServer server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);
        ExecutorService executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        SupplierMockServer mockServer = new SupplierMockServer(
                server, bindHost, server.getAddress().getPort(), store, executor);
        for (String path : ALL_PATHS) {
            server.createContext(path, mockServer::handle);
        }
        server.start();
        return mockServer;
    }

    /**
     * 对外访问基址，{@code 0.0.0.0} 绑定会映射为 {@code 127.0.0.1}。
     */
    public URI baseUri() {
        String host = "0.0.0.0".equals(bindHost) ? "127.0.0.1" : bindHost;
        return URI.create("http://%s:%d".formatted(host, port));
    }

    private void handle(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            respond(exchange, 405, toJson(failedResponse("method not allowed")));
            return;
        }

        String rawBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        SupplierMockOrderStore.MockResponse response;
        try {
            Map<String, Object> payload = objectMapper.readValue(rawBody, MAP_TYPE);
            String orderKey = extractOrderKey(payload);
            if (orderKey == null) {
                response = failedResponse(MESSAGE_MISSING_ORDER_KEY);
            } else {
                String path = exchange.getRequestURI().getPath();
                response = DISPATCH_PATHS.contains(path)
                        ? store.registerDispatch(orderKey)
                        : store.query(orderKey);
            }
        } catch (Exception ex) {
            response = failedResponse(MESSAGE_INVALID_BODY);
        }
        respond(exchange, 200, toJson(response));
    }

    private static SupplierMockOrderStore.MockResponse failedResponse(String message) {
        return new SupplierMockOrderStore.MockResponse(STATUS_FAILED, CODE_INVALID_REQUEST, message, null);
    }

    /**
     * 订单身份主键：优先 {@code order_id}，缺失时回退 {@code supplier_request_id}。
     */
    private static String extractOrderKey(Map<String, Object> payload) {
        String orderId = textValue(payload.get("order_id"));
        if (orderId != null) {
            return orderId;
        }
        return textValue(payload.get("supplier_request_id"));
    }

    private static String textValue(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    private String toJson(SupplierMockOrderStore.MockResponse response) throws IOException {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("status", response.status());
        node.put("code", response.code());
        node.put("message", response.message());
        if (response.supplierOrderId() != null) {
            node.put("supplier_order_id", response.supplierOrderId());
        } else {
            node.putNull("supplier_order_id");
        }
        return objectMapper.writeValueAsString(node);
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
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
