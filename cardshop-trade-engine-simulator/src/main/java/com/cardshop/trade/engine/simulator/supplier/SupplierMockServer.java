package com.cardshop.trade.engine.simulator.supplier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 供应商 HTTP mock：保留通用下单/查单，补充MINISO原生发券、查单、作废。
 * 原生桩只服务本地合成数据，不验证供应商签名，不证明真实供应商或银行验收。
 * 参见 {@code docs/交行直充业务系统详细设计.md}「9.8」。
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

    private static final String PATH_VOID_MINISO = "/tripartite/coupon/abandonUserCoupon";
    private static final String UNKNOWN_AFTER_ISSUE = "UNKNOWN_AFTER_ISSUE";
    private static final int MAX_MOCK_COUPONS = 10;
    private static final int MINISO_CODE_SUCCESS = 200;
    private static final String MOCK_BEGIN_TIME = "2026-01-01 00:00:00";
    private static final String MOCK_END_TIME = "2099-12-31 23:59:59";
    private static final int COUPON_UNUSED = 0;
    private static final int COUPON_VOID = 3;
    private static final System.Logger LOGGER = System.getLogger(SupplierMockServer.class.getName());
    private static final AtomicLong LAST_FAILURE_LOG = new AtomicLong();
    private static final long LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);

    private static final Set<String> DISPATCH_PATHS = Set.of(PATH_DISPATCH, PATH_DISPATCH_MINISO);
    private static final Set<String> ALL_PATHS = Set.of(
            PATH_DISPATCH, PATH_DISPATCH_MINISO, PATH_QUERY, PATH_QUERY_MINISO, PATH_VOID_MINISO);

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
    // 每个进程独立的合成凭据，按原orderNo冻结；重投不能再发新券。参见详细设计9.8无凭据恢复边界。
    private final Map<String, List<Map<String, Object>>> minisoOrders = new ConcurrentHashMap<>();

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
        String path = exchange.getRequestURI().getPath();
        // HttpServer context按前缀匹配，必须拒绝后缀，防止未知接口被误认为成功。
        if (!ALL_PATHS.contains(path)) {
            respond(exchange, 404, toJson(failedResponse("unknown path")));
            return;
        }
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            respond(exchange, 405, toJson(failedResponse("method not allowed")));
            return;
        }

        String rawBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        SupplierMockOrderStore.MockResponse response;
        try {
            Map<String, Object> payload = objectMapper.readValue(rawBody, MAP_TYPE);
            if (PATH_VOID_MINISO.equals(path) || (path.startsWith("/tripartite/") && payload.containsKey("orderNo"))) {
                respond(exchange, 200, objectMapper.writeValueAsString(handleMiniso(path, payload,
                        exchange.getRequestHeaders().getFirst("X-Mock-Miniso-Scenario"))));
                return;
            }
            String orderKey = extractOrderKey(payload);
            if (orderKey == null) {
                response = failedResponse(MESSAGE_MISSING_ORDER_KEY);
            } else {
                response = DISPATCH_PATHS.contains(path)
                        ? store.registerDispatch(orderKey)
                        : store.query(orderKey);
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException | IllegalArgumentException ex) {
            logFailure(ex, System.Logger.Level.WARNING);
            response = failedResponse(MESSAGE_INVALID_BODY);
        } catch (RuntimeException ex) {
            logFailure(ex, System.Logger.Level.ERROR);
            response = failedResponse("mock internal failure");
        }
        respond(exchange, 200, toJson(response));
    }

    /**
     * 最小MINISO离线桩：原生orderNo协议与旧通用别名互不混用。
     * UNKNOWN_AFTER_ISSUE仅隐藏首次发券响应，原单查单仍返冻结凭据；未知订单不伪造未发券证明。
     * 不实现共享inbox/guard/资金裁决；这些证据由真实两仓库合同测试给出。参见详细设计9.8。
     */
    private synchronized Map<String, Object> handleMiniso(String path, Map<String, Object> payload, String scenario) {
        if (PATH_VOID_MINISO.equals(path)) return voidMiniso(payload);
        String orderNo = requiredText(payload.get("orderNo"));
        if (PATH_QUERY_MINISO.equals(path)) {
            List<Map<String, Object>> coupons = minisoOrders.get(orderNo);
            return coupons == null ? unknownMiniso() : minisoResponse(coupons);
        }
        if (scenario != null && !UNKNOWN_AFTER_ISSUE.equals(scenario)) {
            throw new IllegalArgumentException("unsupported mock scenario");
        }
        JsonNode body = objectMapper.valueToTree(payload);
        String phone = requiredText(payload.get("phone"));
        JsonNode batches = body.path("batchList");
        if (!batches.isArray() || batches.size() != 1) throw new IllegalArgumentException("one mock batch required");
        JsonNode batch = batches.get(0).path("batchId");
        JsonNode number = batches.get(0).path("number");
        if (!batch.isInt() || batch.intValue() <= 0 || !number.isInt()
                || number.intValue() < 1 || number.intValue() > MAX_MOCK_COUPONS) {
            throw new IllegalArgumentException("invalid mock batch");
        }
        List<Map<String, Object>> existing = minisoOrders.get(orderNo);
        if (existing != null) {
            if (existing.size() != number.intValue() || !phone.equals(existing.get(0).get("phone"))
                    || !Integer.valueOf(batch.intValue()).equals(existing.get(0).get("batchId"))) {
                throw new IllegalArgumentException("mock order identity conflict");
            }
            return minisoResponse(existing);
        }
        List<Map<String, Object>> coupons = new ArrayList<>();
        for (int index = 0; index < number.intValue(); index++) {
            Map<String, Object> coupon = new LinkedHashMap<>();
            coupon.put("batchId", batch.intValue());
            coupon.put("phone", phone);
            coupon.put("couponCode", "MOCK-MINISO-" + UUID.randomUUID());
            coupon.put("beginTime", MOCK_BEGIN_TIME);
            coupon.put("endTime", MOCK_END_TIME);
            coupon.put("status", COUPON_UNUSED);
            coupons.add(coupon);
        }
        minisoOrders.put(orderNo, coupons);
        return UNKNOWN_AFTER_ISSUE.equals(scenario) ? unknownMiniso() : minisoResponse(coupons);
    }

    /** 精确逐券作废，未知券进入failed；支持旧端逗号串与新端数组，重复作废不新建凭据。见详细设计9.8。 */
    private Map<String, Object> voidMiniso(Map<String, Object> payload) {
        Object value = payload.get("couponCodes");
        List<?> codes = value instanceof String text ? List.of(text.split(",", -1))
                : value instanceof List<?> list ? list : List.of();
        if (codes.isEmpty() || codes.size() > MAX_MOCK_COUPONS || new HashSet<>(codes).size() != codes.size()) {
            throw new IllegalArgumentException("invalid mock coupon list");
        }
        // 在任何修改前验证整个输入，避免非法尾项产生隐蔽部分副作用。
        List<String> validated = codes.stream().map(SupplierMockServer::requiredText).toList();
        List<Map<String, String>> succeed = new ArrayList<>();
        List<Map<String, String>> failed = new ArrayList<>();
        for (String code : validated) {
            Map<String, Object> found = minisoOrders.values().stream().flatMap(List::stream)
                    .filter(coupon -> code.equals(coupon.get("couponCode"))).findFirst().orElse(null);
            if (found == null) {
                failed.add(Map.of("couponCode", code, "msg", "mock coupon unknown"));
            } else {
                found.put("status", COUPON_VOID);
                succeed.add(Map.of("couponCode", code, "msg", "mock coupon void"));
            }
        }
        return minisoResponse(Map.of("succeed", succeed, "failed", failed));
    }

    private static String requiredText(Object value) {
        if (!(value instanceof String text) || text.isBlank()) throw new IllegalArgumentException("mock text required");
        return text;
    }

    private static Map<String, Object> minisoResponse(Object data) {
        return Map.of("code", MINISO_CODE_SUCCESS, "msg", "mock only", "data", data);
    }

    private static Map<String, Object> unknownMiniso() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("code", "MOCK_RESULT_UNKNOWN");
        response.put("msg", "mock result unknown; query original order; no refund evidence");
        response.put("data", null);
        return response;
    }

    /** 高频非法请求限分钟抽样，仅保留类型和栈帧，禁止输出解析正文或字段原文。见详细设计9.8。 */
    private static void logFailure(Exception exception, System.Logger.Level level) {
        long now = System.currentTimeMillis();
        long previous = LAST_FAILURE_LOG.get();
        if (now - previous >= LOG_INTERVAL_MS && LAST_FAILURE_LOG.compareAndSet(previous, now)) {
            Exception safe = new Exception("supplier mock failure: " + exception.getClass().getSimpleName());
            safe.setStackTrace(exception.getStackTrace());
            LOGGER.log(level, "supplier mock request unavailable", safe);
        }
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
            logFailure(ex, System.Logger.Level.WARNING);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
