package com.cardshop.trade.engine.simulator.a39;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
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
 * 验签严格使用V1.2 snake_case字段；按可信商户密钥环验签后，独立核对本地订单。
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
    // 每个实例只服务启动配置指定的商户；报文字段不能切换商户范围，参见详细设计3.1。
    // 密钥环不保存token，也不与订单绑定，普通token刷新只重复登记同一商户密钥。
    private final java.util.concurrent.ConcurrentMap<String, SigningKey> keys = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentMap<String, String> orders = new java.util.concurrent.ConcurrentHashMap<>();
    private final V12Signer signer;
    // 在验签前拒绝歧义JSON，避免重复keyId覆盖或尾随报文绕过完整输入校验，参见详细设计3.1。
    private final ObjectMapper objectMapper = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
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

    /** 仅登记本商户可信的旧直连凭证；历史密钥规则参见详细设计3.1，银行目标见文末认证修订。 */
    public void registerCredentials(Credentials credential) {
        if (credential.keyId() == null || credential.keyId().isBlank()) {
            // TODO：完成2104银行通知接收认证，参见详细设计「交行开放平台通知认证修订 2026-10-19」。
            // 新接口不返回keyId，不能从外部回调学习版本或回退为无签名接收；业务请求发送前明确拒绝旧场景。
            throw new IllegalStateException("Legacy callback requires trusted keyId; bank 2104 callback is not implemented");
        }
        keys.compute(credential.keyId(), (id, existing) -> {
            if (existing != null && !existing.signKey().equals(credential.signKey())) {
                throw new IllegalStateException("Signing keyId already registered with another key");
            }
            // 同keyId再次获取token不能恢复已撤销密钥。
            return existing == null ? new SigningKey(id, credential.signKey(), false) : existing;
        });
    }

    /** 撤销立即阻止该keyId的后续回调；不尝试用最新密钥兜底，参见详细设计3.1。 */
    public void revokeKey(String keyId) {
        keys.computeIfPresent(keyId, (id, key) -> new SigningKey(id, key.signKey(), true));
    }

    /** 发送前独立登记可信订单和账号，兼容回调早于同步响应；订单没有keyId字段，参见详细设计3.1。 */
    public void registerOrder(String outOrderSn, String account) {
        Objects.requireNonNull(outOrderSn, "outOrderSn must not be null");
        Objects.requireNonNull(account, "account must not be null");
        String existing = orders.putIfAbsent(outOrderSn, account);
        if (existing != null && !existing.equals(account)) {
            throw new IllegalStateException("Order already registered with another account");
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
            Object keyId = payload.get("keyId");
            SigningKey key = keyId instanceof String ? keys.get(keyId) : null;
            // 先验商户密钥及签名，再核对本地可信订单/账号；不要求token有效，参见详细设计3.1。
            if (key != null && !key.revoked() && signer.verifyCallback(payload, key.keyId(), key.signKey())) {
                Object orderSn = payload.get("out_order_sn");
                String account = orderSn instanceof String ? orders.get(orderSn) : null;
                signatureValid = account != null && account.equals(payload.get("account"));
            }
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

    // 密钥仅内存保留供模拟器使用，禁止将此记录输出到日志。
    private record SigningKey(String keyId, String signKey, boolean revoked) {
        @Override public String toString() { return "SigningKey[redacted]"; }
    }

    public record CallbackRecord(String path, String rawBody, Map<String, Object> payload, boolean signatureValid) {
    }
}
