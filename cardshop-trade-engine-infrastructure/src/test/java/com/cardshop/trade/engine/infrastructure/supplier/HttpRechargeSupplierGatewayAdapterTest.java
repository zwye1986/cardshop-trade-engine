package com.cardshop.trade.engine.infrastructure.supplier;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.SupplierDispatchGatewayRequest;
import com.cardshop.trade.engine.application.recharge.SupplierGatewayStatus;
import com.cardshop.trade.engine.application.recharge.SupplierInvalidGatewayRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * HTTP 供应商 Gateway fake server 联调测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和
 * 「9.2 作废设计」。D9/D10 启用真实 Worker 前，至少要证明配置化 HTTP 适配器能对本地 fake server
 * 发起真实 JSON 请求、解析供应商成功/处理中响应，并把请求幂等号固定为 outbox event_id。
 */
class HttpRechargeSupplierGatewayAdapterTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private HttpServer server;
    private ExecutorService executor;
    private final List<String> requestBodies = new ArrayList<>();

    @BeforeEach
    void startFakeSupplierServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newSingleThreadExecutor();
        server.setExecutor(executor);
        server.createContext("/dispatch", exchange -> respond(exchange, 200,
                "{\"status\":\"SUCCESS\",\"code\":\"0\",\"message\":\"ok\",\"supplier_order_id\":\"SUP-1\"}"));
        server.createContext("/dispatch-5xx", exchange -> respond(exchange, 500,
                "{\"status\":\"ERROR\",\"code\":\"E_500\",\"message\":\"temporary unavailable\"}"));
        server.createContext("/invalid", exchange -> respond(exchange, 200,
                "{\"status\":\"PROCESSING\",\"code\":\"202\",\"message\":\"invalid accepted\",\"supplier_order_id\":\"SUP-X\"}"));
        server.start();
    }

    @AfterEach
    void stopFakeSupplierServer() {
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void dispatchPostsJsonToFakeSupplierAndParsesSuccessResponse() throws Exception {
        HttpRechargeSupplierGatewayAdapter adapter = adapter();

        var response = adapter.dispatch(new SupplierDispatchGatewayRequest(
                "dispatch-ORDER-1",
                "ORDER-1",
                "OUT-1",
                "A36",
                "13800138000",
                "PARENT-1",
                "SKU-1",
                "FNK_CGS_BX",
                5200L,
                Map.of("outbox_payload", "{\"order_id\":\"ORDER-1\"}")));

        assertThat(response.status()).isEqualTo(SupplierGatewayStatus.SUCCESS);
        assertThat(response.supplierOrderId()).isEqualTo("SUP-1");
        assertThat(response.responseCode()).isEqualTo("0");
        assertThat(requestBodies).singleElement().satisfies(body -> {
            assertThat(body).contains("\"supplier_request_id\":\"dispatch-ORDER-1\"");
            assertThat(body).contains("\"order_id\":\"ORDER-1\"");
            assertThat(body).contains("\"account\":\"13800138000\"");
            assertThat(body).contains("\"order_price\":5200");
        });
    }

    @Test
    void dispatchHttp5xxIsRetryableFailureRatherThanSupplierTerminalFailure() {
        HttpRechargeSupplierGatewayAdapter adapter = adapter("/dispatch-5xx", "/invalid");

        var response = adapter.dispatch(new SupplierDispatchGatewayRequest(
                "dispatch-ORDER-1",
                "ORDER-1",
                "OUT-1",
                "A36",
                "13800138000",
                "PARENT-1",
                "SKU-1",
                "FNK_CGS_BX",
                5200L,
                Map.of("outbox_payload", "{\"order_id\":\"ORDER-1\"}")));

        assertThat(response.status()).isEqualTo(SupplierGatewayStatus.RETRYABLE_FAILED);
        assertThat(response.responseCode()).isEqualTo("500");
        assertThat(response.responseMessage()).contains("not 2xx");
    }

    @Test
    void invalidPostsJsonToFakeSupplierAndParsesProcessingResponse() {
        HttpRechargeSupplierGatewayAdapter adapter = adapter();

        var response = adapter.invalid(new SupplierInvalidGatewayRequest(
                "invalid-ORDER-1",
                "ORDER-1",
                "OUT-1",
                "A36",
                "13800138000",
                "FNK_CGS_BX",
                Map.of("outbox_payload", "{\"reason\":\"merchant invalid\"}")));

        assertThat(response.status()).isEqualTo(SupplierGatewayStatus.PROCESSING);
        assertThat(response.supplierOrderId()).isEqualTo("SUP-X");
        assertThat(response.responseCode()).isEqualTo("202");
        assertThat(requestBodies).singleElement().satisfies(body -> {
            assertThat(body).contains("\"supplier_request_id\":\"invalid-ORDER-1\"");
            assertThat(body).contains("\"out_order_sn\":\"OUT-1\"");
            assertThat(body).contains("\"org_code\":\"A36\"");
        });
    }

    private HttpRechargeSupplierGatewayAdapter adapter() {
        return adapter("/dispatch", "/invalid");
    }

    private HttpRechargeSupplierGatewayAdapter adapter(String dispatchPath, String invalidPath) {
        var endpoint = new HttpRechargeSupplierGatewayAdapter.SupplierChannelEndpoint(
                endpoint(dispatchPath).toString(),
                endpoint(invalidPath).toString(),
                Duration.ofSeconds(2));
        return new HttpRechargeSupplierGatewayAdapter(
                Map.of("FNK_CGS_BX", endpoint),
                OBJECT_MAPPER,
                HttpClient.newHttpClient());
    }

    private URI endpoint(String path) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        requestBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        byte[] responseBytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, responseBytes.length);
        exchange.getResponseBody().write(responseBytes);
        exchange.close();
    }
}
