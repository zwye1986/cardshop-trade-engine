package com.cardshop.trade.engine.infrastructure.supplier;

import com.cardshop.trade.engine.application.recharge.RechargeSupplierGatewayPort;
import com.cardshop.trade.engine.application.recharge.SupplierDispatchGatewayRequest;
import com.cardshop.trade.engine.application.recharge.SupplierGatewayResponse;
import com.cardshop.trade.engine.application.recharge.SupplierInvalidGatewayRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * 基于 HTTP JSON 的供应商 Gateway 适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」和「20.13.11 作废设计」。
 * 首期用于配置化接入 fake server 或简单 HTTP 渠道；复杂渠道 SDK 后续可单独实现同一端口。
 */
public class HttpRechargeSupplierGatewayAdapter implements RechargeSupplierGatewayPort {

    private static final Logger log = LoggerFactory.getLogger(HttpRechargeSupplierGatewayAdapter.class);

    private final Map<String, SupplierChannelEndpoint> channels;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public HttpRechargeSupplierGatewayAdapter(
            Map<String, SupplierChannelEndpoint> channels,
            ObjectMapper objectMapper,
            HttpClient httpClient) {
        this.channels = Map.copyOf(Objects.requireNonNull(channels, "channels must not be null"));
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
    }

    @Override
    public SupplierGatewayResponse dispatch(SupplierDispatchGatewayRequest request) {
        SupplierChannelEndpoint endpoint = channels.get(request.supplierChannel());
        if (endpoint == null || !StringUtils.hasText(endpoint.dispatchUrl())) {
            return SupplierGatewayResponse.failed(
                    "",
                    "SUPPLIER_CHANNEL_NOT_CONFIGURED",
                    "supplier dispatch url not configured",
                    "{}");
        }
        return post(endpoint.dispatchUrl(), endpoint.timeout(), Map.ofEntries(
                Map.entry("supplier_request_id", request.supplierRequestId()),
                Map.entry("order_id", request.orderId()),
                Map.entry("out_order_sn", request.outOrderSn()),
                Map.entry("org_code", request.orgCode()),
                Map.entry("account", request.account()),
                Map.entry("product_id", request.productId()),
                Map.entry("selected_product_id", request.selectedProductId()),
                Map.entry("order_price", request.orderPrice())));
    }

    @Override
    public SupplierGatewayResponse invalid(SupplierInvalidGatewayRequest request) {
        SupplierChannelEndpoint endpoint = channels.get(request.supplierChannel());
        if (endpoint == null || !StringUtils.hasText(endpoint.invalidUrl())) {
            return SupplierGatewayResponse.failed(
                    "",
                    "SUPPLIER_INVALID_NOT_CONFIGURED",
                    "supplier invalid url not configured",
                    "{}");
        }
        return post(endpoint.invalidUrl(), endpoint.timeout(), Map.ofEntries(
                Map.entry("supplier_request_id", request.supplierRequestId()),
                Map.entry("order_id", request.orderId()),
                Map.entry("out_order_sn", request.outOrderSn()),
                Map.entry("org_code", request.orgCode()),
                Map.entry("account", request.account())));
    }

    private SupplierGatewayResponse post(String url, Duration timeout, Map<String, Object> payload) {
        try {
            String body = objectMapper.writeValueAsString(payload);
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return parseResponse(response.statusCode(), response.body());
        } catch (Exception ex) {
            log.error("供应商HTTP调用异常 url={}", url, ex);
            return SupplierGatewayResponse.failed("", "SUPPLIER_HTTP_EXCEPTION", ex.getMessage(), "{}");
        }
    }

    private SupplierGatewayResponse parseResponse(int statusCode, String body) throws java.io.IOException {
        if (statusCode < 200 || statusCode >= 300) {
            return SupplierGatewayResponse.failed("", String.valueOf(statusCode), "supplier http status not 2xx", body);
        }
        JsonNode root = objectMapper.readTree(StringUtils.hasText(body) ? body : "{}");
        String status = text(root, "status", text(root, "order_status", ""));
        String code = text(root, "code", "");
        String message = text(root, "message", text(root, "msg", ""));
        String supplierOrderId = text(root, "supplier_order_id", text(root, "supplierOrderId", ""));
        if (isSuccess(status, code)) {
            return SupplierGatewayResponse.success(supplierOrderId, code, message, body);
        }
        if (isProcessing(status, code)) {
            return SupplierGatewayResponse.processing(supplierOrderId, code, message, body);
        }
        return SupplierGatewayResponse.failed(supplierOrderId, code, message, body);
    }

    private static boolean isSuccess(String status, String code) {
        return "SUCCESS".equalsIgnoreCase(status) || "12".equals(status) || "200".equals(code) || "0".equals(code);
    }

    private static boolean isProcessing(String status, String code) {
        return "PROCESSING".equalsIgnoreCase(status) || "ACCEPTED".equalsIgnoreCase(status) || "13".equals(status)
                || "202".equals(code);
    }

    private static String text(JsonNode root, String field, String fallback) {
        JsonNode value = root.get(field);
        return value == null || value.isNull() ? fallback : value.asText();
    }

    /**
     * 单个供应商渠道的 HTTP 端点配置。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」和「20.13.11 作废设计」。
     *
     * @param dispatchUrl 派单请求地址，用于供应商履约下发
     * @param invalidUrl 作废请求地址，用于已受理订单的供应商侧撤销
     * @param timeout HTTP 请求超时时间；为空、零值或负值时默认 3 秒
     */
    public record SupplierChannelEndpoint(String dispatchUrl, String invalidUrl, Duration timeout) {

        public SupplierChannelEndpoint {
            timeout = timeout == null || timeout.isZero() || timeout.isNegative()
                    ? Duration.ofSeconds(3)
                    : timeout;
        }
    }
}
