package com.cardshop.trade.engine.infrastructure.supplier;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.logging.LogStormGuard.LogDecision;
import com.cardshop.trade.engine.application.recharge.RechargeSupplierGatewayPort;
import com.cardshop.trade.engine.application.recharge.SupplierDispatchGatewayRequest;
import com.cardshop.trade.engine.application.recharge.SupplierGatewayResponse;
import com.cardshop.trade.engine.application.recharge.SupplierInvalidGatewayRequest;
import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
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
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和「9.2 作废设计」。
 * 首期用于配置化接入 fake server 或简单 HTTP 渠道；复杂渠道 SDK 后续可单独实现同一端口。
 */
public class HttpRechargeSupplierGatewayAdapter implements RechargeSupplierGatewayPort {

    private static final Logger log = LoggerFactory.getLogger(HttpRechargeSupplierGatewayAdapter.class);
    private static final int HTTP_SUCCESS_MIN = 200;
    private static final int HTTP_SUCCESS_MAX_EXCLUSIVE = 300;
    private static final Duration DEFAULT_SUPPLIER_TIMEOUT = Duration.ofSeconds(3);
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String EMPTY_JSON = "{}";
    private static final String FIELD_STATUS = "status";
    private static final String FIELD_ORDER_STATUS = "order_status";
    private static final String FIELD_CODE = "code";
    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_MSG = "msg";
    private static final String FIELD_SUPPLIER_ORDER_ID = "supplier_order_id";
    private static final String FIELD_SUPPLIER_ORDER_ID_CAMEL = "supplierOrderId";
    private static final String STATUS_SUCCESS = "SUCCESS";
    private static final String STATUS_PROCESSING = "PROCESSING";
    private static final String STATUS_ACCEPTED = "ACCEPTED";
    private static final String CODE_SUCCESS_HTTP = "200";
    private static final String CODE_SUCCESS_ZERO = "0";
    private static final String CODE_PROCESSING_HTTP = "202";
    private static final String ORDER_STATUS_SUCCESS = Integer.toString(LegacyOrderStatusCode.SUCCESS.code());
    private static final String ORDER_STATUS_SUPPLIER_ACCEPTED =
            Integer.toString(LegacyOrderStatusCode.SUPPLIER_ACCEPTED.code());
    private static final String LOG_EVENT_HTTP_EXCEPTION = "supplier-gateway.http.exception";

    private final Map<String, SupplierChannelEndpoint> channels;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final LogStormGuard logStormGuard = LogStormGuard.defaultGuard();

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
                    EMPTY_JSON);
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
                    EMPTY_JSON);
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
                    .header(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return parseResponse(response.statusCode(), response.body());
        } catch (Exception ex) {
            LogDecision decision = logStormGuard.shouldLog(LOG_EVENT_HTTP_EXCEPTION);
            if (decision.shouldLog()) {
                log.error("供应商HTTP调用异常 url={}, occurrence={}, sampled={}",
                        url,
                        decision.occurrence(),
                        decision.sampledOccurrence(),
                        ex);
            }
            /*
             * docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」要求网络超时、连接异常这类
             * 不可确认窗口回到 outbox 退避重试。这里不能返回明确 FAILED，否则 application Worker
             * 会按供应商终态失败释放余额并创建商户失败回调，SUP-002 会被误判为业务失败。
             */
            return SupplierGatewayResponse.retryableFailed("", "SUPPLIER_HTTP_EXCEPTION", ex.getMessage(), EMPTY_JSON);
        }
    }

    private SupplierGatewayResponse parseResponse(int statusCode, String body) throws java.io.IOException {
        if (statusCode < HTTP_SUCCESS_MIN || statusCode >= HTTP_SUCCESS_MAX_EXCLUSIVE) {
            /*
             * HTTP 5xx/网关错误只证明供应商入口暂不可用，不代表订单在供应商侧明确失败；按可重试失败
             * 交回 outbox。若供应商业务拒绝，应返回 2xx + 业务失败状态，由下方 JSON 解析为 FAILED。
             */
            return SupplierGatewayResponse.retryableFailed(
                    "",
                    String.valueOf(statusCode),
                    "supplier http status not 2xx",
                    body);
        }
        JsonNode root = objectMapper.readTree(StringUtils.hasText(body) ? body : EMPTY_JSON);
        String status = text(root, FIELD_STATUS, text(root, FIELD_ORDER_STATUS, ""));
        String code = text(root, FIELD_CODE, "");
        String message = text(root, FIELD_MESSAGE, text(root, FIELD_MSG, ""));
        String supplierOrderId = text(root, FIELD_SUPPLIER_ORDER_ID, text(root, FIELD_SUPPLIER_ORDER_ID_CAMEL, ""));
        if (isSuccess(status, code)) {
            return SupplierGatewayResponse.success(supplierOrderId, code, message, body);
        }
        if (isProcessing(status, code)) {
            return SupplierGatewayResponse.processing(supplierOrderId, code, message, body);
        }
        return SupplierGatewayResponse.failed(supplierOrderId, code, message, body);
    }

    private static boolean isSuccess(String status, String code) {
        return STATUS_SUCCESS.equalsIgnoreCase(status)
                || ORDER_STATUS_SUCCESS.equals(status)
                || CODE_SUCCESS_HTTP.equals(code)
                || CODE_SUCCESS_ZERO.equals(code);
    }

    private static boolean isProcessing(String status, String code) {
        return STATUS_PROCESSING.equalsIgnoreCase(status)
                || STATUS_ACCEPTED.equalsIgnoreCase(status)
                || ORDER_STATUS_SUPPLIER_ACCEPTED.equals(status)
                || CODE_PROCESSING_HTTP.equals(code);
    }

    private static String text(JsonNode root, String field, String fallback) {
        JsonNode value = root.get(field);
        return value == null || value.isNull() ? fallback : value.asText();
    }

    /**
     * 单个供应商渠道的 HTTP 端点配置。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和「9.2 作废设计」。
     *
     * @param dispatchUrl 派单请求地址，用于供应商履约下发
     * @param invalidUrl 作废请求地址，用于已受理订单的供应商侧撤销
     * @param timeout HTTP 请求超时时间；为空、零值或负值时默认 3 秒
     */
    public record SupplierChannelEndpoint(String dispatchUrl, String invalidUrl, Duration timeout) {

        public SupplierChannelEndpoint {
            timeout = timeout == null || timeout.isZero() || timeout.isNegative()
                    ? DEFAULT_SUPPLIER_TIMEOUT
                    : timeout;
        }
    }
}
