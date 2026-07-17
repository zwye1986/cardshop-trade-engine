package com.cardshop.trade.engine.infrastructure.notify;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.logging.LogStormGuard.LogDecision;
import com.cardshop.trade.engine.application.recharge.MerchantNotifyClientPort;
import com.cardshop.trade.engine.application.recharge.MerchantNotifyRequest;
import com.cardshop.trade.engine.application.recharge.MerchantNotifySendResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 商户回调 HTTP 客户端适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。只有商户响应 JSON 中
 * {@code code=200} 时才视为成功；非 200、网络异常和解析异常均交给回调 Worker 重试。
 */
public class HttpMerchantNotifyClientAdapter implements MerchantNotifyClientPort {

    private static final Logger log = LoggerFactory.getLogger(HttpMerchantNotifyClientAdapter.class);
    private static final int HTTP_SUCCESS_MIN = 200;
    private static final int HTTP_SUCCESS_MAX_EXCLUSIVE = 300;
    private static final int HTTP_STATUS_NOT_SENT = 0;
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(3);
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String EMPTY_JSON = "{}";
    private static final String FIELD_CODE = "code";
    private static final String MERCHANT_SUCCESS_CODE = "200";
    private static final String LOG_EVENT_HTTP_EXCEPTION = "merchant-notify.http.exception";

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Duration timeout;
    private final LogStormGuard logStormGuard = LogStormGuard.defaultGuard();

    public HttpMerchantNotifyClientAdapter(HttpClient httpClient, ObjectMapper objectMapper, Duration timeout) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.timeout = timeout == null || timeout.isZero() || timeout.isNegative() ? DEFAULT_TIMEOUT : timeout;
    }

    @Override
    public MerchantNotifySendResult send(MerchantNotifyRequest request) {
        if (request.notifyUrl() == null || request.notifyUrl().isBlank()) {
            /*
             * Worker 与写库侧应已拦截空 URL；此处再 fail closed，避免 URI.create("") 打出异常噪声。
             * 参见 docs/交行直充业务系统详细设计.md「9.3 回调设计」。
             */
            return MerchantNotifySendResult.failed(HTTP_STATUS_NOT_SENT, "blank notify_url");
        }
        try {
            HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(request.notifyUrl()))
                    .timeout(timeout)
                    .header(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON)
                    .POST(HttpRequest.BodyPublishers.ofString(request.payload()))
                    .build();
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < HTTP_SUCCESS_MIN || response.statusCode() >= HTTP_SUCCESS_MAX_EXCLUSIVE) {
                return MerchantNotifySendResult.failed(response.statusCode(), "merchant http status not 2xx");
            }
            JsonNode root = objectMapper.readTree(response.body() == null || response.body().isBlank()
                    ? EMPTY_JSON
                    : response.body());
            String code = root.has(FIELD_CODE) ? root.get(FIELD_CODE).asText() : "";
            if (MERCHANT_SUCCESS_CODE.equals(code)) {
                return MerchantNotifySendResult.success(response.statusCode(), response.body());
            }
            return MerchantNotifySendResult.failed(response.statusCode(), "merchant response code=" + code);
        } catch (Exception ex) {
            LogDecision decision = logStormGuard.shouldLog(LOG_EVENT_HTTP_EXCEPTION);
            if (decision.shouldLog()) {
                log.error("商户回调HTTP发送异常 taskId={}, notifyUrl={}, occurrence={}, sampled={}",
                        request.taskId(),
                        request.notifyUrl(),
                        decision.occurrence(),
                        decision.sampledOccurrence(),
                        ex);
            }
            return MerchantNotifySendResult.failed(HTTP_STATUS_NOT_SENT, ex.getMessage());
        }
    }
}
