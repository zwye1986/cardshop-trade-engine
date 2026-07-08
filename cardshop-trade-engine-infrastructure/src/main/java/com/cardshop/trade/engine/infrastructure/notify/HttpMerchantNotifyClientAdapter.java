package com.cardshop.trade.engine.infrastructure.notify;

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
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.12 回调设计」。只有商户响应 JSON 中
 * {@code code=200} 时才视为成功；非 200、网络异常和解析异常均交给回调 Worker 重试。
 */
public class HttpMerchantNotifyClientAdapter implements MerchantNotifyClientPort {

    private static final Logger log = LoggerFactory.getLogger(HttpMerchantNotifyClientAdapter.class);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Duration timeout;

    public HttpMerchantNotifyClientAdapter(HttpClient httpClient, ObjectMapper objectMapper, Duration timeout) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.timeout = timeout == null || timeout.isZero() || timeout.isNegative() ? Duration.ofSeconds(3) : timeout;
    }

    @Override
    public MerchantNotifySendResult send(MerchantNotifyRequest request) {
        try {
            HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(request.notifyUrl()))
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(request.payload()))
                    .build();
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                return MerchantNotifySendResult.failed(response.statusCode(), "merchant http status not 2xx");
            }
            JsonNode root = objectMapper.readTree(response.body() == null || response.body().isBlank()
                    ? "{}"
                    : response.body());
            String code = root.has("code") ? root.get("code").asText() : "";
            if ("200".equals(code)) {
                return MerchantNotifySendResult.success(response.statusCode(), response.body());
            }
            return MerchantNotifySendResult.failed(response.statusCode(), "merchant response code=" + code);
        } catch (Exception ex) {
            log.error("商户回调HTTP发送异常 taskId={}, notifyUrl={}", request.taskId(), request.notifyUrl(), ex);
            return MerchantNotifySendResult.failed(0, ex.getMessage());
        }
    }
}
