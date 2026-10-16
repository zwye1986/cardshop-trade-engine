package com.cardshop.trade.engine.simulator.a39;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A39 真实业务场景编排器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」和「9.3 回调设计」：
 * 商户侧先获取 token，再发起已签名下单；只有下单被受理或明确异步处理中时才查单，避免凭证、
 * 验签或参数错误时制造无意义查询流量。
 */
public final class A39BusinessScenarioRunner {

    private static final String SUCCESS_CODE = "200";
    private static final String ASYNC_PROCESSING_CODE = "510";
    private static final List<String> TERMINAL_ORDER_STATUSES = List.of("6", "11", "12");

    private final Consumer<Credentials> credentialRegistration;
    private final java.util.function.BiConsumer<String, String> orderRegistration;
    private final V1CardApiClient client;
    private final V12Signer signer;
    private final Duration queryIntervalOverride;
    private final Consumer<String> logSink;

    public A39BusinessScenarioRunner(V1CardApiClient client, V12Signer signer, Duration queryIntervalOverride) {
        this(client, signer, queryIntervalOverride, ignored -> {
        });
    }

    public A39BusinessScenarioRunner(
            V1CardApiClient client,
            V12Signer signer,
            Duration queryIntervalOverride,
            Consumer<String> logSink) {
        this(client, signer, queryIntervalOverride, logSink, credential -> { }, (order, account) -> { });
    }

    public A39BusinessScenarioRunner(V1CardApiClient client, V12Signer signer, Duration queryIntervalOverride,
            Consumer<String> logSink, Consumer<Credentials> credentialRegistration,
            java.util.function.BiConsumer<String, String> orderRegistration) {
        this.credentialRegistration = Objects.requireNonNull(credentialRegistration);
        this.orderRegistration = Objects.requireNonNull(orderRegistration);
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.signer = Objects.requireNonNull(signer, "signer must not be null");
        this.queryIntervalOverride = queryIntervalOverride;
        this.logSink = Objects.requireNonNull(logSink, "logSink must not be null");
    }

    public ScenarioResult run(A39SimulatorOptions options) throws Exception {
        Objects.requireNonNull(options, "options must not be null");
        Credentials token = client.getToken(options.appId(), options.appSecret());
        Map<String, Object> createPayload = createPayload(options, token.signKey());
        // 下单前分别登记商户密钥和可信订单，避免早到回调；订单不绑定密钥，见详细设计3.1。
        credentialRegistration.accept(token);
        orderRegistration.accept(options.outOrderSn(), options.account());
        V1CardApiClient.ApiResponse createResponse = client.createRechargeOrder(
                token.token(),
                createPayload,
                options.requestSm4Enabled(),
                options.aesKey());
        List<V1CardApiClient.ApiResponse> queryResponses = new ArrayList<>();
        String lastOrderStatus = null;
        if (shouldQuery(createResponse)) {
            for (int attempt = 0; attempt < options.queryAttempts(); attempt++) {
                sleepBeforeNextQuery(attempt, options.queryInterval());
                V1CardApiClient.ApiResponse queryResponse = client.getRechargeOrder(
                        token.token(),
                        queryPayload(options, token.signKey()),
                        options.requestSm4Enabled(),
                        options.aesKey());
                queryResponses.add(queryResponse);
                lastOrderStatus = logStatusChange(options.outOrderSn(), lastOrderStatus, queryResponse, "query#" + (attempt + 1));
                if (isTerminalQueryResponse(queryResponse)) {
                    break;
                }
            }
        }
        return new ScenarioResult(createResponse, queryResponses);
    }

    Map<String, Object> createPayload(A39SimulatorOptions options, String signKey) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("product_id", options.productId());
        payload.put("out_order_sn", options.outOrderSn());
        payload.put("account", options.account());
        payload.put("timestamp", options.timestamp());
        payload.put("nonce_str", options.nonceStr());
        payload.put("notify_url", options.notifyUrl());
        payload.put("num", options.num());
        payload.put("order_price", options.orderPrice());
        payload.put("product_type", options.productType());
        payload.put("sign", signer.signCreateOrder(payload, signKey).sign());
        return payload;
    }

    private Map<String, Object> queryPayload(A39SimulatorOptions options, String signKey) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("out_order_sn", options.outOrderSn());
        payload.put("timestamp", String.valueOf(System.currentTimeMillis()));
        payload.put("nonce_str", A39SimulatorOptions.randomNonce());
        payload.put("sign", signer.signQueryOrder(payload, signKey).sign());
        return payload;
    }

    private boolean shouldQuery(V1CardApiClient.ApiResponse createResponse) {
        return SUCCESS_CODE.equals(createResponse.code()) || ASYNC_PROCESSING_CODE.equals(createResponse.code());
    }

    private boolean isTerminalQueryResponse(V1CardApiClient.ApiResponse queryResponse) {
        Object status = queryResponse.data().get("order_status");
        return SUCCESS_CODE.equals(queryResponse.code()) && status != null && TERMINAL_ORDER_STATUSES.contains(String.valueOf(status));
    }

    private String logStatusChange(
            String outOrderSn,
            String lastOrderStatus,
            V1CardApiClient.ApiResponse queryResponse,
            String source) {
        Object status = queryResponse.data().get("order_status");
        if (!SUCCESS_CODE.equals(queryResponse.code()) || status == null) {
            return lastOrderStatus;
        }
        String currentStatus = String.valueOf(status);
        if (!currentStatus.equals(lastOrderStatus)) {
            logSink.accept("[A39][ORDER][status-change] out_order_sn=%s order_status=%s source=%s"
                    .formatted(outOrderSn, currentStatus, source));
        }
        return currentStatus;
    }

    private void sleepBeforeNextQuery(int attempt, Duration configuredInterval) throws InterruptedException {
        if (attempt == 0) {
            return;
        }
        Duration interval = queryIntervalOverride == null ? configuredInterval : queryIntervalOverride;
        if (interval != null && !interval.isZero() && !interval.isNegative()) {
            Thread.sleep(interval.toMillis());
        }
    }

    public record ScenarioResult(
            V1CardApiClient.ApiResponse createResponse,
            List<V1CardApiClient.ApiResponse> queryResponses) {
    }
}
