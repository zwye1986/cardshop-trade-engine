package com.cardshop.trade.engine.simulator.a39;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 指定既有订单的一次作废与有限确认；参见详细设计9.3.6。
 * 网络结果未知只能查询原单，不能重发作废；主单12不表示券已作废。
 */
public final class A39InvalidationScenarioRunner {
    private static final String SUCCESS = "200";
    private static final String PENDING = "202";
    private static final String NOT_ACCEPTED = "503";
    private static final String ISSUED_ORDER_STATUS = "12";
    private static final String MINISO_STATE_TYPE = "3";
    private static final String OTHER_CHANNEL_STATE_TYPE = "2";
    private static final String CANCELLED_USE_STATE = "3";
    private static final int EXIT_PENDING = 2;
    private final V1CardApiClient client;
    private final V12Signer signer;
    private final Consumer<String> log;

    public A39InvalidationScenarioRunner(V1CardApiClient client, V12Signer signer, Consumer<String> log) {
        this.client = client;
        this.signer = signer;
        this.log = log;
    }

    /** 0=已确认完成，1=明确未受理/协议错误，2=仍待确认；不输出凭据或券原文。 */
    public int run(A39SimulatorOptions options) throws Exception {
        Credentials credential = client.getToken(options.appId(), options.appSecret());
        try {
            var response = client.invalidRechargeOrder(credential.token(), payload(options, credential),
                    options.requestSm4Enabled(), options.aesKey());
            log.accept("Invalid response: code=" + response.code());
            if (completed(response)) {
                log.accept("作废完成");
                return 0;
            }
            if (NOT_ACCEPTED.equals(response.code())) {
                log.accept("作废明确未受理，请核查条件；不自动重发");
                return 1;
            }
            if (!SUCCESS.equals(response.code()) && !PENDING.equals(response.code())) {
                log.accept("作废协议错误，请核查；不自动重发");
                return 1;
            }
        } catch (IOException ex) {
            // 远端可能已受理；只记录类型，不泄露包含凭据/业务回包的异常文本。
            log.accept("作废网络结果未知 type=" + ex.getClass().getSimpleName() + "，查询原单，不重发");
        }
        for (int attempt = 0; attempt < options.queryAttempts(); attempt++) {
            if (attempt > 0) Thread.sleep(options.queryInterval().toMillis());
            try {
                var response = client.getRechargeOrder(credential.token(), payload(options, credential),
                        options.requestSm4Enabled(), options.aesKey());
                if (completed(response)) {
                    log.accept("作废完成");
                    return 0;
                }
                log.accept("作废确认中 query=" + (attempt + 1) + " code=" + response.code());
            } catch (IOException ex) {
                log.accept("查单网络结果未知 type=" + ex.getClass().getSimpleName());
            }
        }
        log.accept("仍待确认/需核查，不是作废失败；保留原订单等待恢复或通知");
        return EXIT_PENDING;
    }

    private Map<String, Object> payload(A39SimulatorOptions options, Credentials credential) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("out_order_sn", options.outOrderSn());
        payload.put("timestamp", String.valueOf(System.currentTimeMillis()));
        payload.put("nonce_str", A39SimulatorOptions.randomNonce());
        payload.put("sign", signer.signQueryOrder(payload, credential.signKey()).sign());
        return payload;
    }

    private static boolean completed(V1CardApiClient.ApiResponse response) {
        var data = response.data();
        String type = String.valueOf(data.get("state_type"));
        return SUCCESS.equals(response.code())
                && ISSUED_ORDER_STATUS.equals(String.valueOf(data.get("order_status")))
                && (MINISO_STATE_TYPE.equals(type) || OTHER_CHANNEL_STATE_TYPE.equals(type))
                && CANCELLED_USE_STATE.equals(String.valueOf(data.get("use_state")))
                && data.get("invalid_time") instanceof String time && !time.isBlank()
                && (data.get("use_time") == null || "".equals(data.get("use_time")));
    }
}
