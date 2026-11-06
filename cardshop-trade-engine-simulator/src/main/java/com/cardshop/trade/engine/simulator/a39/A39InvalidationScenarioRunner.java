package com.cardshop.trade.engine.simulator.a39;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 指定既有订单的一次同步作废；参见详细设计9.3.6。
 * 失败不自动查询转成功、不重发；主单12不表示券已作废，nonce不区分业务轮次。
 */
public final class A39InvalidationScenarioRunner {
    private static final String SUCCESS = "200";
    private static final String ISSUED_ORDER_STATUS = "12";
    private static final String MINISO_STATE_TYPE = "3";
    private static final String OTHER_CHANNEL_STATE_TYPE = "2";
    private static final String CANCELLED_USE_STATE = "3";
    private final V1CardApiClient client;
    private final V12Signer signer;
    private final Consumer<String> log;

    public A39InvalidationScenarioRunner(V1CardApiClient client, V12Signer signer, Consumer<String> log) {
        this.client = client;
        this.signer = signer;
        this.log = log;
    }

    /** 0=成功，1=失败/协议错误/网络故障；不输出凭据或券原文。 */
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
            log.accept("作废失败或响应不符合终态合同；保留证据，不自动查询或重发");
        } catch (IOException ex) {
            // 对外失败不证明供应商未执行，只留安全类型，由运营对账审计。
            log.accept("作废网络失败 type=" + ex.getClass().getSimpleName() + "，保留证据，不自动查询或重发");
        }
        return 1;
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
