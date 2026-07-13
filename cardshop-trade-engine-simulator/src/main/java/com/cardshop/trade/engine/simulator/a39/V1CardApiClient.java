package com.cardshop.trade.engine.simulator.a39;

import java.util.Map;

/**
 * 交行 V1 HTTP 客户端端口。
 *
 * <p>模拟器通过该端口串联 token、下单和查单。端口化是为了让单元测试不用访问真实
 * {@code http://apitest.kalaiwang.com}，避免 dev/test 证据受网络状态影响。
 */
public interface V1CardApiClient {

    String getToken(String appId, String appSecret) throws Exception;

    ApiResponse createRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) throws Exception;

    ApiResponse getRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) throws Exception;

    record ApiResponse(String code, String msg, Map<String, Object> data) {
        public ApiResponse {
            data = data == null ? Map.of() : Map.copyOf(data);
        }
    }
}
