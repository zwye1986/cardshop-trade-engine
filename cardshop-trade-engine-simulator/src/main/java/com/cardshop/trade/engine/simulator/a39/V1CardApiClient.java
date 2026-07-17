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

    /**
     * 单次 HTTP 调用的客户端侧耗时拆分。
     *
     * <p>{@code untilHeaders} 在 JDK {@code HttpClient} 下聚合 DNS/建连/TLS/写请求/等响应头，
     * 无法再细拆；与 nginx/upstream timing 对照时可据此区分客户端等待与服务端处理。
     * 参见 {@code docs/superpowers/specs/2026-07-15-a39-simulator-load-test-design.md}。
     */
    record ClientTiming(
            long prepareNanos,
            long untilHeadersNanos,
            long bodyReadNanos,
            long parseNanos,
            long totalNanos) {

        public static final ClientTiming ZERO = new ClientTiming(0L, 0L, 0L, 0L, 0L);

        public ClientTiming {
            prepareNanos = Math.max(0L, prepareNanos);
            untilHeadersNanos = Math.max(0L, untilHeadersNanos);
            bodyReadNanos = Math.max(0L, bodyReadNanos);
            parseNanos = Math.max(0L, parseNanos);
            totalNanos = Math.max(0L, totalNanos);
        }
    }

    record ApiResponse(String code, String msg, Map<String, Object> data, ClientTiming timing) {
        public ApiResponse(String code, String msg, Map<String, Object> data) {
            this(code, msg, data, ClientTiming.ZERO);
        }

        public ApiResponse {
            data = data == null ? Map.of() : Map.copyOf(data);
            timing = timing == null ? ClientTiming.ZERO : timing;
        }
    }
}
