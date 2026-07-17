package com.cardshop.trade.engine.simulator.a39;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class A39BusinessScenarioRunnerTest {

    @Test
    void createPayloadCarriesRuntimeRequiredFieldsWithoutSigningThem() throws Exception {
        CapturingClient client = new CapturingClient(new V1CardApiClient.ApiResponse("505", "订单号重复", Map.of()));
        A39BusinessScenarioRunner runner = new A39BusinessScenarioRunner(client, new V1Sm3Signer(), Duration.ZERO);

        runner.run(options("OUT-1", 1));

        assertThat(client.createPayload)
                .containsEntry("order_price", 5200L)
                .containsEntry("product_type", 1);
        assertThat(client.createPayload.get("sign")).isEqualTo("2504f57140911a9c8484489fb634086a9a1ab60d9ca47131218c06f9ddb18268");
    }

    @Test
    void queryRunsForAcceptedOrAsyncCreateResponsesOnly() throws Exception {
        CapturingClient accepted = new CapturingClient(new V1CardApiClient.ApiResponse("200", "操作成功", Map.of()));
        new A39BusinessScenarioRunner(accepted, new V1Sm3Signer(), Duration.ZERO).run(options("OUT-2", 2));
        assertThat(accepted.queryPayloads).hasSize(2);

        CapturingClient async = new CapturingClient(new V1CardApiClient.ApiResponse("510", "请等待，稍后重新获卡", Map.of()));
        new A39BusinessScenarioRunner(async, new V1Sm3Signer(), Duration.ZERO).run(options("OUT-3", 2));
        assertThat(async.queryPayloads).hasSize(2);

        CapturingClient failed = new CapturingClient(new V1CardApiClient.ApiResponse("501", "验签不合规", Map.of()));
        new A39BusinessScenarioRunner(failed, new V1Sm3Signer(), Duration.ZERO).run(options("OUT-4", 2));
        assertThat(failed.queryPayloads).isEmpty();
    }

    @Test
    void logsOrderStatusChangesOnlyWhenQueryStatusChanges() throws Exception {
        CapturingClient client = new CapturingClient(new V1CardApiClient.ApiResponse("200", "操作成功", Map.of()));
        client.queryResponses.add(new V1CardApiClient.ApiResponse("200", "操作成功", Map.of("order_status", "2")));
        client.queryResponses.add(new V1CardApiClient.ApiResponse("200", "操作成功", Map.of("order_status", "2")));
        client.queryResponses.add(new V1CardApiClient.ApiResponse("200", "操作成功", Map.of("order_status", "12")));
        List<String> logs = new ArrayList<>();

        new A39BusinessScenarioRunner(client, new V1Sm3Signer(), Duration.ZERO, logs::add)
                .run(options("OUT-5", 3));

        assertThat(logs)
                .filteredOn(log -> log.contains("[A39][ORDER][status-change]"))
                .containsExactly(
                        "[A39][ORDER][status-change] out_order_sn=OUT-5 order_status=2 source=query#1",
                        "[A39][ORDER][status-change] out_order_sn=OUT-5 order_status=12 source=query#3"
                );
    }

    private static A39SimulatorOptions options(String outOrderSn, int queryAttempts) {
        return new A39SimulatorOptions(
                "http://apitest.kalaiwang.com",
                "app-id",
                "app-secret",
                "AES-KEY",
                "73301280",
                "18186222336",
                outOrderSn,
                "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR",
                "1783224958",
                1,
                5200L,
                1,
                "http://127.0.0.1:19039/callback/order",
                queryAttempts,
                Duration.ZERO,
                false,
                "127.0.0.1",
                19039,
                Duration.ZERO,
                A39SimulatorMode.SCENARIO,
                10,
                200,
                Duration.ofSeconds(10),
                "A39TEST",
                false,
                List.of("200", "510"),
                20,
                Duration.ZERO
        );
    }

    private static final class CapturingClient implements V1CardApiClient {

        private final ApiResponse createResponse;
        private Map<String, Object> createPayload = Map.of();
        private final List<Map<String, Object>> queryPayloads = new ArrayList<>();
        private final List<ApiResponse> queryResponses = new ArrayList<>();

        private CapturingClient(ApiResponse createResponse) {
            this.createResponse = createResponse;
        }

        @Override
        public String getToken(String appId, String appSecret) {
            return "token";
        }

        @Override
        public ApiResponse createRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            this.createPayload = Map.copyOf(payload);
            return createResponse;
        }

        @Override
        public ApiResponse getRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            queryPayloads.add(Map.copyOf(payload));
            if (!queryResponses.isEmpty()) {
                return queryResponses.remove(0);
            }
            return new ApiResponse("200", "操作成功", Map.of("order_status", "2"));
        }
    }
}
