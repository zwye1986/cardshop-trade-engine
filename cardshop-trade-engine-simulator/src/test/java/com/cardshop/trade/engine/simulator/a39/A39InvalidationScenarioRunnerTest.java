package com.cardshop.trade.engine.simulator.a39;

import static org.assertj.core.api.Assertions.*;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** 作废消费不得把充值成功12/1当终态；参见详细设计9.3.6。 */
class A39InvalidationScenarioRunnerTest {
    private A39SimulatorOptions options(String... extra) {
        var args = new ArrayList<>(List.of("--mode=invalidate", "--out-order-sn=EXISTING",
                "--request-sm4=false", "--query-interval=0", "--query-attempts=3"));
        args.addAll(List.of(extra));
        return A39SimulatorOptions.parse(args.toArray(String[]::new), Map.of("A39_APP_SECRET", "fixture"), Clock.systemUTC());
    }

    @Test void successRequiresCompleteTerminalResponseWithoutQuerying() throws Exception {
        var invalidations = new AtomicInteger();
        var queries = new AtomicInteger();
        var logs = new ArrayList<String>();
        var payloads = new ArrayList<Map<?, ?>>();
        V1CardApiClient client = (V1CardApiClient) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{V1CardApiClient.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "getToken" -> new Credentials("fixture-token", null, TestKeys.KEY);
                    case "invalidRechargeOrder" -> {
                        invalidations.incrementAndGet(); payloads.add((Map<?, ?>) args[1]);
                        yield new V1CardApiClient.ApiResponse("200", "ok", Map.of("order_status", "12",
                                "state_type", "3", "use_state", "3", "invalid_time", "2026-11-06 10:00:00"));
                    }
                    case "getRechargeOrder" -> {
                        payloads.add((Map<?, ?>) args[1]);
                        int n = queries.incrementAndGet();
                        yield new V1CardApiClient.ApiResponse("200", "ok", Map.of("order_status", "12",
                                "state_type", n == 1 ? "1" : "3", "use_state", n == 1 ? "1" : "3",
                                "invalid_time", n == 3 ? "2026-11-05 10:00:00" : ""));
                    }
                    default -> throw new AssertionError("Unexpected operation: " + method.getName());
                });
        assertThat(A39BusinessSimulatorApplication.run(options(), client, logs::add)).isZero();
        assertThat(invalidations.get()).isEqualTo(1);
        assertThat(queries.get()).isZero();
        assertThat(payloads).allSatisfy(p -> {
            assertThat(p.get("out_order_sn")).isEqualTo("EXISTING");
            assertThat(p.get("sign")).isEqualTo(new V12Signer().signQueryOrder((Map) p, TestKeys.KEY).sign());
        });
        assertThat(payloads).hasSize(1);
        assertThat(String.join("\n", logs)).contains("作废完成").doesNotContain("fixture-token", TestKeys.KEY);
    }

    @Test void requiresExplicitExistingOrderAndBudgetWithNetworkMargin() {
        assertThatThrownBy(() -> A39SimulatorOptions.parse(new String[]{"--mode=invalidate", "--request-sm4=false"},
                Map.of("A39_APP_SECRET", "fixture"), Clock.systemUTC())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out-order-sn");
        assertThatThrownBy(() -> options("--request-timeout=300s")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("310");
        assertThat(options().requestTimeout()).isEqualTo(java.time.Duration.ofSeconds(310));
    }

    @Test void rejectedAndContradictoryResultsNeverReportCompletedOrResendInvalidation() throws Exception {
        for (String code : List.of("503", "202", "200")) {
            var invalidations = new AtomicInteger();
            var queries = new AtomicInteger();
            var logs = new ArrayList<String>();
            var contradictory = Map.<String, Object>of("order_status", "6", "state_type", "3", "use_state", "3",
                    "invalid_time", "2026-11-05 10:00:00");
            V1CardApiClient client = (V1CardApiClient) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{V1CardApiClient.class}, (proxy, method, args) -> switch (method.getName()) {
                        case "getToken" -> new Credentials("fixture-token", null, TestKeys.KEY);
                        case "invalidRechargeOrder" -> {
                            invalidations.incrementAndGet();
                            yield new V1CardApiClient.ApiResponse(code, "fixture", contradictory);
                        }
                        case "getRechargeOrder" -> {
                            queries.incrementAndGet();
                            yield new V1CardApiClient.ApiResponse("200", "fixture", contradictory);
                        }
                        default -> throw new AssertionError(method.getName());
                    });
            assertThat(A39BusinessSimulatorApplication.run(options(), client, logs::add))
                    .isEqualTo(1);
            assertThat(invalidations.get()).isEqualTo(1);
            assertThat(queries.get()).isZero();
            assertThat(String.join("\n", logs)).doesNotContain("作废完成");
        }
    }
}
