package com.cardshop.trade.engine.simulator.a39;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class A39LoadTestRunnerTest {

    @Test
    void sendsConfiguredNumberOfCreateRequestsWithUniqueOrderAndNonce() throws Exception {
        CapturingLoadClient client = new CapturingLoadClient(List.of(
                response("200"), response("510"), response("200"), response("510"), response("200")), 2);
        A39SimulatorOptions options = options(2, 5, List.of("200", "510"), 20);

        A39LoadTestRunner.LoadTestResult result = new A39LoadTestRunner(client, new V1Sm3Signer(), Clock.systemUTC(), ignored -> {
        }).run(options);

        assertThat(result.successful()).isEqualTo(5);
        assertThat(result.failed()).isZero();
        assertThat(client.createPayloads).hasSize(5);
        assertThat(client.maxInFlight.get()).isLessThanOrEqualTo(2);
        assertThat(client.maxInFlight.get()).isEqualTo(2);
        assertThat(client.createPayloads)
                .extracting(payload -> String.valueOf(payload.get("out_order_sn")))
                .doesNotHaveDuplicates();
        assertThat(client.createPayloads)
                .extracting(payload -> String.valueOf(payload.get("nonce_str")))
                .doesNotHaveDuplicates();
        assertThat(client.createPayloads)
                .allSatisfy(payload -> assertThat(payload.get("notify_url")).isEqualTo(""));
    }

    @Test
    void classifiesUnexpectedCodesAndExceptionsAsFailuresWithLimitedSamples() throws Exception {
        CapturingLoadClient client = new CapturingLoadClient(List.of(
                response("200"),
                response("501"),
                failure(new IllegalStateException("HTTP request failed: path=/v1/cardapi/order/createRechargeOrder, status=503")),
                response("510")), 0);
        A39SimulatorOptions options = options(2, 4, List.of("200", "510"), 1);

        A39LoadTestRunner.LoadTestResult result = new A39LoadTestRunner(client, new V1Sm3Signer(), Clock.systemUTC(), ignored -> {
        }).run(options);

        assertThat(result.completed()).isEqualTo(4);
        assertThat(result.successful()).isEqualTo(2);
        assertThat(result.failed()).isEqualTo(2);
        assertThat(result.businessCodeCounts()).containsEntry("200", 1).containsEntry("510", 1).containsEntry("501", 1);
        assertThat(result.errorCounts()).containsEntry("IllegalStateException", 1);
        assertThat(result.errorSamples()).hasSize(1);
        assertThat(result.exitCode()).isEqualTo(1);
    }

    @Test
    void reportContainsStableSummaryFields() throws Exception {
        CapturingLoadClient client = new CapturingLoadClient(List.of(response("200"), response("510")), 0);
        List<String> logs = new ArrayList<>();
        A39SimulatorOptions options = options(1, 2, List.of("200", "510"), 20);

        A39LoadTestRunner.LoadTestResult result = new A39LoadTestRunner(
                client,
                new V1Sm3Signer(),
                Clock.fixed(Instant.parse("2026-07-15T01:00:00Z"), ZoneId.of("UTC")),
                logs::add)
                .run(options);

        String report = result.toReport();
        Map<String, String> fields = reportFields(report);

        assertThat(fields)
                .containsEntry("mode", "LOAD")
                .containsEntry("baseUrl", "http://apitest.kalaiwang.com")
                .containsEntry("outOrderPrefix", "PERF-A39")
                .containsEntry("concurrency", "1")
                .containsEntry("totalOrders", "2")
                .containsEntry("requestTimeout", "PT10S")
                .containsEntry("successful", "2")
                .containsEntry("failed", "0")
                .containsEntry("code.200", "1")
                .containsEntry("code.510", "1");
        assertThat(fields).containsKeys(
                "p50Ms",
                "p95Ms",
                "p99Ms",
                "timing.prepare.p95Ms",
                "timing.untilHeaders.p95Ms",
                "timing.bodyRead.p95Ms",
                "timing.parse.p95Ms",
                "timing.clientTotal.p95Ms");
        assertThat(logs).isEmpty();
    }

    @Test
    void progressLogsStayDisabledWhenProgressIntervalIsZero() throws Exception {
        CapturingLoadClient client = new CapturingLoadClient(List.of(response("200"), response("200")), 0);
        List<String> logs = new ArrayList<>();
        A39SimulatorOptions options = options(1, 2, List.of("200", "510"), 20);

        new A39LoadTestRunner(client, new V1Sm3Signer(), Clock.systemUTC(), logs::add).run(options);

        assertThat(logs).isEmpty();
    }

    @Test
    void retriesNonceGenerationUntilUniqueWithinRun() throws Exception {
        CapturingLoadClient client = new CapturingLoadClient(List.of(response("200"), response("200"), response("200")), 0);
        A39SimulatorOptions options = options(1, 3, List.of("200", "510"), 20);
        Supplier<String> nonceSupplier = new Supplier<>() {
            private final List<String> candidates = List.of("NONCE-1", "NONCE-1", "NONCE-2", "NONCE-2", "NONCE-3");
            private final AtomicInteger index = new AtomicInteger();

            @Override
            public String get() {
                return candidates.get(index.getAndIncrement());
            }
        };

        new A39LoadTestRunner(client, new V1Sm3Signer(), Clock.systemUTC(), ignored -> {
        }, nonceSupplier).run(options);

        assertThat(client.createPayloads)
                .extracting(payload -> String.valueOf(payload.get("nonce_str")))
                .containsExactly("NONCE-1", "NONCE-2", "NONCE-3");
    }

    @Test
    void orderNumbersUseInjectedClockAndSequence() throws Exception {
        CapturingLoadClient client = new CapturingLoadClient(List.of(response("200"), response("200")), 0);
        Clock fixedClock = Clock.fixed(Instant.parse("2026-07-15T01:00:00Z"), ZoneId.of("UTC"));
        A39SimulatorOptions options = options(1, 2, List.of("200", "510"), 20);

        new A39LoadTestRunner(client, new V1Sm3Signer(), fixedClock, ignored -> {
        }).run(options);

        assertThat(client.createPayloads)
                .extracting(payload -> String.valueOf(payload.get("out_order_sn")))
                .containsExactly("PERF-A39-1784077200000-00001", "PERF-A39-1784077200000-00002");
    }

    @Test
    void uniqueAccountsGenerateSequentialAccountsInLoadMode() throws Exception {
        CapturingLoadClient client = new CapturingLoadClient(List.of(response("200"), response("200"), response("200")), 0);
        A39SimulatorOptions options = options(1, 3, List.of("200", "510"), 20, true);

        new A39LoadTestRunner(client, new V1Sm3Signer(), Clock.systemUTC(), ignored -> {
        }).run(options);

        assertThat(client.createPayloads)
                .extracting(payload -> String.valueOf(payload.get("account")))
                .containsExactly("18186222336", "18186222337", "18186222338")
                .doesNotHaveDuplicates();
    }

    @Test
    void durationAndRatePaceSubmissionsToPlannedTotalOrders() throws Exception {
        RepeatingLoadClient client = new RepeatingLoadClient(response("200"));
        A39SimulatorOptions options = durationRateOptions(2, 0, Duration.ofSeconds(1), 5, 5);

        A39LoadTestRunner.LoadTestResult result = new A39LoadTestRunner(client, new V1Sm3Signer(), Clock.systemUTC(), ignored -> {
        }).run(options);

        assertThat(result.completed()).isEqualTo(5);
        assertThat(result.successful()).isEqualTo(5);
        assertThat(result.failed()).isZero();
        assertThat(result.totalOrders()).isEqualTo(5);
        assertThat(result.exitCode()).isZero();
        assertThat(client.createPayloads).hasSize(5);
    }

    @Test
    void durationAndRateAllowSmallToleranceAtHighRate() throws Exception {
        RepeatingLoadClient client = new RepeatingLoadClient(response("200"));
        A39SimulatorOptions options = durationRateOptions(2, 0, Duration.ofSeconds(1), 10, 10);

        A39LoadTestRunner.LoadTestResult result = new A39LoadTestRunner(client, new V1Sm3Signer(), Clock.systemUTC(), ignored -> {
        }).run(options);

        assertThat(result.completed()).isBetween(9, 10);
        assertThat(result.failed()).isZero();
        assertThat(result.totalOrders()).isEqualTo(10);
        assertThat(result.exitCode()).isZero();
    }

    private static A39SimulatorOptions durationRateOptions(
            int concurrency,
            int totalOrders,
            Duration duration,
            int ratePerSecond,
            int plannedTotalOrders) {
        return new A39SimulatorOptions(
                "http://apitest.kalaiwang.com",
                "app-id",
                "app-secret",
                "AES-KEY",
                "73301280",
                "18186222336",
                "IGNORED-SCENARIO-OUT-ORDER",
                "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR",
                "1783224958",
                1,
                5200L,
                1,
                "",
                0,
                Duration.ZERO,
                false,
                "127.0.0.1",
                19039,
                Duration.ZERO,
                A39SimulatorMode.LOAD,
                concurrency,
                plannedTotalOrders > 0 ? plannedTotalOrders : totalOrders,
                Duration.ofSeconds(10),
                "PERF-A39",
                false,
                List.of("200", "510"),
                20,
                Duration.ZERO,
                duration,
                ratePerSecond);
    }

    private static A39SimulatorOptions options(int concurrency, int totalOrders, List<String> expectedCodes, int errorSampleLimit) {
        return options(concurrency, totalOrders, expectedCodes, errorSampleLimit, false);
    }

    private static A39SimulatorOptions options(
            int concurrency,
            int totalOrders,
            List<String> expectedCodes,
            int errorSampleLimit,
            boolean uniqueAccounts) {
        return new A39SimulatorOptions(
                "http://apitest.kalaiwang.com",
                "app-id",
                "app-secret",
                "AES-KEY",
                "73301280",
                "18186222336",
                "IGNORED-SCENARIO-OUT-ORDER",
                "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR",
                "1783224958",
                1,
                5200L,
                1,
                "",
                0,
                Duration.ZERO,
                false,
                "127.0.0.1",
                19039,
                Duration.ZERO,
                A39SimulatorMode.LOAD,
                concurrency,
                totalOrders,
                Duration.ofSeconds(10),
                "PERF-A39",
                uniqueAccounts,
                expectedCodes,
                errorSampleLimit,
                Duration.ZERO,
                Duration.ZERO,
                0);
    }

    private static V1CardApiClient.ApiResponse response(String code) {
        return new V1CardApiClient.ApiResponse(
                code,
                "msg-" + code,
                Map.of(),
                new V1CardApiClient.ClientTiming(1_000_000L, 2_000_000L, 3_000_000L, 4_000_000L, 10_000_000L));
    }

    private static RuntimeException failure(RuntimeException ex) {
        return ex;
    }

    private static Map<String, String> reportFields(String report) {
        Map<String, String> fields = new LinkedHashMap<>();
        report.lines()
                .filter(line -> !line.isBlank())
                .forEach(line -> {
                    int delimiter = line.indexOf('=');
                    assertThat(delimiter).isPositive();
                    fields.put(line.substring(0, delimiter), line.substring(delimiter + 1));
                });
        return fields;
    }

    private static final class RepeatingLoadClient implements V1CardApiClient {

        private final V1CardApiClient.ApiResponse response;
        private final List<Map<String, Object>> createPayloads = java.util.Collections.synchronizedList(new ArrayList<>());

        private RepeatingLoadClient(V1CardApiClient.ApiResponse response) {
            this.response = response;
        }

        @Override
        public String getToken(String appId, String appSecret) {
            return "token";
        }

        @Override
        public V1CardApiClient.ApiResponse createRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            createPayloads.add(new ConcurrentHashMap<>(payload));
            return response;
        }

        @Override
        public V1CardApiClient.ApiResponse getRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            throw new AssertionError("load mode must not query order");
        }
    }

    private static final class CapturingLoadClient implements V1CardApiClient {

        private final List<Object> outcomes;
        private final AtomicInteger next = new AtomicInteger();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger maxInFlight = new AtomicInteger();
        private final List<Map<String, Object>> createPayloads = java.util.Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch enteredBarrier;
        private final CountDownLatch releaseBarrier;

        private CapturingLoadClient(List<?> outcomes, int overlapCount) {
            this.outcomes = List.copyOf(outcomes);
            this.enteredBarrier = overlapCount > 0 ? new CountDownLatch(overlapCount) : null;
            this.releaseBarrier = overlapCount > 0 ? new CountDownLatch(1) : null;
        }

        @Override
        public String getToken(String appId, String appSecret) {
            return "token";
        }

        @Override
        public V1CardApiClient.ApiResponse createRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey)
                throws InterruptedException {
            int current = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(current, Math::max);
            try {
                createPayloads.add(new ConcurrentHashMap<>(payload));
                awaitOverlapBarrier();
                Object outcome = outcomes.get(next.getAndIncrement());
                if (outcome instanceof RuntimeException ex) {
                    throw ex;
                }
                return (V1CardApiClient.ApiResponse) outcome;
            } finally {
                inFlight.decrementAndGet();
            }
        }

        @Override
        public V1CardApiClient.ApiResponse getRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            throw new AssertionError("load mode must not query order");
        }

        private void awaitOverlapBarrier() throws InterruptedException {
            if (enteredBarrier == null || releaseBarrier == null) {
                return;
            }
            enteredBarrier.countDown();
            if (enteredBarrier.await(3, TimeUnit.SECONDS)) {
                releaseBarrier.countDown();
            }
            assertThat(releaseBarrier.await(3, TimeUnit.SECONDS)).isTrue();
        }
    }
}
