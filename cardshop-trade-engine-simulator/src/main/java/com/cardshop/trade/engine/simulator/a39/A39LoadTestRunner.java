package com.cardshop.trade.engine.simulator.a39;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A39 下单压测编排器。
 *
 * <p>对应 {@code docs/superpowers/specs/2026-07-15-a39-simulator-load-test-design.md}
 * 「压测请求模型」和「结果与退出语义」：压测只获取一次 token，随后按固定最大并发创建订单，
 * 不查单、不启动回调、不等待回调，用稳定的 key=value 报告输出入口吞吐和延迟指标。
 */
public final class A39LoadTestRunner {

    private static final String MODE_LOAD = "LOAD";
    private static final String EMPTY_NOTIFY_URL = "";
    private static final String SUCCESS_CODE = "200";
    private static final String ASYNC_PROCESSING_CODE = "510";
    private static final int PERCENTILE_SCALE = 100;
    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final V1CardApiClient client;
    private final V1Sm3Signer signer;
    private final Clock clock;
    private final Consumer<String> logSink;
    private final Supplier<String> nonceSupplier;

    public A39LoadTestRunner(V1CardApiClient client, V1Sm3Signer signer, Clock clock, Consumer<String> logSink) {
        this(client, signer, clock, logSink, A39SimulatorOptions::randomNonce);
    }

    A39LoadTestRunner(
            V1CardApiClient client,
            V1Sm3Signer signer,
            Clock clock,
            Consumer<String> logSink,
            Supplier<String> nonceSupplier) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.signer = Objects.requireNonNull(signer, "signer must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.logSink = Objects.requireNonNull(logSink, "logSink must not be null");
        this.nonceSupplier = Objects.requireNonNull(nonceSupplier, "nonceSupplier must not be null");
    }

    /**
     * 执行固定并发下单压测。
     *
     * <p>token 获取发生在下单计时窗口之前；下单窗口内最多保留 {@code concurrency} 个在途请求。
     * 参见 {@code docs/superpowers/specs/2026-07-15-a39-simulator-load-test-design.md}「压测请求模型」。
     */
    public LoadTestResult run(A39SimulatorOptions options) throws Exception {
        Objects.requireNonNull(options, "options must not be null");
        Instant startedAt = clock.instant();
        long totalStartNanos = System.nanoTime();
        long tokenStartNanos = System.nanoTime();
        String token;
        try {
            token = client.getToken(options.appId(), options.appSecret());
        } catch (Exception ex) {
            long tokenElapsedNanos = elapsedSince(tokenStartNanos);
            long totalElapsedNanos = elapsedSince(totalStartNanos);
            return tokenFailureResult(options, startedAt, tokenElapsedNanos, totalElapsedNanos, ex);
        }
        long tokenElapsedNanos = elapsedSince(tokenStartNanos);

        Set<String> usedNonces = ConcurrentHashMap.newKeySet();
        String runId = String.valueOf(clock.millis());
        Set<String> expectedCodes = Set.copyOf(options.expectedCreateCodes());
        CreateWindowResult createWindowResult = runCreateWindow(options, token, usedNonces, runId, expectedCodes);
        long totalElapsedNanos = elapsedSince(totalStartNanos);
        return summarize(
                options,
                startedAt,
                runId,
                tokenElapsedNanos,
                createWindowResult.elapsedNanos(),
                totalElapsedNanos,
                createWindowResult.requestResults());
    }

    private CreateWindowResult runCreateWindow(
            A39SimulatorOptions options,
            String token,
            Set<String> usedNonces,
            String runId,
            Set<String> expectedCodes) throws Exception {
        int totalOrders = options.totalOrders();
        int maxInFlight = Math.min(options.concurrency(), totalOrders);
        List<CreateRequestResult> results = new ArrayList<>(totalOrders);
        Duration progressInterval = options.progressInterval();
        long progressIntervalNanos = progressInterval == null ? 0L : progressInterval.toNanos();
        long windowStartNanos = System.nanoTime();
        long nextProgressNanos = windowStartNanos + progressIntervalNanos;

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            CompletionService<CreateRequestResult> completionService = new ExecutorCompletionService<>(executor);
            int submitted = 0;
            for (; submitted < maxInFlight; submitted++) {
                submitCreate(completionService, options, token, submitted + 1, usedNonces, runId, expectedCodes);
            }

            while (results.size() < totalOrders) {
                Future<CreateRequestResult> future = completionService.take();
                CreateRequestResult result = completedResult(future, options.errorSampleLimit());
                results.add(result);
                if (submitted < totalOrders) {
                    submitted++;
                    submitCreate(completionService, options, token, submitted, usedNonces, runId, expectedCodes);
                }
                if (progressIntervalNanos > 0L && System.nanoTime() >= nextProgressNanos) {
                    logProgress(results, windowStartNanos);
                    nextProgressNanos += progressIntervalNanos;
                }
            }
        }
        return new CreateWindowResult(results, elapsedSince(windowStartNanos));
    }

    private void submitCreate(
            CompletionService<CreateRequestResult> completionService,
            A39SimulatorOptions options,
            String token,
            int sequence,
            Set<String> usedNonces,
            String runId,
            Set<String> expectedCodes) {
        completionService.submit(() -> createOne(options, token, sequence, usedNonces, runId, expectedCodes));
    }

    private CreateRequestResult createOne(
            A39SimulatorOptions options,
            String token,
            int sequence,
            Set<String> usedNonces,
            String runId,
            Set<String> expectedCodes) {
        long startedAtNanos = System.nanoTime();
        String outOrderSn = outOrderSn(options, sequence, runId);
        try {
            Map<String, Object> payload = createPayload(options, outOrderSn, nextUniqueNonce(usedNonces));
            V1CardApiClient.ApiResponse response = client.createRechargeOrder(
                    token,
                    payload,
                    options.requestSm4Enabled(),
                    options.aesKey());
            long completedAtNanos = System.nanoTime();
            boolean success = expectedCodes.contains(response.code());
            String sample = success ? null : "out_order_sn=%s code=%s msg=%s"
                    .formatted(outOrderSn, response.code(), response.msg());
            return new CreateRequestResult(
                    outOrderSn,
                    response.code(),
                    success,
                    null,
                    sample,
                    startedAtNanos,
                    completedAtNanos);
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            long completedAtNanos = System.nanoTime();
            // 压测高频路径不逐笔打 error，避免异常风暴放大故障；异常类型和样本会汇总到最终报告。
            // 参见 docs/superpowers/specs/2026-07-15-a39-simulator-load-test-design.md「结果与退出语义」。
            return new CreateRequestResult(
                    outOrderSn,
                    null,
                    false,
                    ex,
                    "out_order_sn=%s error=%s message=%s".formatted(outOrderSn, ex.getClass().getSimpleName(), ex.getMessage()),
                    startedAtNanos,
                    completedAtNanos);
        }
    }

    Map<String, Object> createPayload(A39SimulatorOptions options, String outOrderSn, String nonceStr) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("product_id", options.productId());
        payload.put("out_order_sn", outOrderSn);
        payload.put("account", options.account());
        payload.put("timestamp", String.valueOf(clock.millis()));
        payload.put("nonce_str", nonceStr);
        payload.put("notify_url", EMPTY_NOTIFY_URL);
        payload.put("num", options.num());
        payload.put("order_price", options.orderPrice());
        payload.put("product_type", options.productType());
        payload.put("sign", signer.signCreateOrder(payload, options.aesKey()).sign());
        return payload;
    }

    private String nextUniqueNonce(Set<String> usedNonces) {
        String nonce;
        do {
            nonce = nonceSupplier.get();
        } while (!usedNonces.add(nonce));
        return nonce;
    }

    private CreateRequestResult completedResult(Future<CreateRequestResult> future, int errorSampleLimit)
            throws InterruptedException {
        try {
            return future.get();
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause() == null ? ex : ex.getCause();
            Exception normalized = cause instanceof Exception exception ? exception : new RuntimeException(cause);
            long now = System.nanoTime();
            return new CreateRequestResult(
                    "unknown",
                    null,
                    false,
                    normalized,
                    errorSampleLimit > 0 ? "out_order_sn=unknown error=%s message=%s"
                            .formatted(normalized.getClass().getSimpleName(), normalized.getMessage()) : null,
                    now,
                    now);
        }
    }

    private LoadTestResult summarize(
            A39SimulatorOptions options,
            Instant startedAt,
            String runId,
            long tokenElapsedNanos,
            long createElapsedNanos,
            long totalElapsedNanos,
            List<CreateRequestResult> requestResults) {
        Map<String, Integer> businessCodeCounts = new LinkedHashMap<>();
        Map<String, Integer> errorCounts = new LinkedHashMap<>();
        List<String> errorSamples = new ArrayList<>();
        List<Long> latencyMs = new ArrayList<>(requestResults.size());
        int successful = 0;
        for (CreateRequestResult result : requestResults) {
            if (result.businessCode() != null) {
                increment(businessCodeCounts, result.businessCode());
            }
            if (result.error() != null) {
                increment(errorCounts, result.error().getClass().getSimpleName());
            }
            if (result.successful()) {
                successful++;
            } else if (result.errorSample() != null && errorSamples.size() < options.errorSampleLimit()) {
                errorSamples.add(result.errorSample());
            }
            latencyMs.add(toMillis(result.latencyNanos()));
        }
        int completed = requestResults.size();
        int failed = completed - successful;
        return new LoadTestResult(
                MODE_LOAD,
                options.baseUrl(),
                runId,
                options.outOrderPrefix(),
                options.concurrency(),
                options.totalOrders(),
                options.requestTimeout(),
                completed,
                successful,
                failed,
                sortedCopy(businessCodeCounts),
                sortedCopy(errorCounts),
                List.copyOf(errorSamples),
                sortedLatencies(latencyMs),
                toMillis(tokenElapsedNanos),
                toMillis(createElapsedNanos),
                toMillis(totalElapsedNanos),
                qps(completed, createElapsedNanos),
                startedAt);
    }

    private LoadTestResult tokenFailureResult(
            A39SimulatorOptions options,
            Instant startedAt,
            long tokenElapsedNanos,
            long totalElapsedNanos,
            Exception ex) {
        Map<String, Integer> errorCounts = new LinkedHashMap<>();
        increment(errorCounts, ex.getClass().getSimpleName());
        List<String> samples = options.errorSampleLimit() > 0
                ? List.of("phase=token error=%s message=%s".formatted(ex.getClass().getSimpleName(), ex.getMessage()))
                : List.of();
        return new LoadTestResult(
                MODE_LOAD,
                options.baseUrl(),
                String.valueOf(startedAt.toEpochMilli()),
                options.outOrderPrefix(),
                options.concurrency(),
                options.totalOrders(),
                options.requestTimeout(),
                0,
                0,
                options.totalOrders(),
                Map.of(),
                Map.copyOf(errorCounts),
                samples,
                List.of(),
                toMillis(tokenElapsedNanos),
                0,
                toMillis(totalElapsedNanos),
                0.0d,
                startedAt);
    }

    private void logProgress(List<CreateRequestResult> results, long windowStartNanos) {
        long elapsedNanos = elapsedSince(windowStartNanos);
        long successful = results.stream().filter(CreateRequestResult::successful).count();
        int completed = results.size();
        logSink.accept("[A39][LOAD][progress] completed=%d successful=%d failed=%d elapsedMs=%d qps=%.2f"
                .formatted(completed, successful, completed - successful, toMillis(elapsedNanos), qps(completed, elapsedNanos)));
    }

    private static String outOrderSn(A39SimulatorOptions options, int sequence, String runId) {
        return "%s-%s-%05d".formatted(options.outOrderPrefix(), runId, sequence);
    }

    private static void increment(Map<String, Integer> counts, String key) {
        counts.merge(key, 1, Integer::sum);
    }

    private static Map<String, Integer> sortedCopy(Map<String, Integer> values) {
        Map<String, Integer> sorted = new LinkedHashMap<>();
        values.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> sorted.put(entry.getKey(), entry.getValue()));
        return Collections.unmodifiableMap(sorted);
    }

    private static List<Long> sortedLatencies(List<Long> latencyMs) {
        List<Long> sorted = new ArrayList<>(latencyMs);
        sorted.sort(Comparator.naturalOrder());
        return List.copyOf(sorted);
    }

    private static long elapsedSince(long startNanos) {
        return System.nanoTime() - startNanos;
    }

    private static long toMillis(long nanos) {
        return Math.max(0L, nanos / NANOS_PER_MILLI);
    }

    private static double qps(int completed, long elapsedNanos) {
        if (completed == 0 || elapsedNanos <= 0L) {
            return 0.0d;
        }
        return (double) completed * NANOS_PER_SECOND / elapsedNanos;
    }

    private record CreateRequestResult(
            String outOrderSn,
            String businessCode,
            boolean successful,
            Exception error,
            String errorSample,
            long startedAtNanos,
            long completedAtNanos) {

        private long latencyNanos() {
            return Math.max(0L, completedAtNanos - startedAtNanos);
        }
    }

    private record CreateWindowResult(List<CreateRequestResult> requestResults, long elapsedNanos) {
    }

    public record LoadTestResult(
            String mode,
            String baseUrl,
            String runId,
            String outOrderPrefix,
            int concurrency,
            int totalOrders,
            Duration requestTimeout,
            int completed,
            int successful,
            int failed,
            Map<String, Integer> businessCodeCounts,
            Map<String, Integer> errorCounts,
            List<String> errorSamples,
            List<Long> latencyMs,
            long tokenElapsedMs,
            long createElapsedMs,
            long totalElapsedMs,
            double qps,
            Instant startedAt) {

        public LoadTestResult {
            baseUrl = Objects.requireNonNull(baseUrl, "baseUrl must not be null");
            runId = Objects.requireNonNull(runId, "runId must not be null");
            outOrderPrefix = Objects.requireNonNull(outOrderPrefix, "outOrderPrefix must not be null");
            requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
            businessCodeCounts = Collections.unmodifiableMap(new LinkedHashMap<>(businessCodeCounts));
            errorCounts = Collections.unmodifiableMap(new LinkedHashMap<>(errorCounts));
            errorSamples = List.copyOf(errorSamples);
            latencyMs = List.copyOf(latencyMs);
        }

        public int exitCode() {
            return completed == totalOrders && failed == 0 ? 0 : 1;
        }

        /**
         * 输出稳定 key=value 文本，便于联调记录和后续脚本解析。
         *
         * <p>字段集合对应
         * {@code docs/superpowers/specs/2026-07-15-a39-simulator-load-test-design.md}「结果与退出语义」。
         */
        public String toReport() {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("mode", mode);
            fields.put("startedAt", startedAt.toString());
            fields.put("baseUrl", baseUrl);
            fields.put("runId", runId);
            fields.put("outOrderPrefix", outOrderPrefix);
            fields.put("concurrency", String.valueOf(concurrency));
            fields.put("totalOrders", String.valueOf(totalOrders));
            fields.put("requestTimeout", requestTimeout.toString());
            fields.put("completed", String.valueOf(completed));
            fields.put("successful", String.valueOf(successful));
            fields.put("failed", String.valueOf(failed));
            fields.put("successRate", String.format(Locale.ROOT, "%.4f", successRate()));
            fields.put("tokenElapsedMs", String.valueOf(tokenElapsedMs));
            fields.put("createElapsedMs", String.valueOf(createElapsedMs));
            fields.put("totalElapsedMs", String.valueOf(totalElapsedMs));
            fields.put("qps", String.format(Locale.ROOT, "%.2f", qps));
            fields.put("code." + SUCCESS_CODE, String.valueOf(businessCodeCounts.getOrDefault(SUCCESS_CODE, 0)));
            fields.put("code." + ASYNC_PROCESSING_CODE, String.valueOf(businessCodeCounts.getOrDefault(ASYNC_PROCESSING_CODE, 0)));
            businessCodeCounts.entrySet().stream()
                    .filter(entry -> !SUCCESS_CODE.equals(entry.getKey()) && !ASYNC_PROCESSING_CODE.equals(entry.getKey()))
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> fields.put("code." + entry.getKey(), String.valueOf(entry.getValue())));
            errorCounts.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> fields.put("error." + entry.getKey(), String.valueOf(entry.getValue())));
            fields.put("minMs", String.valueOf(minMs()));
            fields.put("maxMs", String.valueOf(maxMs()));
            fields.put("p50Ms", String.valueOf(percentile(50)));
            fields.put("p95Ms", String.valueOf(percentile(95)));
            fields.put("p99Ms", String.valueOf(percentile(99)));
            fields.put("exitCode", String.valueOf(exitCode()));
            for (int index = 0; index < errorSamples.size(); index++) {
                fields.put("errorSample." + (index + 1), errorSamples.get(index));
            }
            StringBuilder report = new StringBuilder(512);
            fields.forEach((key, value) -> report.append(key).append('=').append(value).append(System.lineSeparator()));
            return report.toString();
        }

        private double successRate() {
            return completed == 0 ? 0.0d : (double) successful / completed;
        }

        private long minMs() {
            return latencyMs.isEmpty() ? 0L : Collections.min(latencyMs);
        }

        private long maxMs() {
            return latencyMs.isEmpty() ? 0L : Collections.max(latencyMs);
        }

        private long percentile(int percentile) {
            if (latencyMs.isEmpty()) {
                return 0L;
            }
            int index = (int) Math.ceil((double) percentile * latencyMs.size() / PERCENTILE_SCALE) - 1;
            return latencyMs.get(Math.max(0, Math.min(index, latencyMs.size() - 1)));
        }
    }
}
