package com.cardshop.trade.engine.simulator.a39;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.locks.LockSupport;
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
                createWindowResult.requestResults(),
                options.duration());
    }

    private CreateWindowResult runCreateWindow(
            A39SimulatorOptions options,
            String token,
            Set<String> usedNonces,
            String runId,
            Set<String> expectedCodes) throws Exception {
        int totalOrders = options.totalOrders();
        Duration loadDuration = options.duration();
        int ratePerSecond = options.ratePerSecond();
        boolean hasDeadline = loadDuration != null && !loadDuration.isZero();
        long deadlineNanos = hasDeadline ? System.nanoTime() + loadDuration.toNanos() : Long.MAX_VALUE;
        long intervalNanos = ratePerSecond > 0 ? NANOS_PER_SECOND / ratePerSecond : 0L;
        int maxInFlight = Math.min(options.concurrency(), totalOrders);
        List<CreateRequestResult> results = new ArrayList<>(totalOrders);
        Duration progressInterval = options.progressInterval();
        long progressIntervalNanos = progressInterval == null ? 0L : progressInterval.toNanos();
        long windowStartNanos = System.nanoTime();
        long nextProgressNanos = windowStartNanos + progressIntervalNanos;
        long nextSubmitSlotNanos = windowStartNanos;

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            CompletionService<CreateRequestResult> completionService = new ExecutorCompletionService<>(executor);
            int submitted = 0;
            int inFlight = 0;

            while (inFlight < maxInFlight && submitted < totalOrders && System.nanoTime() < deadlineNanos) {
                paceSubmit(ratePerSecond, intervalNanos, nextSubmitSlotNanos);
                if (ratePerSecond > 0) {
                    nextSubmitSlotNanos += intervalNanos;
                }
                submitted++;
                submitCreate(completionService, options, token, submitted, usedNonces, runId, expectedCodes);
                inFlight++;
            }

            while (inFlight > 0) {
                Future<CreateRequestResult> future = completionService.take();
                CreateRequestResult result = completedResult(future, options.errorSampleLimit());
                results.add(result);
                inFlight--;

                if (submitted < totalOrders && System.nanoTime() < deadlineNanos) {
                    paceSubmit(ratePerSecond, intervalNanos, nextSubmitSlotNanos);
                    if (ratePerSecond > 0) {
                        nextSubmitSlotNanos += intervalNanos;
                    }
                    submitted++;
                    submitCreate(completionService, options, token, submitted, usedNonces, runId, expectedCodes);
                    inFlight++;
                }

                if (progressIntervalNanos > 0L && System.nanoTime() >= nextProgressNanos) {
                    logProgress(options, results, windowStartNanos, inFlight);
                    nextProgressNanos += progressIntervalNanos;
                }
            }
        }
        return new CreateWindowResult(results, elapsedSince(windowStartNanos));
    }

    private static void paceSubmit(int ratePerSecond, long intervalNanos, long nextSubmitSlotNanos) {
        if (ratePerSecond <= 0) {
            return;
        }
        long waitNanos = nextSubmitSlotNanos - System.nanoTime();
        if (waitNanos > 0L) {
            LockSupport.parkNanos(waitNanos);
        }
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
            Map<String, Object> payload = createPayload(options, outOrderSn, sequence, nextUniqueNonce(usedNonces));
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
                    completedAtNanos,
                    response.timing());
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
                    completedAtNanos,
                    V1CardApiClient.ClientTiming.ZERO);
        }
    }

    Map<String, Object> createPayload(A39SimulatorOptions options, String outOrderSn, int sequence, String nonceStr) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("product_id", options.productId());
        payload.put("out_order_sn", outOrderSn);
        payload.put("account", account(options, sequence));
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

    private static String account(A39SimulatorOptions options, int sequence) {
        if (!options.uniqueAccounts()) {
            return options.account();
        }
        long start = Long.parseLong(options.account());
        long value = Math.addExact(start, sequence - 1L);
        String account = String.format(Locale.ROOT, "%0" + options.account().length() + "d", value);
        if (account.length() != options.account().length()) {
            throw new IllegalArgumentException("unique account sequence exceeds account length");
        }
        return account;
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
                    now,
                    V1CardApiClient.ClientTiming.ZERO);
        }
    }

    private LoadTestResult summarize(
            A39SimulatorOptions options,
            Instant startedAt,
            String runId,
            long tokenElapsedNanos,
            long createElapsedNanos,
            long totalElapsedNanos,
            List<CreateRequestResult> requestResults,
            Duration loadDuration) {
        Map<String, Integer> businessCodeCounts = new LinkedHashMap<>();
        Map<String, Integer> errorCounts = new LinkedHashMap<>();
        List<String> errorSamples = new ArrayList<>();
        List<Long> latencyMs = new ArrayList<>(requestResults.size());
        List<Long> prepareMs = new ArrayList<>(requestResults.size());
        List<Long> untilHeadersMs = new ArrayList<>(requestResults.size());
        List<Long> bodyReadMs = new ArrayList<>(requestResults.size());
        List<Long> parseMs = new ArrayList<>(requestResults.size());
        List<Long> clientTotalMs = new ArrayList<>(requestResults.size());
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
            V1CardApiClient.ClientTiming timing = result.timing();
            prepareMs.add(toMillis(timing.prepareNanos()));
            untilHeadersMs.add(toMillis(timing.untilHeadersNanos()));
            bodyReadMs.add(toMillis(timing.bodyReadNanos()));
            parseMs.add(toMillis(timing.parseNanos()));
            clientTotalMs.add(toMillis(timing.totalNanos()));
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
                sortedLatencies(prepareMs),
                sortedLatencies(untilHeadersMs),
                sortedLatencies(bodyReadMs),
                sortedLatencies(parseMs),
                sortedLatencies(clientTotalMs),
                toMillis(tokenElapsedNanos),
                toMillis(createElapsedNanos),
                toMillis(totalElapsedNanos),
                qps(completed, createElapsedNanos),
                startedAt,
                loadDuration);
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
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                toMillis(tokenElapsedNanos),
                0,
                toMillis(totalElapsedNanos),
                0.0d,
                startedAt,
                options.duration());
    }

    /**
     * 按 progress-interval 输出实时性能摘要。
     *
     * <p>QPS 口径：已完成请求数 / 已用时，等价于「每秒完成数在已用时段上的算术平均」。
     * 延迟为单笔「提交前 → 收到完整响应/异常」墙钟；P95/P99 对当前累计样本重算。
     * 参见 {@code docs/superpowers/specs/2026-07-15-a39-simulator-load-test-design.md}「结果与退出语义」。
     */
    private void logProgress(
            A39SimulatorOptions options,
            List<CreateRequestResult> results,
            long windowStartNanos,
            int inFlight) {
        SnapshotStats stats = snapshotStats(results, elapsedSince(windowStartNanos));
        double plannedDurationSec = options.duration() == null || options.duration().isZero()
                ? 0.0d
                : options.duration().toMillis() / 1000.0d;
        // 机器可解析一行，便于脚本采集
        logSink.accept(("[A39][LOAD][progress] concurrency=%d inFlight=%d durationSec=%.2f plannedDurationSec=%.2f "
                + "success=%d failed=%d completed=%d qps=%.2f errorRatePct=%.2f avgMs=%d p95Ms=%d p99Ms=%d")
                .formatted(
                        options.concurrency(),
                        inFlight,
                        stats.durationSec(),
                        plannedDurationSec,
                        stats.successful(),
                        stats.failed(),
                        stats.completed(),
                        stats.qps(),
                        stats.errorRatePct(),
                        stats.avgMs(),
                        stats.p95Ms(),
                        stats.p99Ms()));
        // 中文可读块，字段对齐压测看板常见口径
        logSink.accept("""
                [A39][LOAD][stats]
                并发数：%d
                压测时长：%s 秒
                成功请求数：%s
                失败请求数：%s
                QPS：%s
                错误率：%s%%
                平均响应时间：%dms
                P95 响应时间：%dms
                P99 响应时间：%dms""".formatted(
                options.concurrency(),
                formatSeconds(stats.durationSec()),
                formatCount(stats.successful()),
                formatCount(stats.failed()),
                formatQps(stats.qps()),
                formatErrorRate(stats.errorRatePct()),
                stats.avgMs(),
                stats.p95Ms(),
                stats.p99Ms()));
    }

    private static SnapshotStats snapshotStats(List<CreateRequestResult> results, long elapsedNanos) {
        int completed = results.size();
        int successful = 0;
        long[] latencies = new long[completed];
        long latencySum = 0L;
        for (int i = 0; i < completed; i++) {
            CreateRequestResult result = results.get(i);
            if (result.successful()) {
                successful++;
            }
            long ms = toMillis(result.latencyNanos());
            latencies[i] = ms;
            latencySum += ms;
        }
        int failed = completed - successful;
        Arrays.sort(latencies);
        double durationSec = elapsedNanos <= 0L ? 0.0d : (double) elapsedNanos / NANOS_PER_SECOND;
        long avgMs = completed == 0 ? 0L : Math.round((double) latencySum / completed);
        return new SnapshotStats(
                completed,
                successful,
                failed,
                durationSec,
                qps(completed, elapsedNanos),
                errorRatePct(successful, completed),
                avgMs,
                percentile(latencies, 95),
                percentile(latencies, 99));
    }

    private static double errorRatePct(int successful, int completed) {
        if (completed == 0) {
            return 0.0d;
        }
        return (completed - successful) * 100.0d / completed;
    }

    private static long percentile(long[] sortedValues, int percentile) {
        if (sortedValues.length == 0) {
            return 0L;
        }
        int index = (int) Math.ceil((double) percentile * sortedValues.length / PERCENTILE_SCALE) - 1;
        return sortedValues[Math.max(0, Math.min(index, sortedValues.length - 1))];
    }

    private static String formatCount(long value) {
        return String.format(Locale.US, "%,d", value);
    }

    private static String formatSeconds(double seconds) {
        if (seconds >= 100d || Math.rint(seconds) == seconds) {
            return String.format(Locale.ROOT, "%.0f", seconds);
        }
        return String.format(Locale.ROOT, "%.1f", seconds);
    }

    private static String formatQps(double qps) {
        if (qps >= 100d || Math.rint(qps) == qps) {
            return String.format(Locale.ROOT, "%.0f", qps);
        }
        return String.format(Locale.ROOT, "%.2f", qps);
    }

    private static String formatErrorRate(double errorRatePct) {
        return String.format(Locale.ROOT, "%.2f", errorRatePct);
    }

    private record SnapshotStats(
            int completed,
            int successful,
            int failed,
            double durationSec,
            double qps,
            double errorRatePct,
            long avgMs,
            long p95Ms,
            long p99Ms) {
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
            long completedAtNanos,
            V1CardApiClient.ClientTiming timing) {

        private CreateRequestResult {
            timing = timing == null ? V1CardApiClient.ClientTiming.ZERO : timing;
        }

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
            List<Long> prepareMs,
            List<Long> untilHeadersMs,
            List<Long> bodyReadMs,
            List<Long> parseMs,
            List<Long> clientTotalMs,
            long tokenElapsedMs,
            long createElapsedMs,
            long totalElapsedMs,
            double qps,
            Instant startedAt,
            Duration loadDuration) {

        public LoadTestResult {
            baseUrl = Objects.requireNonNull(baseUrl, "baseUrl must not be null");
            runId = Objects.requireNonNull(runId, "runId must not be null");
            outOrderPrefix = Objects.requireNonNull(outOrderPrefix, "outOrderPrefix must not be null");
            requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
            loadDuration = loadDuration == null ? Duration.ZERO : loadDuration;
            businessCodeCounts = Collections.unmodifiableMap(new LinkedHashMap<>(businessCodeCounts));
            errorCounts = Collections.unmodifiableMap(new LinkedHashMap<>(errorCounts));
            errorSamples = List.copyOf(errorSamples);
            latencyMs = List.copyOf(latencyMs);
            prepareMs = List.copyOf(prepareMs);
            untilHeadersMs = List.copyOf(untilHeadersMs);
            bodyReadMs = List.copyOf(bodyReadMs);
            parseMs = List.copyOf(parseMs);
            clientTotalMs = List.copyOf(clientTotalMs);
        }

        public int exitCode() {
            if (failed != 0) {
                return 1;
            }
            if (!loadDuration.isZero()) {
                return 0;
            }
            return completed == totalOrders ? 0 : 1;
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
            fields.put("errorRatePct", String.format(Locale.ROOT, "%.2f", errorRatePct()));
            fields.put("tokenElapsedMs", String.valueOf(tokenElapsedMs));
            fields.put("createElapsedMs", String.valueOf(createElapsedMs));
            fields.put("durationSec", String.format(Locale.ROOT, "%.3f", createElapsedMs / 1000.0d));
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
            fields.put("avgMs", String.valueOf(avgMs()));
            fields.put("minMs", String.valueOf(minMs()));
            fields.put("maxMs", String.valueOf(maxMs()));
            fields.put("p50Ms", String.valueOf(percentile(latencyMs, 50)));
            fields.put("p95Ms", String.valueOf(percentile(latencyMs, 95)));
            fields.put("p99Ms", String.valueOf(percentile(latencyMs, 99)));
            putTimingFields(fields, "timing.prepare", prepareMs);
            putTimingFields(fields, "timing.untilHeaders", untilHeadersMs);
            putTimingFields(fields, "timing.bodyRead", bodyReadMs);
            putTimingFields(fields, "timing.parse", parseMs);
            putTimingFields(fields, "timing.clientTotal", clientTotalMs);
            fields.put("exitCode", String.valueOf(exitCode()));
            for (int index = 0; index < errorSamples.size(); index++) {
                fields.put("errorSample." + (index + 1), errorSamples.get(index));
            }
            StringBuilder report = new StringBuilder(512);
            fields.forEach((key, value) -> report.append(key).append('=').append(value).append(System.lineSeparator()));
            // 中文摘要块，与实时 stats 字段对齐，便于直接贴进压测记录
            report.append(System.lineSeparator())
                    .append("[A39][LOAD][summary]").append(System.lineSeparator())
                    .append("并发数：").append(concurrency).append(System.lineSeparator())
                    .append("压测时长：").append(formatSeconds(createElapsedMs / 1000.0d)).append(" 秒")
                    .append(System.lineSeparator())
                    .append("成功请求数：").append(formatCount(successful)).append(System.lineSeparator())
                    .append("失败请求数：").append(formatCount(failed)).append(System.lineSeparator())
                    .append("QPS：").append(formatQps(qps)).append(System.lineSeparator())
                    .append("错误率：").append(formatErrorRate(errorRatePct())).append('%')
                    .append(System.lineSeparator())
                    .append("平均响应时间：").append(avgMs()).append("ms").append(System.lineSeparator())
                    .append("P95 响应时间：").append(percentile(latencyMs, 95)).append("ms")
                    .append(System.lineSeparator())
                    .append("P99 响应时间：").append(percentile(latencyMs, 99)).append("ms")
                    .append(System.lineSeparator());
            return report.toString();
        }

        private double errorRatePct() {
            return A39LoadTestRunner.errorRatePct(successful, completed);
        }

        private long avgMs() {
            if (latencyMs.isEmpty()) {
                return 0L;
            }
            long sum = 0L;
            for (Long value : latencyMs) {
                sum += value;
            }
            return Math.round((double) sum / latencyMs.size());
        }

        private void putTimingFields(Map<String, String> fields, String prefix, List<Long> values) {
            fields.put(prefix + ".minMs", String.valueOf(minOf(values)));
            fields.put(prefix + ".maxMs", String.valueOf(maxOf(values)));
            fields.put(prefix + ".p50Ms", String.valueOf(percentile(values, 50)));
            fields.put(prefix + ".p95Ms", String.valueOf(percentile(values, 95)));
            fields.put(prefix + ".p99Ms", String.valueOf(percentile(values, 99)));
        }

        private double successRate() {
            return completed == 0 ? 0.0d : (double) successful / completed;
        }

        private long minMs() {
            return minOf(latencyMs);
        }

        private long maxMs() {
            return maxOf(latencyMs);
        }

        private static long minOf(List<Long> values) {
            return values.isEmpty() ? 0L : Collections.min(values);
        }

        private static long maxOf(List<Long> values) {
            return values.isEmpty() ? 0L : Collections.max(values);
        }

        private static long percentile(List<Long> sortedValues, int percentile) {
            if (sortedValues.isEmpty()) {
                return 0L;
            }
            int index = (int) Math.ceil((double) percentile * sortedValues.size() / PERCENTILE_SCALE) - 1;
            return sortedValues.get(Math.max(0, Math.min(index, sortedValues.size() - 1)));
        }
    }
}
