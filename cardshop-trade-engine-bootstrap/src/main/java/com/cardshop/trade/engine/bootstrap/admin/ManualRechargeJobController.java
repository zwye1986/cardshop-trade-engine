package com.cardshop.trade.engine.bootstrap.admin;

import com.cardshop.trade.engine.application.recharge.RechargeNotifyBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceDeltaFlushJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceReconcileJob;
import com.cardshop.trade.engine.job.recharge.RechargeCacheRepairJob;
import com.cardshop.trade.engine.job.recharge.RechargeOutboxPublishJob;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * dev/test 环境人工触发补偿任务的受控入口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」、
 * 「13. 测试与验收标准」以及 {@code docs/集成测试方案.md}
 * 「6.4.1 Redis pending 补投专项用例」和「8.5 Redis pending 补投联调设计」。这个 Controller 只为 PEND-001/PEND-002/PEND-003
 * 真实联调提供临时触发能力：必须显式打开 {@code trade.engine.integration.manual-job-enabled=true}，
 * 且只能在 {@code dev/test} profile、dev/test 测试机本机回环地址调用。这里不提供任意 Bean/方法调用能力，
 * 只暴露 Redis pending MQ 补投、outbox、回调重试和余额刷账这些固定链路，避免把内部补偿任务变成泛化远程执行入口。
 */
@RestController
@Profile({"dev", "test"})
@ConditionalOnProperty(prefix = "trade.engine.integration", name = "manual-job-enabled", havingValue = "true")
@RequestMapping("/internal/integration/recharge/jobs")
public class ManualRechargeJobController {

    private static final Logger log = LoggerFactory.getLogger(ManualRechargeJobController.class);
    private static final Duration DEFAULT_MANUAL_JOB_TIMEOUT = Duration.ofSeconds(30);
    private static final int MAX_MANUAL_SHARD_TOTAL = 128;
    private static final int MAX_MANUAL_LIMIT = 100;
    private static final Pattern WORKER_ID_PATTERN = Pattern.compile("[A-Za-z0-9._:-]{1,80}");

    private final RechargePersistMqRepublishJob persistMqRepublishJob;
    private final Optional<RechargeOutboxPublishJob> outboxPublishJob;
    private final Optional<RechargeNotifyWorkerService> notifyWorkerService;
    private final Optional<RechargeBalanceDeltaFlushJob> balanceDeltaFlushJob;
    private final Optional<RechargeBalanceReconcileJob> balanceReconcileJob;
    private final Optional<RechargeCacheRepairJob> cacheRepairJob;
    private final Duration manualJobTimeout;

    @Autowired
    public ManualRechargeJobController(
            RechargePersistMqRepublishJob persistMqRepublishJob,
            ObjectProvider<RechargeOutboxPublishJob> outboxPublishJobProvider,
            ObjectProvider<RechargeNotifyWorkerService> notifyWorkerServiceProvider,
            ObjectProvider<RechargeBalanceDeltaFlushJob> balanceDeltaFlushJobProvider,
            ObjectProvider<RechargeBalanceReconcileJob> balanceReconcileJobProvider,
            ObjectProvider<RechargeCacheRepairJob> cacheRepairJobProvider,
            TradeEngineProperties properties) {
        this(
                persistMqRepublishJob,
                Optional.ofNullable(outboxPublishJobProvider.getIfAvailable()),
                Optional.ofNullable(notifyWorkerServiceProvider.getIfAvailable()),
                Optional.ofNullable(balanceDeltaFlushJobProvider.getIfAvailable()),
                Optional.ofNullable(balanceReconcileJobProvider.getIfAvailable()),
                Optional.ofNullable(cacheRepairJobProvider.getIfAvailable()),
                Duration.ofMillis(Objects.requireNonNull(properties, "properties must not be null")
                        .getIntegration()
                        .getManualJobTimeoutMs()));
    }

    ManualRechargeJobController(
            RechargePersistMqRepublishJob persistMqRepublishJob,
            TradeEngineProperties properties) {
        this(
                persistMqRepublishJob,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Duration.ofMillis(Objects.requireNonNull(properties, "properties must not be null")
                        .getIntegration()
                        .getManualJobTimeoutMs()));
    }

    ManualRechargeJobController(RechargePersistMqRepublishJob persistMqRepublishJob, Duration manualJobTimeout) {
        this(
                persistMqRepublishJob,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                manualJobTimeout);
    }

    ManualRechargeJobController(
            RechargePersistMqRepublishJob persistMqRepublishJob,
            Optional<RechargeOutboxPublishJob> outboxPublishJob,
            Optional<RechargeNotifyWorkerService> notifyWorkerService,
            Optional<RechargeBalanceDeltaFlushJob> balanceDeltaFlushJob,
            Optional<RechargeBalanceReconcileJob> balanceReconcileJob,
            Optional<RechargeCacheRepairJob> cacheRepairJob,
            Duration manualJobTimeout) {
        this.persistMqRepublishJob = Objects.requireNonNull(
                persistMqRepublishJob,
                "persistMqRepublishJob must not be null");
        this.outboxPublishJob = Objects.requireNonNull(outboxPublishJob, "outboxPublishJob must not be null");
        this.notifyWorkerService = Objects.requireNonNull(
                notifyWorkerService,
                "notifyWorkerService must not be null");
        this.balanceDeltaFlushJob = Objects.requireNonNull(
                balanceDeltaFlushJob,
                "balanceDeltaFlushJob must not be null");
        this.balanceReconcileJob = Objects.requireNonNull(
                balanceReconcileJob,
                "balanceReconcileJob must not be null");
        this.cacheRepairJob = Objects.requireNonNull(
                cacheRepairJob,
                "cacheRepairJob must not be null");
        this.manualJobTimeout = normalizeTimeout(manualJobTimeout);
    }

    @PostMapping("/persist-mq-republish/run-once")
    public Mono<ResponseEntity<ManualJobRunResponse>> runPersistMqRepublishOnce(
            @RequestBody ManualJobRunRequest request,
            ServerWebExchange exchange) {
        /*
         * 安全边界：本入口只服务 dev/test 真实联调。即使开关被误打开，也要求请求来自当前测试机的
         * loopback 地址，避免 RabbitMQ/MySQL/Redis 补偿任务被远程任意触发。详见详细设计
         * 「10.1 XXL-Job Handler 契约」中关于 runOnce 门面仅承接固定补偿链路的约束。
         */
        if (!isLoopback(exchange)) {
            return Mono.just(ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(ManualJobRunResponse.rejected("Manual integration job endpoint only accepts loopback requests")));
        }
        ManualJobParameters parameters = validate(request);
        /*
         * WebFlux Controller 默认运行在 Reactor event-loop 上，而 Redis pending 补投端口内部会对
         * Reactive Redis 命令做短超时 block，以复用 XXL-Job 的同步 runOnce 契约。参见详细设计
         * 「10.1 XXL-Job Handler 契约」。人工 HTTP 入口必须把这段同步补偿任务切到 boundedElastic，否则 test188
         * 会在 event-loop 线程上触发 Reactor 的 blocking guard，表现为每个 org_index bucket 扫描失败。
         *
         * 这里的 Reactor timeout 只保护 HTTP 响应链路：超时后会向调用方返回 FAILED，但已经提交到
         * boundedElastic 的 blocking runOnce 不保证被中断；真正释放工作线程仍依赖 Redis/RabbitMQ/MySQL
         * 端口各自的操作超时。这个边界必须写清楚，避免后续维护者误以为 HTTP timeout 能完全防止
         * boundedElastic 线程耗尽。
         */
        return Mono.fromCallable(() -> {
                    RechargeCompensationBatchResult result = persistMqRepublishJob.runOnce(
                            parameters.shardTotal(),
                            parameters.shardIndex(),
                            parameters.limit(),
                            parameters.workerId());
                    return ResponseEntity.ok(ManualJobRunResponse.succeeded(parameters, result));
                })
                .subscribeOn(Schedulers.boundedElastic())
                .timeout(manualJobTimeout)
                .onErrorResume(TimeoutException.class, ex -> handleJobTimeout(parameters, ex))
                .onErrorResume(RuntimeException.class, ex -> handleJobFailure(parameters, ex));
    }

    @PostMapping("/outbox-publish/run-once")
    public Mono<ResponseEntity<ManualJobRunResponse>> runOutboxPublishOnce(
            @RequestBody ManualJobRunRequest request,
            ServerWebExchange exchange) {
        /*
         * docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」要求供应商派单、作废和回调任务生成
         * 共用 outbox CAS 抢占语义。该入口只用于 dev/test SUP/INV 用例手工推进到下一步；生产调度仍应走
         * XXL-Job 或内部调度，不允许把任意任务名透传给外部调用方。
         */
        return runManualJob(
                "outbox publish",
                request,
                exchange,
                parameters -> ManualJobRunResponse.succeeded(
                        parameters,
                        requireJob(outboxPublishJob, "RechargeOutboxPublishJob").runOnce(
                                parameters.shardTotal(),
                                parameters.shardIndex(),
                                parameters.limit(),
                                parameters.workerId())));
    }

    @PostMapping("/notify-retry/run-once")
    public Mono<ResponseEntity<ManualJobRunResponse>> runNotifyRetryOnce(
            @RequestBody ManualJobRunRequest request,
            ServerWebExchange exchange) {
        /*
         * docs/交行直充业务系统详细设计.md「9.3 回调设计」要求非 200 或网络失败进入回调任务退避重试。
         * 回调任务本身不使用分片参数，但入口仍复用统一请求对象，便于测试控制台和 curl 生成器只维护一种
         * 参数结构；shardTotal/shardIndex 只做安全校验，不参与 notify worker 执行。
         */
        return runManualJob(
                "notify retry",
                request,
                exchange,
                parameters -> ManualJobRunResponse.succeeded(
                        parameters,
                        requireJob(notifyWorkerService, "RechargeNotifyWorkerService").processDue(
                                parameters.limit(),
                                parameters.workerId())));
    }

    @PostMapping("/balance-delta-flush/run-once")
    public Mono<ResponseEntity<ManualJobRunResponse>> runBalanceDeltaFlushOnce(
            @RequestBody ManualJobRunRequest request,
            ServerWebExchange exchange) {
        /*
         * docs/交行直充业务系统高可用方案设计.md「14.7 mc_ext_depart.balance 刷账协议」要求余额 delta
         * 通过可靠 outbox 异步沉淀到旧表余额。该入口服务 BAL-001 dev/test 验证，用同一 runOnce 参数校验
         * 约束批量规模，避免人工误触发大范围刷账。
         */
        return runManualJob(
                "balance delta flush",
                request,
                exchange,
                parameters -> ManualJobRunResponse.succeeded(
                        parameters,
                        requireJob(balanceDeltaFlushJob, "RechargeBalanceDeltaFlushJob").runOnce(
                                parameters.shardTotal(),
                                parameters.shardIndex(),
                                parameters.limit(),
                                parameters.workerId())));
    }

    @PostMapping("/balance-reconcile/run-once")
    public Mono<ResponseEntity<ManualJobRunResponse>> runBalanceReconcileOnce(
            @RequestBody ManualJobRunRequest request,
            ServerWebExchange exchange) {
        /*
         * docs/交行直充业务系统高可用方案设计.md「17.5 余额对账任务」要求对账任务只发现差异和触发后续修复，
         * 不在未确认 pending/outbox 前直接释放资金。本入口服务 BAL-002 dev/test 证据采集，返回计数即可。
         */
        return runManualJob(
                "balance reconcile",
                request,
                exchange,
                parameters -> ManualJobRunResponse.succeeded(
                        parameters,
                        requireJob(balanceReconcileJob, "RechargeBalanceReconcileJob").runOnce(
                                parameters.shardTotal(),
                                parameters.shardIndex(),
                                parameters.limit(),
                                parameters.workerId())));
    }

    @PostMapping("/cache-repair/run-once")
    public Mono<ResponseEntity<ManualJobRunResponse>> runCacheRepairOnce(
            @RequestBody ManualJobRunRequest request,
            ServerWebExchange exchange) {
        /*
         * docs/交行直充业务系统高可用方案设计.md「17.4 缓存修复任务」要求修复任务持有 repairing_lock 后
         * 才能重建 Redis 基线。当前首版用于 CACHE-001 余额基线预热，商品配置修复仍按文档单独推进。
         */
        return runManualJob(
                "cache repair",
                request,
                exchange,
                parameters -> ManualJobRunResponse.succeeded(
                        parameters,
                        requireJob(cacheRepairJob, "RechargeCacheRepairJob").runOnce(
                                parameters.shardTotal(),
                                parameters.shardIndex(),
                                parameters.limit(),
                                parameters.workerId())));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ManualJobRunResponse> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(ManualJobRunResponse.rejected(ex.getMessage()));
    }

    /**
     * 只接受测试机本地回环请求。
     *
     * <p>集成测试文档中的 {@code 127.0.0.1:10080} 指的是在 test188 或后续 dev/test
     * 服务器上执行 curl，并不是 local profile 或研发笔记本环境。这样既能人工触发补投，
     * 又不把内部补偿入口暴露给远端调用方。
     */
    private static boolean isLoopback(ServerWebExchange exchange) {
        if (exchange == null || exchange.getRequest() == null) {
            return false;
        }
        InetSocketAddress remoteAddress = exchange.getRequest().getRemoteAddress();
        if (remoteAddress == null) {
            return false;
        }
        InetAddress address = remoteAddress.getAddress();
        return address != null && address.isLoopbackAddress();
    }

    /**
     * 限制人工触发参数规模。
     *
     * <p>真实 XXL-Job handler 接入前，人工入口只覆盖 PEND-001 到 PEND-003 的单笔/小批量联调，
     * 因此限制 shard 和 limit，避免一次 HTTP 调用放大为大批量补偿扫描。workerId 也只允许可读的
     * ASCII 标识，便于在日志和 Redis claim key 中排查。
     */
    private static ManualJobParameters validate(ManualJobRunRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request body is required");
        }
        int shardTotal = positive("shardTotal", request.shardTotal());
        if (shardTotal > MAX_MANUAL_SHARD_TOTAL) {
            throw new IllegalArgumentException("shardTotal must be <= " + MAX_MANUAL_SHARD_TOTAL);
        }
        int shardIndex = nonNegative("shardIndex", request.shardIndex());
        if (shardIndex >= shardTotal) {
            throw new IllegalArgumentException("shardIndex must be less than shardTotal");
        }
        int limit = positive("limit", request.limit());
        if (limit > MAX_MANUAL_LIMIT) {
            throw new IllegalArgumentException("limit must be <= " + MAX_MANUAL_LIMIT);
        }
        String workerId = request.workerId();
        if (!StringUtils.hasText(workerId) || !WORKER_ID_PATTERN.matcher(workerId).matches()) {
            throw new IllegalArgumentException("workerId must match " + WORKER_ID_PATTERN.pattern());
        }
        return new ManualJobParameters(shardTotal, shardIndex, limit, workerId);
    }

    private static int positive(String name, Integer value) {
        if (value == null || value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static int nonNegative(String name, Integer value) {
        if (value == null || value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private Mono<ResponseEntity<ManualJobRunResponse>> handleJobTimeout(
            ManualJobParameters parameters,
            TimeoutException ex) {
        log.error("人工触发Redis pending补投超时 shardTotal={}, shardIndex={}, limit={}, workerId={}, timeoutMs={}",
                parameters.shardTotal(),
                parameters.shardIndex(),
                parameters.limit(),
                parameters.workerId(),
                manualJobTimeout.toMillis(),
                ex);
        return Mono.just(ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body(ManualJobRunResponse.failed(
                        parameters,
                        "Manual integration job timed out after " + manualJobTimeout.toMillis() + "ms")));
    }

    private Mono<ResponseEntity<ManualJobRunResponse>> runManualJob(
            String taskName,
            ManualJobRunRequest request,
            ServerWebExchange exchange,
            ManualJobExecutor executor) {
        if (!isLoopback(exchange)) {
            return Mono.just(ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(ManualJobRunResponse.rejected("Manual integration job endpoint only accepts loopback requests")));
        }
        ManualJobParameters parameters = validate(request);
        return Mono.fromCallable(() -> ResponseEntity.ok(executor.execute(parameters)))
                .subscribeOn(Schedulers.boundedElastic())
                .timeout(manualJobTimeout)
                .onErrorResume(TimeoutException.class, ex -> handleTaskTimeout(taskName, parameters, ex))
                .onErrorResume(RuntimeException.class, ex -> handleTaskFailure(taskName, parameters, ex));
    }

    private Mono<ResponseEntity<ManualJobRunResponse>> handleTaskTimeout(
            String taskName,
            ManualJobParameters parameters,
            TimeoutException ex) {
        log.error("人工触发补偿任务超时 taskName={}, shardTotal={}, shardIndex={}, limit={}, workerId={}, timeoutMs={}",
                taskName,
                parameters.shardTotal(),
                parameters.shardIndex(),
                parameters.limit(),
                parameters.workerId(),
                manualJobTimeout.toMillis(),
                ex);
        return Mono.just(ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body(ManualJobRunResponse.failed(
                        parameters,
                        "Manual integration job timed out after " + manualJobTimeout.toMillis() + "ms")));
    }

    private Mono<ResponseEntity<ManualJobRunResponse>> handleTaskFailure(
            String taskName,
            ManualJobParameters parameters,
            RuntimeException ex) {
        log.error("人工触发补偿任务失败 taskName={}, shardTotal={}, shardIndex={}, limit={}, workerId={}",
                taskName,
                parameters.shardTotal(),
                parameters.shardIndex(),
                parameters.limit(),
                parameters.workerId(),
                ex);
        return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ManualJobRunResponse.failed(
                        parameters,
                        "Manual integration job failed: " + safeMessage(ex))));
    }

    private Mono<ResponseEntity<ManualJobRunResponse>> handleJobFailure(
            ManualJobParameters parameters,
            RuntimeException ex) {
        log.error("人工触发Redis pending补投失败 shardTotal={}, shardIndex={}, limit={}, workerId={}",
                parameters.shardTotal(),
                parameters.shardIndex(),
                parameters.limit(),
                parameters.workerId(),
                ex);
        return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ManualJobRunResponse.failed(
                        parameters,
                        "Manual integration job failed: " + safeMessage(ex))));
    }

    private static Duration normalizeTimeout(Duration timeout) {
        /*
         * Nacos 临时联调配置可能被误填为 0 或负数。这里回落到默认 30s，而不是压成 1ms，
         * 避免 PEND-001/PEND-003 人工触发入口在参数合法时几乎必然返回 FAILED。
         */
        Duration value = timeout == null || timeout.isZero() || timeout.isNegative()
                ? DEFAULT_MANUAL_JOB_TIMEOUT
                : timeout;
        return value;
    }

    private static String safeMessage(RuntimeException ex) {
        String message = ex.getMessage();
        return StringUtils.hasText(message) ? message : ex.getClass().getSimpleName();
    }

    private static <T> T requireJob(Optional<T> maybeJob, String beanName) {
        return maybeJob.orElseThrow(() -> new IllegalStateException(beanName + " is not available"));
    }

    @FunctionalInterface
    private interface ManualJobExecutor {

        ManualJobRunResponse execute(ManualJobParameters parameters);
    }

    public record ManualJobRunRequest(Integer shardTotal, Integer shardIndex, Integer limit, String workerId) {
    }

    public record ManualJobParameters(int shardTotal, int shardIndex, int limit, String workerId) {
    }

    /**
     * 人工补投入口的 HTTP 响应。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」和
     * {@code docs/集成测试方案.md}「8.5 Redis pending 补投联调设计」。该响应会被 test188 上的 curl、
     * 测试辅助前端和 `docs/集成测试.md` 同时作为执行证据使用，因此字段必须保持简单 JSON 类型。`triggeredAt`
     * 使用 ISO-8601 字符串而不是 {@link Instant}，避免 dev/test 部署包中 Jackson 未启用 JSR-310 模块时，
     * 人工入口任务已执行成功却在响应序列化阶段返回 500。
     */
    public record ManualJobRunResponse(
            String status,
            String triggeredAt,
            ManualJobParameters parameters,
            Integer scanned,
            Integer succeeded,
            Integer failed,
            Integer skipped,
            String message) {

        static ManualJobRunResponse succeeded(
                ManualJobParameters parameters,
                RechargeCompensationBatchResult result) {
            return new ManualJobRunResponse(
                    "OK",
                    Instant.now().toString(),
                    parameters,
                    result.scanned(),
                    result.succeeded(),
                    result.failed(),
                    result.skipped(),
                    "");
        }

        static ManualJobRunResponse succeeded(
                ManualJobParameters parameters,
                RechargeOutboxBatchResult result) {
            return new ManualJobRunResponse(
                    "OK",
                    Instant.now().toString(),
                    parameters,
                    result.scanned(),
                    result.succeeded(),
                    result.failed(),
                    result.skipped(),
                    "");
        }

        static ManualJobRunResponse succeeded(
                ManualJobParameters parameters,
                RechargeNotifyBatchResult result) {
            return new ManualJobRunResponse(
                    "OK",
                    Instant.now().toString(),
                    parameters,
                    result.scanned(),
                    result.succeeded(),
                    result.failed(),
                    result.skipped(),
                    "");
        }

        static ManualJobRunResponse rejected(String message) {
            return new ManualJobRunResponse(
                    "REJECTED",
                    Instant.now().toString(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    message);
        }

        static ManualJobRunResponse failed(ManualJobParameters parameters, String message) {
            return new ManualJobRunResponse(
                    "FAILED",
                    Instant.now().toString(),
                    parameters,
                    null,
                    null,
                    null,
                    null,
                    message);
        }
    }
}
