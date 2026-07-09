package com.cardshop.trade.engine.bootstrap.admin;

import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Objects;
import java.util.regex.Pattern;
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

/**
 * dev/test 环境人工触发补偿任务的受控入口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.11 XXL-Job Handler 契约」、
 * 「20.13.15 /v1/cardapi 测试清单」以及 {@code docs/集成测试方案.md}
 * 「6.4.1 Redis pending 补投专项用例」和「8.5 Redis pending 补投联调设计」。这个 Controller 只为 PEND-001/PEND-002/PEND-003
 * 真实联调提供临时触发能力：必须显式打开 {@code trade.engine.integration.manual-job-enabled=true}，
 * 且只能在 {@code dev/test} profile、dev/test 测试机本机回环地址调用。这里不提供任意 Bean/方法调用能力，
 * 只暴露 Redis pending MQ 补投这一条固定链路，避免把内部补偿任务变成泛化远程执行入口。
 */
@RestController
@Profile({"dev", "test"})
@ConditionalOnProperty(prefix = "trade.engine.integration", name = "manual-job-enabled", havingValue = "true")
@RequestMapping("/internal/integration/recharge/jobs")
public class ManualRechargeJobController {

    private static final int MAX_MANUAL_SHARD_TOTAL = 128;
    private static final int MAX_MANUAL_LIMIT = 100;
    private static final Pattern WORKER_ID_PATTERN = Pattern.compile("[A-Za-z0-9._:-]{1,80}");

    private final RechargePersistMqRepublishJob persistMqRepublishJob;

    public ManualRechargeJobController(RechargePersistMqRepublishJob persistMqRepublishJob) {
        this.persistMqRepublishJob = Objects.requireNonNull(
                persistMqRepublishJob,
                "persistMqRepublishJob must not be null");
    }

    @PostMapping("/persist-mq-republish/run-once")
    public ResponseEntity<ManualJobRunResponse> runPersistMqRepublishOnce(
            @RequestBody ManualJobRunRequest request,
            ServerWebExchange exchange) {
        /*
         * 安全边界：本入口只服务 dev/test 真实联调。即使开关被误打开，也要求请求来自当前测试机的
         * loopback 地址，避免 RabbitMQ/MySQL/Redis 补偿任务被远程任意触发。详见详细设计「20.11
         * XXL-Job Handler 契约」中关于 runOnce 门面仅承接固定补偿链路的约束。
         */
        if (!isLoopback(exchange)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(ManualJobRunResponse.rejected("Manual integration job endpoint only accepts loopback requests"));
        }
        ManualJobParameters parameters = validate(request);
        RechargeCompensationBatchResult result = persistMqRepublishJob.runOnce(
                parameters.shardTotal(),
                parameters.shardIndex(),
                parameters.limit(),
                parameters.workerId());
        return ResponseEntity.ok(ManualJobRunResponse.succeeded(parameters, result));
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

    public record ManualJobRunRequest(Integer shardTotal, Integer shardIndex, Integer limit, String workerId) {
    }

    public record ManualJobParameters(int shardTotal, int shardIndex, int limit, String workerId) {
    }

    public record ManualJobRunResponse(
            String status,
            Instant triggeredAt,
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
                    Instant.now(),
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
                    Instant.now(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    message);
        }
    }
}
