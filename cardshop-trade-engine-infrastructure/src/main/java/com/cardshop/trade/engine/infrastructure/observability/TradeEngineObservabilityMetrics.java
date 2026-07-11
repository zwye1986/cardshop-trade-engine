package com.cardshop.trade.engine.infrastructure.observability;

import com.cardshop.trade.engine.domain.order.AcceptAnomalyStatus;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 交易引擎业务指标注册与热路径事件记录器。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「12.2 指标」和
 * {@code docs/开发进度.md}「5.1 监控告警最小闭环执行边界」。该类刻意只使用低基数标签：
 * environment、status、event_type、anomaly_type、operation、result、fail_code。订单号、商户订单号、
 * 商户号和账号都只能进入日志或集成测试证据，不能进入 Micrometer label。
 */
public class TradeEngineObservabilityMetrics {

    private static final Logger log = LoggerFactory.getLogger(TradeEngineObservabilityMetrics.class);
    private static final String NONE = "none";
    private static final String RESULT_CONFIRMED = "confirmed";
    private static final int MAX_TAG_VALUE_LENGTH = 64;
    private static final long SNAPSHOT_FALLBACK_LOG_INTERVAL_MILLIS = Duration.ofSeconds(60).toMillis();
    private static final AtomicLong LAST_SNAPSHOT_FALLBACK_LOG_MILLIS = new AtomicLong();

    private final TradeEngineObservabilitySnapshotProvider snapshotProvider;
    private final String environment;
    private final Duration luaSlowThreshold;
    private final AtomicInteger mqPublishBreakerOpen = new AtomicInteger();
    private final ConcurrentMap<String, Counter> redisWaitCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> redisLuaSlowCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> redisLuaTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> mqPublishConfirmCounters = new ConcurrentHashMap<>();
    private volatile MeterRegistry registry;

    public TradeEngineObservabilityMetrics(
            TradeEngineObservabilitySnapshotProvider snapshotProvider,
            String environment,
            Duration luaSlowThreshold) {
        this.snapshotProvider = snapshotProvider == null
                ? TradeEngineObservabilitySnapshotProvider.empty()
                : snapshotProvider;
        this.environment = tagValue(environment == null || environment.isBlank() ? "unknown" : environment);
        this.luaSlowThreshold = normalizeThreshold(luaSlowThreshold);
    }

    public static TradeEngineObservabilityMetrics noop() {
        return new TradeEngineObservabilityMetrics(
                TradeEngineObservabilitySnapshotProvider.empty(),
                "noop",
                Duration.ofDays(1));
    }

    public void bindTo(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        registerRedisPendingGauges(registry);
        registerOutboxGauges(registry);
        registerAcceptAnomalyGauges(registry);
        Gauge.builder(
                        "trade.engine.balance.reconcile.diff.orgs",
                        snapshotProvider,
                        provider -> snapshot(provider).balanceReconcileDiffOrgs())
                .description("余额对账最近采样发现的差异商户数量")
                .tag("environment", environment)
                .register(registry);
        Gauge.builder("trade.engine.mq.publish.breaker.open", mqPublishBreakerOpen, AtomicInteger::get)
                .description("RabbitMQ 发布熔断是否打开，1 表示打开")
                .tag("environment", environment)
                .register(registry);
    }

    /**
     * 记录 Redis WAIT 结果。
     *
     * <p>WAIT 是下单热路径事件，只记录低基数 result/fail_code，不写订单维度 label。
     */
    public void recordRedisWait(String result, String failCode) {
        MeterRegistry current = registry;
        if (current == null) {
            return;
        }
        String safeResult = tagValue(result == null || result.isBlank() ? RESULT_CONFIRMED : result);
        String safeFailCode = tagValue(failCode == null || failCode.isBlank() ? NONE : failCode);
        String key = safeResult + "|" + safeFailCode;
        redisWaitCounters.computeIfAbsent(key, ignored -> Counter.builder("trade.engine.redis.wait.total")
                        .description("Redis WAIT 执行结果计数")
                        .tag("environment", environment)
                        .tag("result", safeResult)
                        .tag("fail_code", safeFailCode)
                        .register(current))
                .increment();
    }

    /**
     * 记录 Redis Lua 执行耗时和慢执行计数。
     */
    public void recordRedisLua(String operation, Duration duration, boolean success) {
        MeterRegistry current = registry;
        if (current == null) {
            return;
        }
        String safeOperation = tagValue(operation == null || operation.isBlank() ? "unknown" : operation);
        String key = safeOperation + "|" + success;
        Duration safeDuration = duration == null || duration.isNegative() ? Duration.ZERO : duration;
        redisLuaTimers.computeIfAbsent(key, ignored -> Timer.builder("trade.engine.redis.lua.duration")
                        .description("Redis Lua 执行耗时")
                        .tag("environment", environment)
                        .tag("operation", safeOperation)
                        .tag("result", success ? "success" : "failed")
                        .register(current))
                .record(safeDuration);
        if (safeDuration.compareTo(luaSlowThreshold) > 0) {
            redisLuaSlowCounters.computeIfAbsent(safeOperation, ignored -> Counter.builder("trade.engine.redis.lua.slow.total")
                            .description("Redis Lua 慢执行计数")
                            .tag("environment", environment)
                            .tag("operation", safeOperation)
                            .register(current))
                    .increment();
        }
    }

    /**
     * 记录 RabbitMQ publisher confirm 或发布尝试结果。
     */
    public void recordMqPublishConfirm(String result, String failCode) {
        MeterRegistry current = registry;
        if (current == null) {
            return;
        }
        String safeResult = tagValue(result == null || result.isBlank() ? RESULT_CONFIRMED : result);
        String safeFailCode = tagValue(failCode == null || failCode.isBlank() ? NONE : failCode);
        String key = safeResult + "|" + safeFailCode;
        mqPublishConfirmCounters.computeIfAbsent(key, ignored -> Counter.builder("trade.engine.mq.publish.confirm.total")
                        .description("RabbitMQ 发布确认结果计数")
                        .tag("environment", environment)
                        .tag("result", safeResult)
                        .tag("fail_code", safeFailCode)
                        .register(current))
                .increment();
    }

    public void setMqPublishBreakerOpen(boolean open) {
        mqPublishBreakerOpen.set(open ? 1 : 0);
    }

    private void registerRedisPendingGauges(MeterRegistry registry) {
        Gauge.builder(
                        "trade.engine.redis.pending.total",
                        snapshotProvider,
                        provider -> snapshot(provider).redisPending().total())
                .description("Redis pending 订单总量")
                .tag("environment", environment)
                .register(registry);
        Gauge.builder(
                        "trade.engine.redis.pending.due",
                        snapshotProvider,
                        provider -> snapshot(provider).redisPending().due())
                .description("Redis pending 已到期待补投订单量")
                .tag("environment", environment)
                .register(registry);
        Gauge.builder(
                        "trade.engine.redis.pending.max.age.seconds",
                        snapshotProvider,
                        provider -> snapshot(provider).redisPending().maxAgeSeconds())
                .description("Redis pending 最大滞留时间秒数")
                .tag("environment", environment)
                .register(registry);
    }

    private void registerOutboxGauges(MeterRegistry registry) {
        for (OutboxEventType eventType : OutboxEventType.values()) {
            for (TaskStatus status : TaskStatus.values()) {
                String event = eventType.name();
                String taskStatus = status.name();
                Gauge.builder(
                                "trade.engine.outbox.pending",
                                snapshotProvider,
                                provider -> snapshot(provider).outboxCount(event, taskStatus))
                        .description("Outbox 未完成事件数量")
                        .tag("environment", environment)
                        .tag("event_type", event)
                        .tag("status", taskStatus)
                        .register(registry);
                Gauge.builder(
                                "trade.engine.outbox.max.age.seconds",
                                snapshotProvider,
                                provider -> snapshot(provider).outboxMaxAgeSeconds(event, taskStatus))
                        .description("Outbox 未完成事件最大滞留时间秒数")
                        .tag("environment", environment)
                        .tag("event_type", event)
                        .tag("status", taskStatus)
                        .register(registry);
            }
        }
    }

    private void registerAcceptAnomalyGauges(MeterRegistry registry) {
        for (AcceptAnomalyType anomalyType : AcceptAnomalyType.values()) {
            for (AcceptAnomalyStatus status : AcceptAnomalyStatus.values()) {
                String type = anomalyType.name();
                String anomalyStatus = status.name();
                Gauge.builder(
                                "trade.engine.accept.anomaly.open",
                                snapshotProvider,
                                provider -> snapshot(provider).acceptAnomalyCount(type, anomalyStatus))
                        .description("Redis 接单状态未知异常未关闭记录数")
                        .tag("environment", environment)
                        .tag("anomaly_type", type)
                        .tag("status", anomalyStatus)
                        .register(registry);
            }
        }
    }

    private static TradeEngineObservabilitySnapshot snapshot(TradeEngineObservabilitySnapshotProvider provider) {
        try {
            TradeEngineObservabilitySnapshot snapshot = provider.currentSnapshot();
            return snapshot == null ? TradeEngineObservabilitySnapshot.empty() : snapshot;
        } catch (RuntimeException ex) {
            /*
             * Gauge 读取处于 Prometheus scrape 热路径，不能让异常打断 /actuator/prometheus。
             * 这里按 60 秒采样输出 warn：保留异常栈与故障证据，同时避免 provider 连续失败造成日志风暴。
             */
            long nowMillis = System.currentTimeMillis();
            long lastLoggedMillis = LAST_SNAPSHOT_FALLBACK_LOG_MILLIS.get();
            if (nowMillis - lastLoggedMillis >= SNAPSHOT_FALLBACK_LOG_INTERVAL_MILLIS
                    && LAST_SNAPSHOT_FALLBACK_LOG_MILLIS.compareAndSet(lastLoggedMillis, nowMillis)) {
                log.warn("交易引擎业务监控 Gauge 快照读取失败，返回空快照 fallbackLogIntervalSeconds={}",
                        Duration.ofMillis(SNAPSHOT_FALLBACK_LOG_INTERVAL_MILLIS).toSeconds(),
                        ex);
            }
            return TradeEngineObservabilitySnapshot.empty();
        }
    }

    private static Duration normalizeThreshold(Duration threshold) {
        if (threshold == null || threshold.isNegative() || threshold.isZero()) {
            return Duration.ofMillis(30);
        }
        return threshold;
    }

    private static String tagValue(String raw) {
        String value = raw == null || raw.isBlank() ? "unknown" : raw.strip();
        value = value.replaceAll("[^A-Za-z0-9_.:-]", "_");
        if (value.length() > MAX_TAG_VALUE_LENGTH) {
            return value.substring(0, MAX_TAG_VALUE_LENGTH);
        }
        return value;
    }
}
