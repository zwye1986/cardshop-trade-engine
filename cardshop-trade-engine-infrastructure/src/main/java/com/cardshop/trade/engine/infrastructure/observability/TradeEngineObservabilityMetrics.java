package com.cardshop.trade.engine.infrastructure.observability;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.v1.V1CreateOrderStageObserver;
import com.cardshop.trade.engine.application.v1.V1MerchantProfileCacheObserver;
import com.cardshop.trade.engine.application.v1.V1ProductCacheObserver;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyStatus;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
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
public class TradeEngineObservabilityMetrics
        implements V1ProductCacheObserver, V1MerchantProfileCacheObserver, V1CreateOrderStageObserver {

    private static final Logger log = LoggerFactory.getLogger(TradeEngineObservabilityMetrics.class);
    private static final String NONE = "none";
    private static final String RESULT_CONFIRMED = "confirmed";
    private static final int MAX_TAG_VALUE_LENGTH = 64;
    private static final Duration SNAPSHOT_FALLBACK_LOG_INTERVAL = Duration.ofSeconds(60);
    private static final String LOG_EVENT_SNAPSHOT_FALLBACK = "observability.snapshot.fallback";
    private static final LogStormGuard SNAPSHOT_FALLBACK_LOG_GUARD =
            LogStormGuard.atMostOncePer(SNAPSHOT_FALLBACK_LOG_INTERVAL);

    private final TradeEngineObservabilitySnapshotProvider snapshotProvider;
    private final String environment;
    private final Duration luaSlowThreshold;
    private final AtomicInteger mqPublishBreakerOpen = new AtomicInteger();
    private final ConcurrentMap<String, Counter> redisWaitCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> redisLuaSlowCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> redisLuaTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> mqPublishConfirmCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> productCacheHitCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DistributionSummary> productCacheAgeSummaries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> productCacheRefreshCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> productCacheRefreshTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> merchantProfileCacheHitCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> createOrderStageTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> persistBatchOrderCounters = new ConcurrentHashMap<>();
    private volatile Counter productCacheMysqlLoadCounter;
    private volatile DistributionSummary persistBatchSizeSummary;
    private volatile Timer persistBatchMysqlTimer;
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

    /**
     * 记录 V1 父商品选品缓存命中结果和快照年龄。
     *
     * <p>参见详细设计「12. 可观测性」。result 仅允许 fresh、soft_stale、hard_stale、miss、negative
     * 等低基数枚举，不接收商品号或商户号。
     */
    @Override
    public void recordCacheResult(String result, Duration age) {
        MeterRegistry current = registry;
        if (current == null) {
            return;
        }
        String safeResult = tagValue(result == null || result.isBlank() ? "unknown" : result);
        productCacheHitCounters.computeIfAbsent(safeResult, ignored -> Counter.builder(
                                "trade.engine.product.cache.hit.total")
                        .description("V1父商品选品缓存读取结果计数")
                        .tag("environment", environment)
                        .tag("result", safeResult)
                        .register(current))
                .increment();
        Duration safeAge = age == null || age.isNegative() ? Duration.ZERO : age;
        productCacheAgeSummaries.computeIfAbsent(safeResult, ignored -> DistributionSummary.builder(
                                "trade.engine.product.cache.age.seconds")
                        .description("V1父商品选品缓存快照年龄秒数")
                        .baseUnit("seconds")
                        .tag("environment", environment)
                        .tag("result", safeResult)
                        .register(current))
                .record(safeAge.toMillis() / 1000.0);
    }

    /**
     * 记录 V1 商品缓存单飞刷新结果和耗时。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「12. 观测、日志与告警」。result 使用固定
     * 低基数枚举，刷新耗时用于识别 Redis/MySQL 抖动和硬过期等待风险。
     */
    @Override
    public void recordRefresh(String result, Duration duration) {
        MeterRegistry current = registry;
        if (current == null) {
            return;
        }
        String safeResult = tagValue(result == null || result.isBlank() ? "unknown" : result);
        productCacheRefreshCounters.computeIfAbsent(safeResult, ignored -> Counter.builder(
                                "trade.engine.product.cache.refresh.total")
                        .description("V1父商品选品缓存刷新结果计数")
                        .tag("environment", environment)
                        .tag("result", safeResult)
                        .register(current))
                .increment();
        Duration safeDuration = duration == null || duration.isNegative() ? Duration.ZERO : duration;
        productCacheRefreshTimers.computeIfAbsent(safeResult, ignored -> Timer.builder(
                                "trade.engine.product.cache.refresh.duration")
                        .description("V1父商品选品缓存刷新耗时")
                        .tag("environment", environment)
                        .tag("result", safeResult)
                        .register(current))
                .record(safeDuration);
    }

    /**
     * 记录实际访问旧卡管 MySQL 商品表的次数，用于验证秒杀流量没有退化成每单回源。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「12. 观测、日志与告警」。该指标不携带
     * 商户号或商品号标签，避免高基数时序放大监控系统压力。
     */
    @Override
    public void recordMysqlLoad() {
        MeterRegistry current = registry;
        if (current == null) {
            return;
        }
        Counter counter = productCacheMysqlLoadCounter;
        if (counter == null) {
            synchronized (this) {
                counter = productCacheMysqlLoadCounter;
                if (counter == null) {
                    counter = Counter.builder("trade.engine.product.cache.mysql.load.total")
                            .description("V1父商品选品缓存实际MySQL回源次数")
                            .tag("environment", environment)
                            .register(current);
                    productCacheMysqlLoadCounter = counter;
                }
            }
        }
        counter.increment();
    }

    /**
     * 记录 V1 商户资料短 TTL 本地缓存查找结果。
     *
     * <p>参见详细设计「3. API/V1 接入层」和「12. 可观测性」。result 仅允许 hit/negative/miss，
     * lookup_type 仅允许 app_org/org；禁止 appId、orgCode、aesKey 进入标签。
     */
    @Override
    public void recordLookup(String result, String lookupType) {
        MeterRegistry current = registry;
        if (current == null) {
            return;
        }
        String safeResult = tagValue(result == null || result.isBlank() ? "unknown" : result);
        String safeLookupType = tagValue(lookupType == null || lookupType.isBlank() ? "unknown" : lookupType);
        String key = safeResult + "|" + safeLookupType;
        merchantProfileCacheHitCounters.computeIfAbsent(key, ignored -> Counter.builder(
                                "trade.engine.merchant.profile.cache.hit.total")
                        .description("V1商户资料短TTL本地缓存查找结果计数")
                        .tag("environment", environment)
                        .tag("result", safeResult)
                        .tag("lookup_type", safeLookupType)
                        .register(current))
                .increment();
    }

    /**
     * 记录异步落库批大小。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。只记录批大小本身，
     * 不写订单号等业务标签。Prometheus 暴露后名称为 {@code trade_engine_persist_batch_size}。
     */
    public void recordPersistBatchSize(int batchSize) {
        MeterRegistry current = registry;
        if (current == null || batchSize < 0) {
            return;
        }
        DistributionSummary summary = persistBatchSizeSummary;
        if (summary == null) {
            synchronized (this) {
                summary = persistBatchSizeSummary;
                if (summary == null) {
                    summary = DistributionSummary.builder("trade.engine.persist.batch.size")
                            .description("异步落库批消费实际批大小")
                            .tag("environment", environment)
                            .register(current);
                    persistBatchSizeSummary = summary;
                }
            }
        }
        summary.record(batchSize);
    }

    /**
     * 记录异步落库批 MySQL 事务耗时。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。该耗时应覆盖
     * MyBatis BATCH flush、事务 commit/rollback 与锁等待，而不是仅方法体内攒 SQL 的时间；由批写
     * adapter 在事务 {@code afterCompletion} 上报。成功与失败都应记录。Prometheus 暴露后名称为
     * {@code trade_engine_persist_batch_mysql_duration_seconds}。
     */
    public void recordPersistBatchMysqlDuration(Duration duration) {
        MeterRegistry current = registry;
        if (current == null) {
            return;
        }
        Duration safeDuration = duration == null || duration.isNegative() ? Duration.ZERO : duration;
        Timer timer = persistBatchMysqlTimer;
        if (timer == null) {
            synchronized (this) {
                timer = persistBatchMysqlTimer;
                if (timer == null) {
                    timer = Timer.builder("trade.engine.persist.batch.mysql.duration")
                            .description("异步落库批 MySQL 事务耗时")
                            .tag("environment", environment)
                            .register(current);
                    persistBatchMysqlTimer = timer;
                }
            }
        }
        timer.record(safeDuration);
    }

    /**
     * 记录异步落库批内订单结果计数。
     *
     * <p>result 仅允许 {@code success}/{@code failed}/{@code skipped_already_persisted} 等低基数取值，禁止订单号进标签。
     * Prometheus 暴露后名称为 {@code trade_engine_persist_batch_orders_total}。
     */
    public void recordPersistBatchOrders(int orderCount, String result) {
        MeterRegistry current = registry;
        if (current == null || orderCount <= 0) {
            return;
        }
        String safeResult = tagValue(result == null || result.isBlank() ? "unknown" : result);
        persistBatchOrderCounters.computeIfAbsent(safeResult, ignored -> Counter.builder(
                                "trade.engine.persist.batch.orders.total")
                        .description("异步落库批内订单结果计数")
                        .tag("environment", environment)
                        .tag("result", safeResult)
                        .register(current))
                .increment(orderCount);
    }

    /**
     * 记录 V1 下单同步子阶段耗时。
     *
     * <p>参见详细设计「12. 可观测性」。stage 仅允许 prepare/accept/wait/mq_confirm/
     * mark_persist_queued/redis_session_connect/sync_total。
     */
    @Override
    public void recordStage(String stage, Duration duration) {
        MeterRegistry current = registry;
        if (current == null) {
            return;
        }
        String safeStage = tagValue(stage == null || stage.isBlank() ? "unknown" : stage);
        Duration safeDuration = duration == null || duration.isNegative() ? Duration.ZERO : duration;
        createOrderStageTimers.computeIfAbsent(safeStage, ignored -> Timer.builder(
                                "trade.engine.v1.create.stage.duration")
                        .description("V1下单同步路径子阶段耗时")
                        .tag("environment", environment)
                        .tag("stage", safeStage)
                        .register(current))
                .record(safeDuration);
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
            if (SNAPSHOT_FALLBACK_LOG_GUARD.shouldLog(LOG_EVENT_SNAPSHOT_FALLBACK).shouldLog()) {
                log.warn("交易引擎业务监控 Gauge 快照读取失败，返回空快照 fallbackLogIntervalSeconds={}",
                        SNAPSHOT_FALLBACK_LOG_INTERVAL.toSeconds(),
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
