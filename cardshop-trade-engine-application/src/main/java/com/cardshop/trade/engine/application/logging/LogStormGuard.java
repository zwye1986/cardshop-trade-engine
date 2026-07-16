package com.cardshop.trade.engine.application.logging;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 高频异常路径的日志防风暴决策器。
 *
 * <p>对应 {@code AGENTS.md}「日志风暴防护」规则。Redis、MQ、HTTP 回调等按请求或消息逐条执行的
 * catch 分支仍需要保留可观测性，但不能在中间件故障时把 error 日志量放大到请求量级；本类只负责按事件
 * key 计数并给出首条/采样条是否应记录，具体日志级别、堆栈和业务上下文仍由调用方决定。
 */
public final class LogStormGuard {

    /**
     * 默认每 100 次同类事件记录一次采样日志，兼顾事故首现场证据和日志系统承载能力。
     */
    public static final int DEFAULT_SAMPLE_INTERVAL = 100;

    private final int sampleInterval;
    private final Duration timeWindow;
    private final Clock clock;
    private final ConcurrentMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TimeWindowState> timeWindowStates = new ConcurrentHashMap<>();

    private LogStormGuard(int sampleInterval, Duration timeWindow, Clock clock) {
        if (sampleInterval <= 0) {
            throw new IllegalArgumentException("sampleInterval must be positive");
        }
        this.sampleInterval = sampleInterval;
        this.timeWindow = timeWindow;
        this.clock = clock;
    }

    /**
     * 创建使用默认采样间隔的日志防风暴决策器。
     */
    public static LogStormGuard defaultGuard() {
        return everyNth(DEFAULT_SAMPLE_INTERVAL);
    }

    /**
     * 创建每 {@code sampleInterval} 次同类事件记录一次采样日志的决策器。
     */
    public static LogStormGuard everyNth(int sampleInterval) {
        return new LogStormGuard(sampleInterval, null, null);
    }

    /**
     * 创建同一事件在固定时间窗内最多记录一次的日志防风暴决策器。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「12. 可观测性」。适用于 Redis 故障、缓存脏数据
     * 等按请求高频触发但仍需周期保留异常栈的路径；不同事件 key 分别维护时间窗。
     */
    public static LogStormGuard atMostOncePer(Duration window) {
        return atMostOncePer(window, Clock.systemUTC());
    }

    static LogStormGuard atMostOncePer(Duration window, Clock clock) {
        if (window == null || window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("window must be positive");
        }
        return new LogStormGuard(1, window, Objects.requireNonNull(clock, "clock must not be null"));
    }

    /**
     * 记录一次事件并返回本次是否应该写日志。
     *
     * <p>同一 {@code eventKey} 独立计数；调用方应使用稳定、非敏感的业务事件名，避免把订单号、URL、
     * token 等高基数字段放入 key，否则限频会失效。
     */
    public LogDecision shouldLog(String eventKey) {
        String normalizedEventKey = Objects.requireNonNull(eventKey, "eventKey must not be null").trim();
        if (normalizedEventKey.isEmpty()) {
            throw new IllegalArgumentException("eventKey must not be blank");
        }
        if (timeWindow != null) {
            return timeWindowStates
                    .computeIfAbsent(normalizedEventKey, ignored -> new TimeWindowState())
                    .shouldLog(clock.millis(), timeWindow.toMillis());
        }
        long occurrence = counters.computeIfAbsent(normalizedEventKey, ignored -> new AtomicLong()).incrementAndGet();
        boolean firstOccurrence = occurrence == 1;
        boolean sampledOccurrence = !firstOccurrence && occurrence % sampleInterval == 0;
        return new LogDecision(firstOccurrence || sampledOccurrence, occurrence, firstOccurrence, sampledOccurrence);
    }

    private static final class TimeWindowState {

        private long occurrence;
        private long lastLoggedMillis;

        private synchronized LogDecision shouldLog(long nowMillis, long windowMillis) {
            occurrence++;
            boolean firstOccurrence = occurrence == 1;
            boolean sampledOccurrence = !firstOccurrence && nowMillis - lastLoggedMillis >= windowMillis;
            boolean shouldLog = firstOccurrence || sampledOccurrence;
            if (shouldLog) {
                lastLoggedMillis = nowMillis;
            }
            return new LogDecision(shouldLog, occurrence, firstOccurrence, sampledOccurrence);
        }
    }

    /**
     * 单次日志限频判断结果。
     */
    public record LogDecision(
            boolean shouldLog,
            long occurrence,
            boolean firstOccurrence,
            boolean sampledOccurrence) {
    }
}
