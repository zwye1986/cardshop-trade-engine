package com.cardshop.trade.engine.infrastructure.observability;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoWriteOffEventMapper;

import io.micrometer.core.instrument.*;

import org.slf4j.*;
import org.springframework.beans.factory.ObjectProvider;

import java.time.*;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;

/** 低基数指标与Job轮末缓存聚合；参见docs/交行直充业务系统详细设计.md「9.5」，scrape线程不访问数据库。 */
public final class MinisoWriteOffMetrics implements MinisoWriteOffObserver {
    private static final Logger LOG = LoggerFactory.getLogger(MinisoWriteOffMetrics.class);
    private final MeterRegistry registry;
    private final ObjectProvider<MinisoWriteOffEventMapper> events;
    private final Clock clock;
    private final LogStormGuard logs = LogStormGuard.defaultGuard();
    private final AtomicLong pending = new AtomicLong(),
            manual = new AtomicLong(),
            oldest = new AtomicLong(),
            notifications = new AtomicLong(),
            lastScan = new AtomicLong();

    public MinisoWriteOffMetrics(
            MeterRegistry registry, ObjectProvider<MinisoWriteOffEventMapper> events, Clock clock) {
        this.registry = registry;
        this.events = events;
        this.clock = clock;
        registry.gauge("trade.miniso.writeoff.pending", pending);
        registry.gauge("trade.miniso.writeoff.manual", manual);
        registry.gauge("trade.miniso.writeoff.notifications.pending", notifications);
        registry.gauge("trade.miniso.writeoff.last.scan.epoch.seconds", lastScan);
        Gauge.builder(
                        "trade.miniso.writeoff.oldest.age.seconds",
                        () ->
                                oldest.get() == 0
                                        ? 0
                                        : Math.max(
                                                0, clock.instant().getEpochSecond() - oldest.get()))
                .register(registry);
    }

    public void started() {
        refresh();
    }

    public void event(String category) {
        if ("accepted".equals(category)) {
            pending.incrementAndGet();
            oldest.compareAndSet(0, clock.instant().getEpochSecond());
        }
        registry.counter("trade.miniso.writeoff.received", "outcome", category).increment();
    }

    public void batch(MinisoWriteOffBatchResult result) {
        count("processed", result.processed());
        count("retried", result.retried());
        count("manual", result.manual());
        count("deferred", result.deferred());
        count("failed", result.failed());
        lastScan.set(clock.instant().getEpochSecond());
        refresh();
    }

    private void refresh() {
        try {
            var mapper = events.getIfAvailable();
            if (mapper == null) return;
            var stats = mapper.pendingStats();
            pending.set(stats.pending());
            manual.set(stats.manual());
            oldest.set(
                    stats.oldest() == null
                            ? 0
                            : stats.oldest().atZone(ZoneId.of("Asia/Shanghai")).toEpochSecond());
            notifications.set(mapper.pendingNotifications());
        } catch (RuntimeException ex) {
            // 指标刷新失败不改变已经提交的业务结果；轮末采样安全栈，不记录原SQL异常。
            if (logs.shouldLog("miniso.metrics.failed").shouldLog())
                LOG.error(
                        "名创核销指标刷新失败",
                        com.cardshop.trade.engine.infrastructure.persistence.MinisoWriteOffFailures
                                .storage(ex));
        }
    }

    private void count(String outcome, int n) {
        registry.counter("trade.miniso.writeoff.processed", "outcome", outcome).increment(n);
    }
}
