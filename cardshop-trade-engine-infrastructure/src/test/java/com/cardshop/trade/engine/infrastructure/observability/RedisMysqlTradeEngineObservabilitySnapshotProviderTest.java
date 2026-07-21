package com.cardshop.trade.engine.infrastructure.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.domain.order.AcceptAnomalyStatus;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import org.junit.jupiter.api.Test;

/**
 * Redis/MySQL 业务监控快照 Provider 测试。
 *
 * <p>参见 {@code docs/开发进度.md}「5.1 监控告警最小闭环执行边界」。Provider 必须缓存 Redis/MySQL
 * 聚合结果，Prometheus scrape 只能读取快照，不能每次请求都扫描共享旧卡管测试库或 Redis pending。
 */
class RedisMysqlTradeEngineObservabilitySnapshotProviderTest {

    @Test
    void cachesSnapshotWithinTtlToAvoidScrapeAmplifyingRedisAndMysqlQueries() {
        var clock = new MutableClock(Instant.parse("2026-07-11T10:00:00Z"));
        var operations = new QueueOperations(snapshot(12, 5));
        var provider = new RedisMysqlTradeEngineObservabilitySnapshotProvider(
                operations,
                clock,
                Duration.ofSeconds(15));

        TradeEngineObservabilitySnapshot first = provider.currentSnapshot();
        TradeEngineObservabilitySnapshot second = provider.currentSnapshot();
        clock.advance(Duration.ofSeconds(16));
        TradeEngineObservabilitySnapshot third = provider.currentSnapshot();

        assertThat(first.redisPending().total()).isEqualTo(12);
        assertThat(second.redisPending().total()).isEqualTo(12);
        assertThat(third.redisPending().total()).isEqualTo(12);
        assertThat(operations.refreshCount).isEqualTo(2);
    }

    @Test
    void returnsLastSnapshotWhenRefreshFails() {
        var clock = new MutableClock(Instant.parse("2026-07-11T10:00:00Z"));
        var operations = new QueueOperations(snapshot(7, 2));
        operations.snapshots.add(new IllegalStateException("mysql timeout"));
        var provider = new RedisMysqlTradeEngineObservabilitySnapshotProvider(
                operations,
                clock,
                Duration.ofSeconds(1));

        TradeEngineObservabilitySnapshot first = provider.currentSnapshot();
        clock.advance(Duration.ofSeconds(2));
        TradeEngineObservabilitySnapshot fallback = provider.currentSnapshot();

        assertThat(first.redisPending().total()).isEqualTo(7);
        assertThat(fallback.redisPending().total()).isEqualTo(7);
        assertThat(operations.refreshCount).isEqualTo(2);
    }

    @Test
    void exposesAggregatedOutboxAnomalyAndBalanceValues() {
        var clock = new MutableClock(Instant.parse("2026-07-11T10:00:00Z"));
        var operations = new QueueOperations(new TradeEngineObservabilitySnapshot(
                new TradeEngineObservabilitySnapshot.RedisPending(3, 1, 44),
                Map.of(
                        new TradeEngineObservabilitySnapshot.OutboxKey(
                                OutboxEventType.SUPPLIER_QUERY.name(), TaskStatus.FAILED.name()),
                        new TradeEngineObservabilitySnapshot.CountAge(6, 300)),
                Map.of(
                        TaskStatus.INIT.name(),
                        new TradeEngineObservabilitySnapshot.CountAge(2, 90)),
                Map.of(
                        new TradeEngineObservabilitySnapshot.AcceptAnomalyKey(
                                AcceptAnomalyType.REDIS_BUSY.name(), AcceptAnomalyStatus.INIT.name()),
                        4L),
                2,
                false));
        var provider = new RedisMysqlTradeEngineObservabilitySnapshotProvider(
                operations,
                clock,
                Duration.ofSeconds(15));

        TradeEngineObservabilitySnapshot snapshot = provider.currentSnapshot();

        assertThat(snapshot.redisPending().due()).isEqualTo(1);
        assertThat(snapshot.outboxCount(OutboxEventType.SUPPLIER_QUERY.name(), TaskStatus.FAILED.name()))
                .isEqualTo(6.0);
        assertThat(snapshot.outboxMaxAgeSeconds(OutboxEventType.SUPPLIER_QUERY.name(), TaskStatus.FAILED.name()))
                .isEqualTo(300.0);
        assertThat(snapshot.balanceDeltaCount(TaskStatus.INIT.name())).isEqualTo(2.0);
        assertThat(snapshot.acceptAnomalyCount(AcceptAnomalyType.REDIS_BUSY.name(), AcceptAnomalyStatus.INIT.name()))
                .isEqualTo(4.0);
        assertThat(snapshot.balanceReconcileDiffOrgs()).isEqualTo(2);
    }

    @Test
    void convertsClockInstantToLegacyCardshopDbZoneWhenQueryingMysqlAggregates() {
        Clock clock = Clock.fixed(Instant.parse("2026-07-11T10:00:00Z"), ZoneOffset.UTC);

        LocalDateTime now = RedisMysqlTradeEngineObservabilitySnapshotProvider.legacyCardshopDbNow(clock);

        assertThat(now).isEqualTo(LocalDateTime.parse("2026-07-11T18:00:00"));
    }

    private static TradeEngineObservabilitySnapshot snapshot(long total, long due) {
        return new TradeEngineObservabilitySnapshot(
                new TradeEngineObservabilitySnapshot.RedisPending(total, due, 60),
                Map.of(),
                Map.of(),
                Map.of(),
                0,
                false);
    }

    private static final class QueueOperations implements RedisMysqlTradeEngineObservabilitySnapshotProvider.Operations {

        private final Queue<Object> snapshots = new ArrayDeque<>();
        private TradeEngineObservabilitySnapshot lastSnapshot;
        private int refreshCount;

        private QueueOperations(TradeEngineObservabilitySnapshot firstSnapshot) {
            snapshots.add(firstSnapshot);
            lastSnapshot = firstSnapshot;
        }

        @Override
        public TradeEngineObservabilitySnapshot refreshSnapshot() {
            refreshCount++;
            Object next = snapshots.poll();
            if (next instanceof RuntimeException ex) {
                throw ex;
            }
            if (next instanceof TradeEngineObservabilitySnapshot snapshot) {
                lastSnapshot = snapshot;
                return snapshot;
            }
            return lastSnapshot;
        }
    }

    private static final class MutableClock extends Clock {

        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
