package com.cardshop.trade.engine.infrastructure.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.v1.V1CreateOrderStageObserver;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyStatus;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

/**
 * 交易引擎业务监控指标测试。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「12.2 指标」和
 * {@code docs/开发进度.md}「5.1 监控告警最小闭环执行边界」。测试只允许低基数标签进入 Micrometer，
 * 避免把订单号、商户订单号或商户号打成 Prometheus time-series。
 */
@ExtendWith(OutputCaptureExtension.class)
class TradeEngineObservabilityMetricsTest {

    @Test
    void registersLowCardinalityBusinessGaugesFromSnapshotProvider() {
        var registry = new SimpleMeterRegistry();
        var provider = new StaticSnapshotProvider(new TradeEngineObservabilitySnapshot(
                new TradeEngineObservabilitySnapshot.RedisPending(12, 5, 90),
                Map.of(
                        new TradeEngineObservabilitySnapshot.OutboxKey(
                                OutboxEventType.SUPPLIER_DISPATCH.name(), TaskStatus.INIT.name()),
                        new TradeEngineObservabilitySnapshot.CountAge(3, 120)),
                Map.of(
                        TaskStatus.MANUAL_REQUIRED.name(),
                        new TradeEngineObservabilitySnapshot.CountAge(1, 600)),
                Map.of(
                        new TradeEngineObservabilitySnapshot.AcceptAnomalyKey(
                                AcceptAnomalyType.LUA_TIMEOUT.name(), AcceptAnomalyStatus.MANUAL_REQUIRED.name()),
                        2L),
                4,
                false));
        new TradeEngineObservabilityMetrics(provider, "test", Duration.ofMillis(30)).bindTo(registry);

        assertThat(registry.get("trade.engine.redis.pending.total").tag("environment", "test").gauge().value())
                .isEqualTo(12.0);
        assertThat(registry.get("trade.engine.redis.pending.due").tag("environment", "test").gauge().value())
                .isEqualTo(5.0);
        assertThat(registry.get("trade.engine.redis.pending.max.age.seconds").tag("environment", "test").gauge().value())
                .isEqualTo(90.0);
        assertThat(registry.get("trade.engine.dispatch.outbox.pending.total")
                        .tag("environment", "test")
                        .tag("event_type", OutboxEventType.SUPPLIER_DISPATCH.name())
                        .tag("status", TaskStatus.INIT.name())
                        .gauge()
                        .value())
                .isEqualTo(3.0);
        assertThat(registry.get("trade.engine.outbox.pending")
                        .tag("environment", "test")
                        .tag("event_type", OutboxEventType.SUPPLIER_DISPATCH.name())
                        .tag("status", TaskStatus.INIT.name())
                        .gauge()
                        .value())
                .isEqualTo(3.0);
        assertThat(registry.get("trade.engine.balance.delta.pending.total")
                        .tag("environment", "test")
                        .tag("status", TaskStatus.MANUAL_REQUIRED.name())
                        .gauge()
                        .value())
                .isEqualTo(1.0);
        assertThat(registry.get("trade.engine.outbox.max.age.seconds")
                        .tag("environment", "test")
                        .tag("event_type", OutboxEventType.BALANCE_DELTA_FLUSH.name())
                        .tag("status", TaskStatus.MANUAL_REQUIRED.name())
                        .gauge()
                        .value())
                .isEqualTo(600.0);
        assertThat(registry.get("trade.engine.accept.anomaly.open")
                        .tag("environment", "test")
                        .tag("anomaly_type", AcceptAnomalyType.LUA_TIMEOUT.name())
                        .tag("status", AcceptAnomalyStatus.MANUAL_REQUIRED.name())
                        .gauge()
                        .value())
                .isEqualTo(2.0);
        assertThat(registry.get("trade.engine.balance.reconcile.diff.orgs")
                        .tag("environment", "test")
                        .gauge()
                        .value())
                .isEqualTo(4.0);

        List<String> tagKeys = registry.getMeters()
                .stream()
                .map(Meter::getId)
                .flatMap(id -> id.getTags().stream())
                .map(tag -> tag.getKey())
                .toList();
        assertThat(tagKeys)
                .doesNotContain("orderId", "outOrderSn", "orgCode", "phone", "account");
    }

    @Test
    void recordsWaitLuaMqAndBreakerEventsWithLowCardinalityTags() {
        var registry = new SimpleMeterRegistry();
        var metrics = new TradeEngineObservabilityMetrics(
                TradeEngineObservabilitySnapshotProvider.empty(),
                "test",
                Duration.ofMillis(30));
        metrics.bindTo(registry);

        metrics.recordRedisWait("missed", "WAIT_ERROR");
        metrics.recordRedisLua("accept", Duration.ofMillis(45), true);
        metrics.recordRedisLua("mark_db_persisted", Duration.ofMillis(5), true);
        metrics.recordMqPublishConfirm("failed", "CONFIRM_TIMEOUT");
        metrics.setMqPublishBreakerOpen(true);

        assertThat(registry.get("trade.engine.redis.wait.total")
                        .tag("environment", "test")
                        .tag("result", "missed")
                        .tag("fail_code", "WAIT_ERROR")
                        .counter()
                        .count())
                .isEqualTo(1.0);
        assertThat(registry.get("trade.engine.redis.lua.slow.total")
                        .tag("environment", "test")
                        .tag("operation", "accept")
                        .counter()
                        .count())
                .isEqualTo(1.0);
        assertThat(registry.get("trade.engine.redis.lua.duration")
                        .tag("environment", "test")
                        .tag("operation", "accept")
                        .timer()
                        .count())
                .isEqualTo(1L);
        assertThat(registry.get("trade.engine.mq.publish.confirm.total")
                        .tag("environment", "test")
                        .tag("result", "failed")
                        .tag("fail_code", "CONFIRM_TIMEOUT")
                        .counter()
                        .count())
                .isEqualTo(1.0);
        assertThat(registry.get("trade.engine.mq.publish.breaker.open")
                        .tag("environment", "test")
                        .gauge()
                        .value())
                .isEqualTo(1.0);
    }

    @Test
    void recordsProductCacheEventsWithOnlyResultAndEnvironmentTags() {
        var registry = new SimpleMeterRegistry();
        var metrics = new TradeEngineObservabilityMetrics(
                TradeEngineObservabilitySnapshotProvider.empty(),
                "test",
                Duration.ofMillis(30));
        metrics.bindTo(registry);

        metrics.recordCacheResult("fresh", Duration.ofMinutes(2));
        metrics.recordRefresh("success", Duration.ofMillis(12));
        metrics.recordMysqlLoad();

        assertThat(registry.get("trade.engine.product.cache.hit.total")
                        .tag("environment", "test")
                        .tag("result", "fresh")
                        .counter()
                        .count())
                .isEqualTo(1.0);
        assertThat(registry.get("trade.engine.product.cache.refresh.total")
                        .tag("environment", "test")
                        .tag("result", "success")
                        .counter()
                        .count())
                .isEqualTo(1.0);
        assertThat(registry.get("trade.engine.product.cache.refresh.duration")
                        .tag("environment", "test")
                        .tag("result", "success")
                        .timer()
                        .count())
                .isEqualTo(1L);
        assertThat(registry.get("trade.engine.product.cache.age.seconds")
                        .tag("environment", "test")
                        .tag("result", "fresh")
                        .summary()
                        .count())
                .isEqualTo(1L);
        assertThat(registry.get("trade.engine.product.cache.mysql.load.total")
                        .tag("environment", "test")
                        .counter()
                        .count())
                .isEqualTo(1.0);

        List<String> productCacheTagKeys = registry.getMeters().stream()
                .map(Meter::getId)
                .filter(id -> id.getName().contains("product.cache"))
                .flatMap(id -> id.getTags().stream())
                .map(tag -> tag.getKey())
                .distinct()
                .toList();
        assertThat(productCacheTagKeys).containsOnly("environment", "result");
    }

    @Test
    void recordsMerchantProfileCacheLookupsWithOnlyResultLookupTypeAndEnvironmentTags() {
        var registry = new SimpleMeterRegistry();
        var metrics = new TradeEngineObservabilityMetrics(
                TradeEngineObservabilitySnapshotProvider.empty(),
                "test",
                Duration.ofMillis(30));
        metrics.bindTo(registry);

        metrics.recordLookup("miss", "app_org");
        metrics.recordLookup("hit", "app_org");
        metrics.recordLookup("negative", "org");

        assertThat(registry.get("trade.engine.merchant.profile.cache.hit.total")
                        .tag("environment", "test")
                        .tag("result", "miss")
                        .tag("lookup_type", "app_org")
                        .counter()
                        .count())
                .isEqualTo(1.0);
        assertThat(registry.get("trade.engine.merchant.profile.cache.hit.total")
                        .tag("environment", "test")
                        .tag("result", "hit")
                        .tag("lookup_type", "app_org")
                        .counter()
                        .count())
                .isEqualTo(1.0);
        assertThat(registry.get("trade.engine.merchant.profile.cache.hit.total")
                        .tag("environment", "test")
                        .tag("result", "negative")
                        .tag("lookup_type", "org")
                        .counter()
                        .count())
                .isEqualTo(1.0);
        List<String> merchantCacheTagKeys = registry.getMeters().stream()
                .map(Meter::getId)
                .filter(id -> "trade.engine.merchant.profile.cache.hit.total".equals(id.getName()))
                .flatMap(id -> id.getTags().stream())
                .map(tag -> tag.getKey())
                .distinct()
                .toList();
        assertThat(merchantCacheTagKeys).containsOnly("environment", "result", "lookup_type");
    }

    @Test
    void recordsCreateOrderStageDurationsWithOnlyStageAndEnvironmentTags() {
        var registry = new SimpleMeterRegistry();
        var metrics = new TradeEngineObservabilityMetrics(
                TradeEngineObservabilitySnapshotProvider.empty(),
                "test",
                Duration.ofMillis(30));
        metrics.bindTo(registry);

        metrics.recordStage(V1CreateOrderStageObserver.STAGE_PREPARE, Duration.ofMillis(12));
        metrics.recordStage(V1CreateOrderStageObserver.STAGE_MQ_CONFIRM, Duration.ofMillis(35));
        metrics.recordStage(V1CreateOrderStageObserver.STAGE_REDIS_SESSION_CONNECT, Duration.ofMillis(8));
        metrics.recordStage(V1CreateOrderStageObserver.STAGE_SYNC_TOTAL, Duration.ofMillis(90));

        assertThat(registry.get("trade.engine.v1.create.stage.duration")
                        .tag("environment", "test")
                        .tag("stage", "prepare")
                        .timer()
                        .count())
                .isEqualTo(1L);
        assertThat(registry.get("trade.engine.v1.create.stage.duration")
                        .tag("environment", "test")
                        .tag("stage", "mq_confirm")
                        .timer()
                        .totalTime(TimeUnit.MILLISECONDS))
                .isEqualTo(35.0);
        assertThat(registry.get("trade.engine.v1.create.stage.duration")
                        .tag("environment", "test")
                        .tag("stage", "redis_session_connect")
                        .timer()
                        .count())
                .isEqualTo(1L);
        assertThat(registry.get("trade.engine.v1.create.stage.duration")
                        .tag("environment", "test")
                        .tag("stage", "sync_total")
                        .timer()
                        .count())
                .isEqualTo(1L);

        List<String> stageTagKeys = registry.getMeters().stream()
                .map(Meter::getId)
                .filter(id -> "trade.engine.v1.create.stage.duration".equals(id.getName()))
                .flatMap(id -> id.getTags().stream())
                .map(tag -> tag.getKey())
                .distinct()
                .toList();
        assertThat(stageTagKeys).containsOnly("environment", "stage");
    }

    @Test
    void recordsPersistBatchMetricsWithLowCardinalityTags() {
        var registry = new SimpleMeterRegistry();
        var metrics = new TradeEngineObservabilityMetrics(
                TradeEngineObservabilitySnapshotProvider.empty(),
                "test",
                Duration.ofMillis(30));
        metrics.bindTo(registry);

        metrics.recordPersistBatchSize(7);
        metrics.recordPersistBatchMysqlDuration(Duration.ofMillis(12));
        metrics.recordPersistBatchMysqlPhaseDuration("mc_order", Duration.ofMillis(3));
        // 详细设计「8.3」phase 白名单：拆表后使用 recharge_dispatch_outbox / recharge_balance_delta_outbox。
        metrics.recordPersistBatchMysqlPhaseDuration("recharge_dispatch_outbox", Duration.ofMillis(2));
        metrics.recordPersistBatchMysqlPhaseDuration("recharge_balance_delta_outbox", Duration.ofMillis(1));
        // 旧混合表 phase 不得进入低基数白名单，应折叠为 unknown。
        metrics.recordPersistBatchMysqlPhaseDuration("recharge_outbox", Duration.ofMillis(4));
        metrics.recordPersistBatchOrders(7, "success");
        metrics.recordPersistBatchOrders(3, "failed");

        assertThat(registry.get("trade.engine.persist.batch.size")
                        .tag("environment", "test")
                        .summary()
                        .count())
                .isEqualTo(1L);
        assertThat(registry.get("trade.engine.persist.batch.size")
                        .tag("environment", "test")
                        .summary()
                        .totalAmount())
                .isEqualTo(7.0);
        assertThat(registry.get("trade.engine.persist.batch.mysql.duration")
                        .tag("environment", "test")
                        .timer()
                        .count())
                .isEqualTo(1L);
        assertThat(registry.get("trade.engine.persist.batch.mysql.phase.duration")
                        .tag("environment", "test")
                        .tag("phase", "mc_order")
                        .timer()
                        .totalTime(TimeUnit.MILLISECONDS))
                .isEqualTo(3.0);
        assertThat(registry.get("trade.engine.persist.batch.mysql.phase.duration")
                        .tag("environment", "test")
                        .tag("phase", "recharge_dispatch_outbox")
                        .timer()
                        .totalTime(TimeUnit.MILLISECONDS))
                .isEqualTo(2.0);
        assertThat(registry.get("trade.engine.persist.batch.mysql.phase.duration")
                        .tag("environment", "test")
                        .tag("phase", "recharge_balance_delta_outbox")
                        .timer()
                        .totalTime(TimeUnit.MILLISECONDS))
                .isEqualTo(1.0);
        assertThat(registry.get("trade.engine.persist.batch.mysql.phase.duration")
                        .tag("environment", "test")
                        .tag("phase", "unknown")
                        .timer()
                        .totalTime(TimeUnit.MILLISECONDS))
                .isEqualTo(4.0);
        assertThat(registry.find("trade.engine.persist.batch.mysql.phase.duration")
                        .tag("environment", "test")
                        .tag("phase", "recharge_outbox")
                        .timer())
                .isNull();
        assertThat(registry.get("trade.engine.persist.batch.orders.total")
                        .tag("environment", "test")
                        .tag("result", "success")
                        .counter()
                        .count())
                .isEqualTo(7.0);
        assertThat(registry.get("trade.engine.persist.batch.orders.total")
                        .tag("environment", "test")
                        .tag("result", "failed")
                        .counter()
                        .count())
                .isEqualTo(3.0);
    }

    @Test
    void logsAndReturnsEmptyGaugeValuesWhenSnapshotProviderFails(CapturedOutput output) {
        var registry = new SimpleMeterRegistry();
        var metrics = new TradeEngineObservabilityMetrics(
                () -> {
                    throw new IllegalStateException("provider exploded");
                },
                "test",
                Duration.ofMillis(30));
        metrics.bindTo(registry);

        double value = registry.get("trade.engine.redis.pending.total")
                .tag("environment", "test")
                .gauge()
                .value();

        assertThat(value).isZero();
        assertThat(output).contains("交易引擎业务监控 Gauge 快照读取失败");
    }

    private record StaticSnapshotProvider(TradeEngineObservabilitySnapshot snapshot)
            implements TradeEngineObservabilitySnapshotProvider {

        @Override
        public TradeEngineObservabilitySnapshot currentSnapshot() {
            return snapshot;
        }
    }
}
