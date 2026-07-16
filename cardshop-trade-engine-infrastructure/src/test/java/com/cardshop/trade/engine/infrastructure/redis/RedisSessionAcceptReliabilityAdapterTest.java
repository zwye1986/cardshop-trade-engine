package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecord;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecordPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilitySnapshotProvider;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

/**
 * Redis session 版接单/可靠性适配器测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「4. Application 用例编排层」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「14.2 Redis WAIT 协议」。Redis WAIT 只确认同一客户端连接
 * 上此前写入的复制进度，因此生产适配器必须把 accept Lua 与 WAIT 放在同一个 session 中，而不是在另一条连接上补发 WAIT。
 */
class RedisSessionAcceptReliabilityAdapterTest {

    @Test
    void waitsOnTheSameSessionThatExecutedAcceptLua() {
        var sessionFactory = new RecordingSessionFactory(List.of(Mono.just(acceptedLuaResult())));
        var adapter = adapter(sessionFactory, properties(true, 2, 120));

        AcceptRechargeOrderResult acceptResult = adapter.accept(sampleCommand()).toCompletableFuture().join();
        var waitResult = adapter.waitForReplication(sampleCommand(), acceptResult).toCompletableFuture().join();

        assertThat(acceptResult.status()).isEqualTo(AcceptRechargeOrderStatus.ACCEPTED);
        assertThat(waitResult.confirmed()).isTrue();
        assertThat(sessionFactory.sessions).hasSize(1);
        RecordingSession session = sessionFactory.sessions.getFirst();
        assertThat(session.executedScripts).extracting(RedisScript::getScriptAsString)
                .anySatisfy(script -> assertThat(script).contains("accept_recharge_order.lua"));
        assertThat(session.waitCalls).isEqualTo(1);
        assertThat(session.waitReplicas).isEqualTo(2);
        assertThat(session.waitTimeout).isEqualTo(Duration.ofMillis(120));
        assertThat(session.closed).isTrue();
    }

    @Test
    void keepsSessionForCancelWhenWaitMissesAndCancelsUnconfirmedAccept() {
        var sessionFactory = new RecordingSessionFactory(List.of(
                Mono.just(acceptedLuaResult()),
                Mono.just(List.of("code", "CANCELLED", "order_id", "202607051337458961474292"))));
        var adapter = adapter(sessionFactory, properties(true, 2, 120));

        AcceptRechargeOrderResult acceptResult = adapter.accept(sampleCommand()).toCompletableFuture().join();
        sessionFactory.sessions.getFirst().waitResult = Mono.just(1L);
        var waitResult = adapter.waitForReplication(sampleCommand(), acceptResult).toCompletableFuture().join();
        var cancelResult = adapter
                .cancelUnconfirmedAccept(sampleCommand(), acceptResult, waitResult.reason())
                .toCompletableFuture()
                .join();

        RecordingSession session = sessionFactory.sessions.getFirst();
        assertThat(waitResult.confirmed()).isFalse();
        assertThat(cancelResult.rollbackConfirmed()).isTrue();
        assertThat(session.executedScripts).extracting(RedisScript::getScriptAsString)
                .anySatisfy(script -> assertThat(script).contains("cancel_unconfirmed_accept.lua"));
        assertThat(session.lastArgs)
                .containsExactly(
                        "A36",
                        "202607051337458961474292",
                        "OUT-1001",
                        "5200",
                        "1783224958000",
                        "WAIT replicas not reached: expected=2, actual=1");
        assertThat(session.closed).isTrue();
    }

    @Test
    void opensShortSessionForMarkPersistQueuedAfterMqConfirm() {
        var sessionFactory = new RecordingSessionFactory(List.of(
                Mono.just(List.of("code", "MARKED", "order_id", "202607051337458961474292"))));
        var adapter = adapter(sessionFactory, properties(true, 1, 100));

        var result = adapter
                .markPersistQueued(sampleCommand(), acceptedResult(), "persist-msg-1", 2)
                .toCompletableFuture()
                .join();

        RecordingSession session = sessionFactory.sessions.getFirst();
        assertThat(result.marked()).isTrue();
        assertThat(session.executedScripts).extracting(RedisScript::getScriptAsString)
                .anySatisfy(script -> assertThat(script).contains("mark_persist_queued.lua"));
        assertThat(session.lastArgs)
                .containsExactly("202607051337458961474292", "persist-msg-1", "2", "1783224958000");
        assertThat(session.closed).isTrue();
    }

    @Test
    void opensShortSessionForMarkDbPersistedAfterMysqlCommit() {
        var sessionFactory = new RecordingSessionFactory(List.of(
                Mono.just(List.of("code", "MARKED", "order_id", "202607051337458961474292"))));
        var adapter = adapter(sessionFactory, properties(true, 1, 100));

        var result = adapter
                .markDbPersisted(sampleCommand(), acceptedResult(), 7L, 1783224999000L)
                .toCompletableFuture()
                .join();

        RecordingSession session = sessionFactory.sessions.getFirst();
        assertThat(result.marked()).isTrue();
        assertThat(session.executedScripts).extracting(RedisScript::getScriptAsString)
                .anySatisfy(script -> assertThat(script).contains("mark_db_persisted.lua"));
        assertThat(session.lastArgs)
                .containsExactly("A36", "202607051337458961474292", "7", "1783224999000", "259200");
        assertThat(session.closed).isTrue();
    }

    @Test
    void unwrapsSingleNestedLettuceMultiResultForMarkDbPersisted() {
        var sessionFactory = new RecordingSessionFactory(List.of(
                Mono.just(List.of(List.of("code", "MARKED", "order_id", "202607051337458961474292")))));
        var adapter = adapter(sessionFactory, properties(true, 1, 100));

        var result = adapter
                .markDbPersisted(sampleCommand(), acceptedResult(), 7L, 1783224999000L)
                .toCompletableFuture()
                .join();

        assertThat(result.marked()).isTrue();
        assertThat(result.failCode()).isEmpty();
        assertThat(result.failReason()).isEmpty();
    }

    @Test
    void opensShortSessionForMarkPersistFailedAfterMysqlError() {
        var sessionFactory = new RecordingSessionFactory(List.of(
                Mono.just(List.of("code", "MARKED", "order_id", "202607051337458961474292"))));
        var adapter = adapter(sessionFactory, properties(true, 1, 100));

        var result = adapter
                .markPersistFailed(
                        sampleCommand(),
                        acceptedResult(),
                        "MYSQL_DEADLOCK",
                        "deadlock found",
                        3,
                        1783225555000L,
                        1783224999000L)
                .toCompletableFuture()
                .join();

        RecordingSession session = sessionFactory.sessions.getFirst();
        assertThat(result.marked()).isTrue();
        assertThat(session.executedScripts).extracting(RedisScript::getScriptAsString)
                .anySatisfy(script -> assertThat(script).contains("mark_persist_failed.lua"));
        assertThat(session.lastArgs)
                .containsExactly(
                        "A36",
                        "202607051337458961474292",
                        "1783225555000",
                        "1783224999000",
                        "MYSQL_DEADLOCK",
                        "deadlock found",
                        "3");
        assertThat(session.closed).isTrue();
    }

    @Test
    void returnsWriteDisabledWithoutOpeningRedisSessionWhenSwitchClosed() {
        var sessionFactory = new RecordingSessionFactory(List.of());
        var adapter = adapter(sessionFactory, properties(false, 1, 100));

        AcceptRechargeOrderResult result = adapter.accept(sampleCommand()).toCompletableFuture().join();

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.REDIS_WRITE_DISABLED);
        assertThat(result.orderWritten()).isFalse();
        assertThat(sessionFactory.sessions).isEmpty();
    }

    @Test
    void returnsWriteDisabledWithoutOpeningRedisSessionWhenHaGuardClosed() {
        var sessionFactory = new RecordingSessionFactory(List.of());
        var adapter = new RedisSessionAcceptReliabilityAdapter(
                sessionFactory,
                new RechargeRedisKeyBuilder("cardshop:trade", 128),
                new RechargeRedisScriptLoader(),
                properties(true, 1, 100),
                closedGuard());

        AcceptRechargeOrderResult result = adapter.accept(sampleCommand()).toCompletableFuture().join();

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.REDIS_WRITE_DISABLED);
        assertThat(result.orderWritten()).isFalse();
        assertThat(sessionFactory.sessions).isEmpty();
    }

    @Test
    void doesNotOpenSessionWhenAcceptLuaPreparationFailsBeforeRedisCall() {
        var sessionFactory = new RecordingSessionFactory(List.of());
        var adapter = new RedisSessionAcceptReliabilityAdapter(
                sessionFactory,
                new RechargeRedisKeyBuilder("cardshop:trade", 128),
                new FailingAcceptScriptLoader(),
                properties(true, 1, 100));

        assertThatThrownBy(() -> adapter.accept(sampleCommand()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("script unavailable");
        assertThat(sessionFactory.sessions).isEmpty();
    }

    @Test
    void recordsLuaTimeoutAnomalyAndClosesHaGuardWhenAcceptStateIsUnknown() {
        var sessionFactory = new RecordingSessionFactory(List.of(
                Mono.error(new TimeoutException("command timed out after 100ms"))));
        var haGuard = new RecordingHaGuard();
        var anomalyPort = new RecordingAnomalyPort();
        var adapter = adapterWithAnomaly(sessionFactory, haGuard, anomalyPort);

        AcceptRechargeOrderResult result = adapter.accept(sampleCommand()).toCompletableFuture().join();

        assertUnknownAcceptHandled(result, sessionFactory, haGuard, anomalyPort, AcceptAnomalyType.LUA_TIMEOUT);
        assertThat(result.failReason()).contains("Redis Lua 执行超时");
        assertThat(anomalyPort.records.getFirst().lastError()).contains("command timed out after 100ms");
    }

    @Test
    void recordsRedisBusyAnomalyAndClosesHaGuardWhenAcceptScriptReturnsBusy() {
        var sessionFactory = new RecordingSessionFactory(List.of(
                Mono.error(new IllegalStateException("BUSY Redis is busy running a script"))));
        var haGuard = new RecordingHaGuard();
        var anomalyPort = new RecordingAnomalyPort();
        var adapter = adapterWithAnomaly(sessionFactory, haGuard, anomalyPort);

        AcceptRechargeOrderResult result = adapter.accept(sampleCommand()).toCompletableFuture().join();

        assertUnknownAcceptHandled(result, sessionFactory, haGuard, anomalyPort, AcceptAnomalyType.REDIS_BUSY);
        assertThat(result.failReason()).contains("Redis BUSY");
        assertThat(anomalyPort.records.getFirst().rawContext())
                .containsEntry("exception_message", "BUSY Redis is busy running a script");
    }

    @Test
    void recordsGenericAcceptStateUnknownAnomalyForUnexpectedRedisError() {
        var sessionFactory = new RecordingSessionFactory(List.of(
                Mono.error(new IllegalStateException("connection reset after eval"))));
        var haGuard = new RecordingHaGuard();
        var anomalyPort = new RecordingAnomalyPort();
        var adapter = adapterWithAnomaly(sessionFactory, haGuard, anomalyPort);

        AcceptRechargeOrderResult result = adapter.accept(sampleCommand()).toCompletableFuture().join();

        assertUnknownAcceptHandled(result, sessionFactory, haGuard, anomalyPort, AcceptAnomalyType.ACCEPT_STATE_UNKNOWN);
        assertThat(result.failReason()).contains("Redis Lua 执行状态未知");
    }

    @Test
    void recordsWaitAndLuaMetricsWithoutOrderLabels() {
        var registry = new SimpleMeterRegistry();
        var metrics = new TradeEngineObservabilityMetrics(
                TradeEngineObservabilitySnapshotProvider.empty(),
                "test",
                Duration.ofNanos(1));
        metrics.bindTo(registry);
        var sessionFactory = new RecordingSessionFactory(List.of(Mono.just(acceptedLuaResult())));
        var adapter = new RedisSessionAcceptReliabilityAdapter(
                sessionFactory,
                new RechargeRedisKeyBuilder("cardshop:trade", 128),
                new RechargeRedisScriptLoader(),
                properties(true, 2, 120),
                RedisHaGuardPort.noop(),
                AcceptAnomalyRecordPort.noop(),
                Runnable::run,
                metrics);

        AcceptRechargeOrderResult acceptResult = adapter.accept(sampleCommand()).toCompletableFuture().join();
        sessionFactory.sessions.getFirst().waitResult = Mono.just(1L);
        adapter.waitForReplication(sampleCommand(), acceptResult).toCompletableFuture().join();

        assertThat(registry.get("trade.engine.redis.lua.duration")
                        .tag("environment", "test")
                        .tag("operation", "accept")
                        .tag("result", "success")
                        .timer()
                        .count())
                .isEqualTo(1L);
        assertThat(registry.get("trade.engine.redis.lua.slow.total")
                        .tag("environment", "test")
                        .tag("operation", "accept")
                        .counter()
                        .count())
                .isEqualTo(1.0);
        assertThat(registry.get("trade.engine.redis.wait.total")
                        .tag("environment", "test")
                        .tag("result", "missed")
                        .tag("fail_code", "REPLICAS_NOT_REACHED")
                        .counter()
                        .count())
                .isEqualTo(1.0);
        assertThat(registry.getMeters()
                        .stream()
                        .flatMap(meter -> meter.getId().getTags().stream())
                        .map(tag -> tag.getKey())
                        .toList())
                .doesNotContain("orderId", "outOrderSn", "orgCode");
    }

    private static RedisSessionAcceptReliabilityAdapter adapter(
            RecordingSessionFactory sessionFactory,
            TradeEngineProperties properties) {
        return new RedisSessionAcceptReliabilityAdapter(
                sessionFactory,
                new RechargeRedisKeyBuilder("cardshop:trade", 128),
                new RechargeRedisScriptLoader(),
                properties);
    }

    private static RedisSessionAcceptReliabilityAdapter adapterWithAnomaly(
            RecordingSessionFactory sessionFactory,
            RecordingHaGuard haGuard,
            RecordingAnomalyPort anomalyPort) {
        return new RedisSessionAcceptReliabilityAdapter(
                sessionFactory,
                new RechargeRedisKeyBuilder("cardshop:trade", 128),
                new RechargeRedisScriptLoader(),
                properties(true, 1, 100),
                haGuard,
                anomalyPort,
                Runnable::run);
    }

    private static void assertUnknownAcceptHandled(
            AcceptRechargeOrderResult result,
            RecordingSessionFactory sessionFactory,
            RecordingHaGuard haGuard,
            RecordingAnomalyPort anomalyPort,
            AcceptAnomalyType anomalyType) {
        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.REDIS_UNAVAILABLE);
        assertThat(result.orderWritten()).isFalse();
        assertThat(sessionFactory.sessions).hasSize(1);
        assertThat(sessionFactory.sessions.getFirst().closed).isTrue();
        assertThat(haGuard.closeReason).isEqualTo(anomalyType.name());
        assertThat(haGuard.closeContext)
                .containsEntry("org_code", "A36")
                .containsEntry("out_order_sn", "OUT-1001")
                .containsEntry("order_id", "202607051337458961474292")
                .containsEntry("request_hash", "hash-v1")
                .containsEntry("failure_phase", "ACCEPT_LUA")
                .containsEntry("accept_status", "UNKNOWN")
                .containsEntry("redis_write_closed", true);
        assertThat(haGuard.closeContext).doesNotContainKey("account");
        assertThat(anomalyPort.records).hasSize(1);
        AcceptAnomalyRecord record = anomalyPort.records.getFirst();
        assertThat(record.anomalyId()).isEqualTo("accept-" + anomalyType.name() + "-202607051337458961474292");
        assertThat(record.anomalyType()).isEqualTo(anomalyType);
        assertThat(record.orgCode()).isEqualTo("A36");
        assertThat(record.outOrderSn()).isEqualTo("OUT-1001");
        assertThat(record.orderId()).isEqualTo("202607051337458961474292");
        assertThat(record.requestHash()).isEqualTo("hash-v1");
        assertThat(record.rawContext())
                .containsEntry("anomaly_type", anomalyType.name())
                .containsEntry("order_written", null);
        assertThat(record.rawContext()).doesNotContainKey("account");
    }

    private static TradeEngineProperties properties(boolean writeEnabled, int waitReplicas, int waitTimeoutMs) {
        var properties = new TradeEngineProperties();
        properties.getRedis().setWriteEnabled(writeEnabled);
        properties.getRedis().setWaitReplicas(waitReplicas);
        properties.getRedis().setWaitTimeoutMs(waitTimeoutMs);
        return properties;
    }

    private static RedisHaGuardPort closedGuard() {
        return new RedisHaGuardPort() {
            @Override
            public boolean isWriteAllowed() {
                return false;
            }

            @Override
            public void closeWrite(String reason, java.util.Map<String, Object> context) {
            }
        };
    }

    private static List<String> acceptedLuaResult() {
        return List.of(
                "code", "ACCEPTED",
                "order_id", "202607051337458961474292",
                "validation_status", "PASSED",
                "fail_code", "",
                "fail_reason", "",
                "dispatch_allowed", "1",
                "balance_reserved", "1",
                "order_version", "1");
    }

    private static AcceptRechargeOrderResult acceptedResult() {
        return new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.ACCEPTED,
                "202607051337458961474292",
                "PASSED",
                "",
                "",
                true,
                true,
                1L);
    }

    private static AcceptRechargeOrderCommand sampleCommand() {
        return new AcceptRechargeOrderCommand(
                "A36",
                "app-1",
                "OUT-1001",
                "202607051337458961474292",
                "73301280",
                "73301289",
                "18186222336",
                "http://merchant.example/callback",
                "nonce-12345678901234567890123456",
                1,
                5200L,
                "话费50元",
                5000L,
                1,
                "codekey",
                "TF_KA",
                "hash-v1",
                "v1",
                "SHA-256",
                ValidationStatus.PASSED,
                "",
                "",
                7L,
                11L,
                "selection-fingerprint",
                true,
                1783224958000L,
                1783225258000L);
    }

    private static final class RecordingSessionFactory implements RechargeRedisSessionFactory {

        private final List<Mono<Object>> scriptResults;
        private final AtomicInteger scriptIndex = new AtomicInteger();
        private final List<RecordingSession> sessions = new ArrayList<>();

        private RecordingSessionFactory(List<Mono<Object>> scriptResults) {
            this.scriptResults = scriptResults;
        }

        @Override
        public RechargeRedisSession openSession() {
            var session = new RecordingSession(scriptResults, scriptIndex);
            sessions.add(session);
            return session;
        }
    }

    private static final class RecordingSession implements RechargeRedisSession {

        private final List<Mono<Object>> scriptResults;
        private final AtomicInteger scriptIndex;
        private final List<RedisScript<?>> executedScripts = new ArrayList<>();
        private Mono<Long> waitResult = Mono.just(2L);
        private int waitCalls;
        private int waitReplicas;
        private Duration waitTimeout;
        private List<String> lastArgs = List.of();
        private boolean closed;

        private RecordingSession(List<Mono<Object>> scriptResults, AtomicInteger scriptIndex) {
            this.scriptResults = scriptResults;
            this.scriptIndex = scriptIndex;
        }

        @Override
        public Mono<Object> execute(RedisScript<?> script, List<String> keys, List<String> args) {
            executedScripts.add(script);
            lastArgs = args;
            return scriptResults.get(scriptIndex.getAndIncrement());
        }

        @Override
        public Mono<Long> waitForReplication(int replicas, Duration timeout) {
            waitCalls++;
            waitReplicas = replicas;
            waitTimeout = timeout;
            return waitResult;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class FailingAcceptScriptLoader extends RechargeRedisScriptLoader {

        @Override
        public RedisScript<List> acceptRechargeOrderScript() {
            throw new IllegalStateException("script unavailable");
        }
    }

    private static final class RecordingHaGuard implements RedisHaGuardPort {

        private String closeReason;
        private Map<String, Object> closeContext = Map.of();

        @Override
        public boolean isWriteAllowed() {
            return true;
        }

        @Override
        public void closeWrite(String reason, Map<String, Object> context) {
            closeReason = reason;
            closeContext = context;
        }
    }

    private static final class RecordingAnomalyPort implements AcceptAnomalyRecordPort {

        private final List<AcceptAnomalyRecord> records = new ArrayList<>();

        @Override
        public void record(AcceptAnomalyRecord record) {
            records.add(record);
        }
    }
}
