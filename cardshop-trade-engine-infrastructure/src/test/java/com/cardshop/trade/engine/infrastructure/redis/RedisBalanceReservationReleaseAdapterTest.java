package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.BalanceReservationReleaseCommand;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

/**
 * Redis 余额预扣释放适配器测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统高可用方案设计.md}「14.6 订单最终失败/作废退款协议」
 * 和「16.6 release.lua」。供应商最终失败或作废成功后，释放动作必须按 releaseEventId 幂等，
 * 避免 outbox 重试造成 Redis 可用余额重复回补。
 */
class RedisBalanceReservationReleaseAdapterTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC);

    @Test
    void executesReleaseLuaAndMapsReleasedResult() {
        var sessionFactory = new RecordingSessionFactory(List.of(
                Mono.just(List.of("code", "RELEASED", "order_id", "ORDER-1"))));
        var adapter = adapter(sessionFactory);

        var result = adapter.release(command());

        RecordingSession session = sessionFactory.sessions.getFirst();
        assertThat(result.released()).isTrue();
        assertThat(result.succeeded()).isTrue();
        assertThat(session.closed).isTrue();
        assertThat(session.executedScripts).extracting(RedisScript::getScriptAsString)
                .anySatisfy(script -> assertThat(script).contains("release_balance_reservation.lua"));
        assertThat(session.lastKeys)
                .containsExactly(
                        "cardshop:trade:recharge:{A36}:balance",
                        "cardshop:trade:recharge:{A36}:order:OUT-1",
                        "cardshop:trade:recharge:{A36}:reservation:ORDER-1",
                        "cardshop:trade:recharge:{A36}:order:event_index:ORDER-1",
                        "cardshop:trade:recharge:{A36}:order:event:ORDER-1:4",
                        "cardshop:trade:recharge:{A36}:release:dispatch-ORDER-1");
        assertThat(session.lastArgs)
                .containsExactly(
                        "A36",
                        "ORDER-1",
                        "OUT-1",
                        "5200",
                        "RELEASE",
                        "dispatch-ORDER-1",
                        "supplier failed",
                        "1783480800000");
    }

    @Test
    void mapsAlreadyReleasedAsIdempotentSuccess() {
        var sessionFactory = new RecordingSessionFactory(List.of(
                Mono.just(List.of("code", "ALREADY_RELEASED", "order_id", "ORDER-1"))));
        var adapter = adapter(sessionFactory);

        var result = adapter.release(command());

        assertThat(result.alreadyReleased()).isTrue();
        assertThat(result.succeeded()).isTrue();
    }

    @Test
    void mapsLuaFailureWithoutThrowing() {
        var sessionFactory = new RecordingSessionFactory(List.of(
                Mono.just(List.of(
                        "code", "RELEASE_FAILED",
                        "order_id", "ORDER-1",
                        "fail_code", "AMOUNT_MISMATCH",
                        "fail_reason", "金额不匹配"))));
        var adapter = adapter(sessionFactory);

        var result = adapter.release(command());

        assertThat(result.succeeded()).isFalse();
        assertThat(result.failCode()).isEqualTo("AMOUNT_MISMATCH");
        assertThat(result.failReason()).isEqualTo("金额不匹配");
    }

    @Test
    void returnsFailureAndClosesSessionWhenRedisThrows() {
        var sessionFactory = new RecordingSessionFactory(List.of(Mono.error(new IllegalStateException("redis down"))));
        var adapter = adapter(sessionFactory);

        var result = adapter.release(command());

        assertThat(result.succeeded()).isFalse();
        assertThat(result.failCode()).isEqualTo("RELEASE_LUA_ERROR");
        assertThat(result.failReason()).contains("Redis");
        assertThat(sessionFactory.sessions.getFirst().closed).isTrue();
    }

    @Test
    void returnsFailureAndClosesSessionWhenRedisCallTimesOut() {
        var sessionFactory = new RecordingSessionFactory(List.of(Mono.never()));
        var adapter = adapter(sessionFactory, Duration.ofMillis(10));

        var result = adapter.release(command());

        assertThat(result.succeeded()).isFalse();
        assertThat(result.failCode()).isEqualTo("RELEASE_LUA_ERROR");
        assertThat(result.failReason()).contains("Redis");
        assertThat(sessionFactory.sessions.getFirst().closed).isTrue();
    }

    private static RedisBalanceReservationReleaseAdapter adapter(RecordingSessionFactory sessionFactory) {
        return adapter(sessionFactory, Duration.ofSeconds(5));
    }

    private static RedisBalanceReservationReleaseAdapter adapter(
            RecordingSessionFactory sessionFactory,
            Duration redisOperationTimeout) {
        return new RedisBalanceReservationReleaseAdapter(
                sessionFactory,
                new RechargeRedisKeyBuilder("cardshop:trade", 128),
                new RechargeRedisScriptLoader(),
                CLOCK,
                redisOperationTimeout);
    }

    private static BalanceReservationReleaseCommand command() {
        return new BalanceReservationReleaseCommand(
                "A36",
                "ORDER-1",
                "OUT-1",
                5200L,
                "RELEASE",
                "dispatch-ORDER-1",
                "supplier failed");
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
        private List<String> lastKeys = List.of();
        private List<String> lastArgs = List.of();
        private boolean closed;

        private RecordingSession(List<Mono<Object>> scriptResults, AtomicInteger scriptIndex) {
            this.scriptResults = scriptResults;
            this.scriptIndex = scriptIndex;
        }

        @Override
        public Mono<Object> execute(RedisScript<?> script, List<String> keys, List<String> args) {
            executedScripts.add(script);
            lastKeys = keys;
            lastArgs = args;
            return scriptResults.get(scriptIndex.getAndIncrement());
        }

        @Override
        public Mono<Long> waitForReplication(int replicas, java.time.Duration timeout) {
            return Mono.just(0L);
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
