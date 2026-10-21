package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.cardshop.trade.engine.application.recharge.BalanceReservationReleaseCommand;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/** 仅创建随机本机Redis进程，验证真实Lua资金不变量；参见详细设计9.2成功预扣收尾最小修订。 */
@EnabledIfSystemProperty(named = "balance.localRedisTests", matches = "true")
class BalanceReservationLocalRedisContractTest {
    private static Process process;
    private static Path log;
    private static LettuceConnectionFactory factory;
    private static ReactiveStringRedisTemplate redis;
    private static final RechargeRedisKeyBuilder KEYS = new RechargeRedisKeyBuilder("funds:local-test", 128);

    @BeforeAll
    static void startRedis() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        log = Files.createTempFile("funds-local-redis-", ".log");
        process = new ProcessBuilder("redis-server", "--bind", "127.0.0.1", "--port", Integer.toString(port),
                "--save", "", "--appendonly", "no").redirectErrorStream(true).redirectOutput(log.toFile()).start();
        boolean ready = false;
        for (int i = 0; i < 100; i++) {
            try (var socket = new java.net.Socket("127.0.0.1", port)) { ready = true; break; }
            catch (java.io.IOException unavailable) { Thread.sleep(20); }
        }
        assertThat(ready).isTrue();
        factory = new LettuceConnectionFactory("127.0.0.1", port);
        factory.afterPropertiesSet(); factory.start();
        redis = new ReactiveStringRedisTemplate(factory);
    }

    @BeforeEach
    void resetPrivateRedis() {
        // 连接只来自上述随机端口的专用进程，绝不使用项目/共享环境配置。
        try (var connection = factory.getConnection()) { connection.serverCommands().flushDb(); }
        redis.opsForHash().putAll(KEYS.balanceKey("A35"), Map.of("available", "900", "frozen", "100")).block();
        redis.opsForHash().putAll(KEYS.orderKey("A35", "OUT1"), Map.of("order_id", "O1", "org_code", "A35",
                "out_order_sn", "OUT1", "balance_reserved", "1", "persist_status", "DB_PERSISTED",
                "order_version", "1", "persisted_version", "0")).block();
        redis.opsForHash().putAll(KEYS.reservationKey("A35", "O1"), Map.of("order_id", "O1", "out_order_sn", "OUT1",
                "org_code", "A35", "amount", "100", "status", "DB_PERSISTED", "funds_status", "RESERVED")).block();
    }

    @AfterAll
    static void stopRedis() throws Exception {
        if (factory != null) factory.destroy();
        if (process != null) {
            process.destroy();
            if (!process.waitFor(3, java.util.concurrent.TimeUnit.SECONDS)) process.destroyForcibly();
        }
        if (log != null) Files.deleteIfExists(log);
    }

    @Test
    void confirmationConsumesOnlyFrozenAndReplayCannotConsumeAnotherOrdersFrozen() {
        assertThat(execute("CONFIRM", "balance-confirm-O1", 100)).contains("CONFIRMED");
        assertBalance("900", "0");
        assertThat(field(KEYS.reservationKey("A35", "O1"), "funds_status")).isEqualTo("CONSUMED");
        assertThat(field(KEYS.orderKey("A35", "OUT1"), "balance_reserved")).isEqualTo("1");
        redis.opsForHash().put(KEYS.balanceKey("A35"), "frozen", "200").block();
        assertThat(execute("CONFIRM", "balance-confirm-O1", 100)).contains("ALREADY_CONFIRMED");
        assertBalance("900", "200");
    }

    @Test
    void consumedRefundCreditsOnceWithoutTouchingOtherOrdersFrozen() {
        execute("CONFIRM", "balance-confirm-O1", 100);
        redis.opsForHash().put(KEYS.balanceKey("A35"), "frozen", "200").block();
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("RELEASED");
        assertBalance("1000", "200");
        assertThat(field(KEYS.reservationKey("A35", "O1"), "funds_status")).isEqualTo("REFUNDED");
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("ALREADY_RELEASED");
        assertBalance("1000", "200");
    }

    @Test
    void reservedFailureStillReleasesAvailableAndFrozenExactlyOnce() {
        assertThat(execute("RELEASE", "dispatch-O1", 100)).contains("RELEASED");
        assertBalance("1000", "0");
        assertThat(execute("RELEASE", "dispatch-O1", 100)).contains("ALREADY_RELEASED");
        assertBalance("1000", "0");
    }

    @Test
    void legacyReservationWithoutFundsMarkerCannotConfirmOrRefund() {
        redis.opsForHash().remove(KEYS.reservationKey("A35", "O1"), "funds_status").block();
        assertThat(execute("CONFIRM", "balance-confirm-O1", 100)).contains("RESERVATION_FUNDS_STATUS_MISSING");
        assertBalance("900", "100");
        assertThat(execute("RELEASE", "dispatch-O1", 100)).contains("RESERVATION_FUNDS_STATUS_MISSING");
        assertBalance("900", "100");
    }

    @Test
    void insufficientFrozenAndWrongAmountCannotBeConfirmed() {
        redis.opsForHash().put(KEYS.balanceKey("A35"), "frozen", "0").block();
        assertThat(execute("CONFIRM", "balance-confirm-O1", 100)).contains("FROZEN_BALANCE_NOT_ENOUGH");
        assertBalance("900", "0");
        redis.opsForHash().put(KEYS.balanceKey("A35"), "frozen", "100").block();
        assertThat(execute("CONFIRM", "balance-confirm-O1", 101)).contains("AMOUNT_MISMATCH");
        assertBalance("900", "100");
    }

    @Test
    void inconsistentRefundedMarkerCannotCreditFundsAgain() {
        redis.opsForHash().put(KEYS.reservationKey("A35", "O1"), "funds_status", "REFUNDED").block();
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("RESERVATION_FUNDS_STATUS_INVALID");
        assertBalance("900", "100");
    }

    @Test
    void consumedRefundCannotInventBalanceAfterRedisBaselineIsLost() {
        execute("CONFIRM", "balance-confirm-O1", 100);
        redis.delete(KEYS.balanceKey("A35")).block();
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("BALANCE_BASELINE_MISSING");
        assertThat(redis.hasKey(KEYS.balanceKey("A35")).block()).isFalse();
    }

    @Test
    void mysqlPersistenceMovesReservationStatusButNeverRevertsReleasedState() {
        redis.expire(KEYS.reservationKey("A35", "O1"), java.time.Duration.ofHours(1)).block();
        var keys = List.of(KEYS.orderKey("A35", "OUT1"), KEYS.idempotentKey("A35", "OUT1"), KEYS.pendingKey("A35"),
                "funds:local-test:recharge:{A35}:events", "funds:local-test:recharge:{A35}:event:3",
                KEYS.pendingOrgIndexKey(KEYS.pendingOrgBucket("A35")), KEYS.reservationKey("A35", "O1"));
        redis.execute(new RechargeRedisScriptLoader().markDbPersistedScript(), keys, List.of("A35", "O1", "1", "1000", "3600")).next().block();
        assertThat(field(KEYS.reservationKey("A35", "O1"), "status")).isEqualTo("DB_PERSISTED");
        assertThat(redis.getExpire(KEYS.reservationKey("A35", "O1")).block()).isEqualTo(java.time.Duration.ZERO);
        assertThat(field(KEYS.reservationKey("A35", "O1"), "funds_status")).isEqualTo("RESERVED");
        redis.opsForHash().put(KEYS.reservationKey("A35", "O1"), "status", "RELEASED").block();
        redis.execute(new RechargeRedisScriptLoader().markDbPersistedScript(), keys, List.of("A35", "O1", "1", "1001", "3600")).next().block();
        assertThat(field(KEYS.reservationKey("A35", "O1"), "status")).isEqualTo("RELEASED");
    }

    @Test
    void confirmsPersistedReservationAfterBusinessSnapshotExpiresWithoutRecreatingSnapshot() {
        redis.delete(KEYS.orderKey("A35", "OUT1")).block();
        assertThat(execute("CONFIRM", "balance-confirm-O1", 100)).contains("CONFIRMED");
        assertBalance("900", "0");
        assertThat(redis.hasKey(KEYS.orderKey("A35", "OUT1")).block()).isFalse();
    }

    @Test
    void consumedRefundAfterSnapshotExpiresIsIdempotentAcrossDifferentEvents() {
        execute("CONFIRM", "balance-confirm-O1", 100);
        redis.delete(KEYS.orderKey("A35", "OUT1")).block();
        redis.opsForHash().put(KEYS.balanceKey("A35"), "frozen", "200").block();
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("RELEASED");
        assertThat(execute("RELEASE", "other-event-O1", 100)).contains("ALREADY_RELEASED");
        assertBalance("1000", "200");
        assertThat(redis.hasKey(KEYS.orderKey("A35", "OUT1")).block()).isFalse();
    }

    @Test
    void reservedFailureRefundWorksWithoutSnapshot() {
        redis.delete(KEYS.orderKey("A35", "OUT1")).block();
        assertThat(execute("RELEASE", "dispatch-O1", 100)).contains("RELEASED");
        assertBalance("1000", "0");
        assertThat(redis.hasKey(KEYS.orderKey("A35", "OUT1")).block()).isFalse();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"org_code,OTHER", "out_order_sn,OTHER", "order_id,OTHER"})
    void wrongReservationIdentityCannotRefund(String name, String value) {
        redis.opsForHash().put(KEYS.reservationKey("A35", "O1"), name, value).block();
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("RELEASE_FAILED");
        assertBalance("900", "100");
    }

    @Test
    void missingReservationAmountCannotBeGuessedFromRequest() {
        redis.opsForHash().remove(KEYS.reservationKey("A35", "O1"), "amount").block();
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("AMOUNT_MISMATCH");
        assertBalance("900", "100");
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"available,abc", "available,-1", "available,1.5", "frozen,-1", "frozen,abc", "available,9007199254740991"})
    void malformedOrOverflowingBalanceCannotPartiallyExecuteRefund(String name, String value) {
        redis.opsForHash().put(KEYS.balanceKey("A35"), name, value).block();
        var before = redis.opsForHash().entries(KEYS.balanceKey("A35")).collectMap(Map.Entry::getKey, Map.Entry::getValue).block();
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("RELEASE_FAILED");
        assertThat(redis.opsForHash().entries(KEYS.balanceKey("A35")).collectMap(Map.Entry::getKey, Map.Entry::getValue).block()).isEqualTo(before);
        assertThat(field(KEYS.reservationKey("A35", "O1"), "funds_status")).isEqualTo("RESERVED");
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"available", "frozen"})
    void missingBalanceFieldCannotCreatePartialBaseline(String name) {
        redis.opsForHash().remove(KEYS.balanceKey("A35"), name).block();
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("BALANCE_BASELINE_MISSING");
        assertThat(redis.opsForHash().hasKey(KEYS.balanceKey("A35"), name).block()).isFalse();
    }

    @Test
    void corruptDiagnosticKeyCannotLeaveMoneyChangedWhenLuaFails() {
        var command = new BalanceReservationReleaseCommand("A35", "O1", "OUT1", 100, "RELEASE", "invalid-O1", "test");
        redis.opsForValue().set(KEYS.releaseBalanceReservationKeys(command).get(4), "wrong-type").block();
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("RELEASE_FAILED");
        assertBalance("900", "100");
    }

    @Test
    void existingReleaseMarkerCannotBypassReservationIdentityChecks() {
        execute("RELEASE", "invalid-O1", 100);
        assertThat(execute("RELEASE", "invalid-O1", 101)).contains("AMOUNT_MISMATCH");
        assertBalance("1000", "0");
    }

    @Test
    void missingReservationNeverBecomesIdempotentSuccess() {
        redis.delete(KEYS.reservationKey("A35", "O1")).block();
        redis.opsForHash().put(KEYS.orderKey("A35", "OUT1"), "balance_reserved", "0").block();
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("RESERVATION_NOT_FOUND");
        assertBalance("900", "100");
    }

    @Test
    void confirmPreservesNewFundsRecordLifetimeEvenAfterSnapshotExpires() {
        redis.delete(KEYS.orderKey("A35", "OUT1")).block();
        redis.expire(KEYS.reservationKey("A35", "O1"), java.time.Duration.ofHours(1)).block();
        assertThat(execute("CONFIRM", "balance-confirm-O1", 100)).contains("CONFIRMED");
        assertThat(redis.getExpire(KEYS.reservationKey("A35", "O1")).block()).isEqualTo(java.time.Duration.ZERO);
    }

    @Test
    void fullyIdentifiedLegacyRefundWithAlreadyReleasedMarkerIsNeverCreditedAgain() {
        execute("RELEASE", "old-event-O1", 100);
        var oldCommand = new BalanceReservationReleaseCommand("A35", "O1", "OUT1", 100, "RELEASE", "old-event-O1", "test");
        redis.opsForValue().set(KEYS.releaseBalanceReservationKeys(oldCommand).get(5), "ALREADY_RELEASED").block();
        redis.delete(KEYS.orderKey("A35", "OUT1")).block();
        assertThat(execute("RELEASE", "balance-flush-release-O1", 100)).contains("ALREADY_RELEASED");
        assertBalance("1000", "0");
        assertThat(field(KEYS.reservationKey("A35", "O1"), "release_event_id")).isEqualTo("old-event-O1");
    }

    @Test
    void lostReplyAfterRealLuaCommitIsRecoveredByAdapterWithoutAnotherCredit() {
        redis.delete(KEYS.orderKey("A35", "OUT1")).block();
        var completed = new java.util.concurrent.atomic.AtomicBoolean();
        var command = new BalanceReservationReleaseCommand("A35", "O1", "OUT1", 100,
                "RELEASE", "balance-flush-release-O1", "lost reply", "RESERVED", false);
        var lostReply = redisAdapter(true, completed).release(command);
        assertThat(completed).isTrue();
        assertThat(lostReply.succeeded()).isFalse();
        assertThat(lostReply.failCode()).isEqualTo("RELEASE_LUA_ERROR");
        assertBalance("1000", "0");
        assertThat(redisAdapter(false, completed).release(command).alreadyReleased()).isTrue();
        assertBalance("1000", "0");
        assertThat(redis.hasKey(KEYS.orderKey("A35", "OUT1")).block()).isFalse();
    }

    private RedisBalanceReservationReleaseAdapter redisAdapter(boolean loseReply,
            java.util.concurrent.atomic.AtomicBoolean completed) {
        RechargeRedisSessionFactory sessions = () -> new RechargeRedisSession() {
            @Override @SuppressWarnings("unchecked")
            public reactor.core.publisher.Mono<Object> execute(org.springframework.data.redis.core.script.RedisScript<?> script,
                    List<String> keys, List<String> args) {
                // 先在独立Redis真实提交Lua，再丢弃回包；超时不是用“未执行”的Mock冒充。
                var executed = redis.execute((org.springframework.data.redis.core.script.RedisScript<Object>) script, keys, args)
                        .single().doOnNext(ignored -> completed.set(true));
                return loseReply ? executed.then(reactor.core.publisher.Mono.never()) : executed;
            }
            @Override public reactor.core.publisher.Mono<Long> waitForReplication(int replicas, java.time.Duration timeout) {
                throw new AssertionError("资金测试不执行WAIT");
            }
            @Override public void close() { }
        };
        return new RedisBalanceReservationReleaseAdapter(sessions, KEYS, new RechargeRedisScriptLoader(),
                java.time.Clock.systemUTC(), java.time.Duration.ofMillis(200));
    }

    @Test
    void largeExactIntegerAmountDoesNotBecomeScientificNotationInsideHincrby() {
        long amount = 1_000_000_000_000_000L;
        redis.opsForHash().put(KEYS.reservationKey("A35", "O1"), "amount", Long.toString(amount)).block();
        redis.opsForHash().put(KEYS.balanceKey("A35"), "frozen", Long.toString(amount)).block();
        assertThatCode(() -> assertThat(execute("RELEASE", "large-refund", amount)).contains("RELEASED"))
                .doesNotThrowAnyException();
        assertBalance("1000000000000900", "0");
    }

    @Test
    void consumedFlagWithoutConfirmationReceiptCannotAuthorizeRefund() {
        execute("CONFIRM", "balance-confirm-O1", 100);
        redis.opsForHash().remove(KEYS.reservationKey("A35", "O1"), "confirm_event_id").block();
        assertThat(execute("RELEASE", "invalid-O1", 100)).contains("CONFIRM_EVIDENCE_MISSING");
        assertBalance("900", "0");
    }

    @Test
    void missingRefundReceiptCannotBeGuessedFromRefundedStatus() {
        redis.opsForHash().putAll(KEYS.reservationKey("A35", "O1"), Map.of(
                "status", "RELEASED", "funds_status", "REFUNDED", "release_event_id", "lost-event", "release_type", "RELEASE" )).block();
        assertThat(execute("RELEASE", "balance-flush-release-O1", 100)).contains("RELEASE_EVIDENCE_MISSING");
        assertBalance("900", "100");
    }

    @Test
    void concurrentDifferentEventsRefundConsumedOrderWithoutSnapshotOnlyOnce() throws Exception {
        execute("CONFIRM", "balance-confirm-O1", 100);
        redis.delete(KEYS.orderKey("A35", "OUT1")).block();
        try (var pool = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var first = pool.submit(() -> execute("RELEASE", "first-event", 100));
            var second = pool.submit(() -> execute("RELEASE", "second-event", 100));
            var results = List.of(first.get(), second.get());
            assertThat(results.stream().filter(r -> r.contains("RELEASED")).count()).isEqualTo(1);
            assertThat(results.stream().filter(r -> r.contains("ALREADY_RELEASED")).count()).isEqualTo(1);
        }
        assertBalance("1000", "0");
    }

    @Test
    void newCancelledOrderCannotRefundAnUnconsumedReservation() {
        var command = new BalanceReservationReleaseCommand("A35", "O1", "OUT1", 100, "RELEASE", "invalid-O1", "test");
        List<?> result = redis.execute(new RechargeRedisScriptLoader().releaseBalanceReservationScript(), KEYS.releaseBalanceReservationKeys(command),
                List.of("A35", "O1", "OUT1", "100", "RELEASE", "invalid-O1", "test", "1000", "CONSUMED", "0")).next().block();
        assertThat(result.stream().map(String::valueOf).toList()).contains("RESERVATION_FUNDS_STATUS_INVALID");
        assertBalance("900", "100");
    }

    @Test
    void existingMysqlRefundCannotCauseAnotherRedisCreditAfterReceiptLoss() {
        var command = new BalanceReservationReleaseCommand("A35", "O1", "OUT1", 100, "RELEASE", "invalid-O1", "test");
        List<?> result = redis.execute(new RechargeRedisScriptLoader().releaseBalanceReservationScript(), KEYS.releaseBalanceReservationKeys(command),
                List.of("A35", "O1", "OUT1", "100", "RELEASE", "invalid-O1", "test", "1000", "RESERVED", "1")).next().block();
        assertThat(result.stream().map(String::valueOf).toList()).contains("RELEASE_EVIDENCE_MISSING");
        assertBalance("900", "100");
    }

    @Test
    void cacheTaskNeverRewritesExistingBalanceOrQueriesUnsafeInitializationSource() {
        var mapper = merchantMapper();
        var result = cache(mapper).repairDue(1, 0, 10, "worker");
        assertThat(result.skipped()).isEqualTo(1);
        assertBalance("900", "100");
        org.mockito.Mockito.verify(mapper, org.mockito.Mockito.never()).selectUnusedMerchantBalance("A35");
    }

    @Test
    void cacheTaskCannotInitializeWhilePendingOrDatabaseHistoryExists() {
        redis.delete(KEYS.balanceKey("A35")).block();
        redis.opsForZSet().add(KEYS.pendingKey("A35"), "O1", 1).block();
        var mapper = merchantMapper();
        assertThat(cache(mapper).repairDue(1, 0, 10, "worker").skipped()).isEqualTo(1);
        org.mockito.Mockito.verify(mapper, org.mockito.Mockito.never()).selectUnusedMerchantBalance("A35");
        redis.delete(KEYS.pendingKey("A35")).block();
        org.mockito.Mockito.when(mapper.selectUnusedMerchantBalance("A35")).thenReturn(null);
        assertThat(cache(mapper).repairDue(1, 0, 10, "worker").skipped()).isEqualTo(1);
        assertThat(redis.hasKey(KEYS.balanceKey("A35")).block()).isFalse();
    }

    @Test
    void cacheInitializationUsesFreshSourceAfterChecksRatherThanOldScanBalance() {
        redis.delete(KEYS.balanceKey("A35")).block();
        var mapper = merchantMapper();
        var fresh = merchant(700);
        org.mockito.Mockito.when(mapper.selectUnusedMerchantBalance("A35")).thenReturn(fresh);
        assertThat(cache(mapper).repairDue(1, 0, 10, "worker").succeeded()).isEqualTo(1);
        assertBalance("700", "0");
    }

    @Test
    void cacheInitializationAndUnlockCannotActAfterLockOwnerChanges() {
        redis.delete(KEYS.balanceKey("A35")).block();
        var mapper = merchantMapper();
        org.mockito.Mockito.when(mapper.selectUnusedMerchantBalance("A35")).thenAnswer(call -> {
            redis.opsForValue().set(KEYS.balanceRepairingLockKey("A35"), "new-owner").block();
            return merchant(700);
        });
        assertThat(cache(mapper).repairDue(1, 0, 10, "worker").skipped()).isEqualTo(1);
        assertThat(redis.hasKey(KEYS.balanceKey("A35")).block()).isFalse();
        assertThat(redis.opsForValue().get(KEYS.balanceRepairingLockKey("A35")).block()).isEqualTo("new-owner");
    }

    private com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper merchantMapper() {
        var mapper = org.mockito.Mockito.mock(com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper.class);
        org.mockito.Mockito.when(mapper.selectBalanceRowsForShard(1, 0, 10)).thenReturn(List.of(merchant(1000)));
        return mapper;
    }

    private static com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO merchant(long balance) {
        var row = new com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO();
        row.setId(56L); row.setOrgCode("A35"); row.setBalance(balance);
        return row;
    }

    private RedisRechargeCacheRepairAdapter cache(com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper mapper) {
        return new RedisRechargeCacheRepairAdapter(mapper, redis, KEYS, java.time.Duration.ofSeconds(30), java.time.Duration.ofSeconds(3));
    }

    private List<String> execute(String operation, String event, long amount) {
        var command = new BalanceReservationReleaseCommand("A35", "O1", "OUT1", amount, operation, event, "test");
        List<?> result = redis.execute(new RechargeRedisScriptLoader().releaseBalanceReservationScript(), KEYS.releaseBalanceReservationKeys(command),
                List.of("A35", "O1", "OUT1", Long.toString(amount), operation, event, "test", "1000")).next().block();
        return result.stream().map(String::valueOf).toList();
    }

    private String field(String key, String field) { return String.valueOf(redis.opsForHash().get(key, field).block()); }
    private void assertBalance(String available, String frozen) {
        assertThat(field(KEYS.balanceKey("A35"), "available")).isEqualTo(available);
        assertThat(field(KEYS.balanceKey("A35"), "frozen")).isEqualTo(frozen);
    }
}
