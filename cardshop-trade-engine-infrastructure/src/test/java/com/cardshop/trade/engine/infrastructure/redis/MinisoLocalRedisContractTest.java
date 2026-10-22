package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.BalanceReservationReleaseCommand;
import com.cardshop.trade.engine.application.recharge.BalanceReservationReleaseResult;
import com.cardshop.trade.engine.application.v1.V1ProductCacheSettings;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/** 显式启用的本地Redis合同测试，无外部中间件或凭证；参见详细设计名创实施约束。 */
@EnabledIfSystemProperty(named="miniso.localRedisTests",matches="true")
class MinisoLocalRedisContractTest {
    private static final String RELEASE_ORG = "A39";
    private static final String RELEASE_ORDER = "TASK17-ORDER";
    private static final String RELEASE_OUT_ORDER = "TASK17-OUT";
    private static final String FROZEN_RELEASE_EVENT = "task17-frozen-release-event";
    private static final Instant RELEASE_TIME = Instant.parse("2026-10-22T00:00:00Z");
    private static final long RESERVED_AMOUNT = 137L;

    @Test void batchSurvivesRealCacheLuaAcceptLuaAndRedisPersistenceSnapshot() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        var log = Files.createTempFile("miniso-local-redis-", ".log");
        var process = new ProcessBuilder("redis-server", "--bind", "127.0.0.1", "--port", Integer.toString(port),
                "--save", "", "--appendonly", "no").redirectErrorStream(true).redirectOutput(log.toFile()).start();
        LettuceConnectionFactory factory = null;
        try {
            boolean ready = false;
            for (int i=0;i<100;i++) {
                try (var socket = new java.net.Socket("127.0.0.1",port)) { ready=true; break; }
                catch (java.io.IOException ex) { Thread.sleep(20); }
            }
            assertThat(ready).isTrue();
            factory = new LettuceConnectionFactory("127.0.0.1",port); factory.afterPropertiesSet(); factory.start();
            var redis = new ReactiveStringRedisTemplate(factory);
            var keys = new RechargeRedisKeyBuilder("miniso:local-test",128);
            var settings = new V1ProductCacheSettings(Duration.ofMinutes(25),Duration.ofMinutes(30),Duration.ofHours(1),
                    Duration.ofSeconds(5),Duration.ofSeconds(1),Duration.ofSeconds(10));
            var cache = new RedisV1ProductSelectionCacheAdapter(redis,keys,settings,Duration.ofSeconds(3));
            var selection = V1ProductSelection.passed("P","SKU","MINISO","测试直充","codekey",1,
                    100L,100L,50L,1L).withSupplierBatchId("100023");
            var token = cache.tryAcquireRefreshLock("A39","P").orElseThrow();
            var now = Instant.now();
            assertThat(cache.writeSnapshotIfLockOwned("A39","P",token,selection,now)).isTrue();
            assertThat(cache.find("A39","P").orElseThrow().selection()).isEqualTo(selection);
            var command = new AcceptRechargeOrderCommand("A39","app","OUT1","ORDER1","P","SKU","13800138000",
                    "","abcdefghijklmnopqrstuvwx12345678",1,100L,"测试直充",100L,1,"codekey","MINISO","hash","v1","SHA-256",
                    ValidationStatus.PASSED,"","",1L,1L,selection.cacheFingerprint(),true,now.toEpochMilli(),now.plusSeconds(30).toEpochMilli())
                    .withSupplierBatchId("100023");
            redis.opsForHash().putAll(keys.merchantConfigKey("A39"),Map.of("status","ENABLED","version","1")).block();
            redis.opsForHash().putAll(keys.balanceKey("A39"),Map.of("available","1000","frozen","0")).block();
            var arguments = AcceptRechargeOrderLuaArguments.from(command,keys,Duration.ofDays(7),Duration.ofMinutes(10));
            var script = new DefaultRedisScript<java.util.List>();
            script.setLocation(new org.springframework.core.io.ClassPathResource("redis/lua/accept_recharge_order.lua"));
            script.setResultType(java.util.List.class);
            var result = redis.execute(script,arguments.keys(),arguments.args()).next().block();
            assertThat(result).contains("ACCEPTED");
            assertThat(redis.opsForHash().get(keys.reservationKey("A39", "ORDER1"), "funds_status").block()).isEqualTo("RESERVED");
            assertThat(redis.opsForHash().get(keys.reservationKey("A39", "ORDER1"), "org_code").block()).isEqualTo("A39");
            assertThat(redis.getExpire(keys.reservationKey("A39", "ORDER1")).block()).isEqualTo(Duration.ZERO);
            assertThat(redis.getExpire(keys.orderKey("A39", "OUT1")).block()).isPositive();
            assertThat(redis.opsForHash().get(keys.orderKey("A39","OUT1"),"supplier_batch_id").block()).isEqualTo("100023");
            var reader = new RedisPersistOrderSnapshotAdapter(redis,keys,Duration.ofSeconds(3));
            assertThat(reader.findLatestSnapshot("A39","OUT1","ORDER1").orElseThrow().supplierBatchId()).isEqualTo("100023");
            // 独立资金协议端到端：真实accept→落库标记→业务快照过期→消费确认→作废退款及重放。
            var loader = new RechargeRedisScriptLoader();
            redis.execute(loader.markDbPersistedScript(), keys.markDbPersistedKeys(command),
                    java.util.List.of("A39", "ORDER1", "1", Long.toString(now.toEpochMilli()), "28800")).next().block();
            redis.expire(keys.orderKey("A39", "OUT1"), Duration.ofMillis(1)).block();
            for (int i = 0; i < 100 && Boolean.TRUE.equals(redis.hasKey(keys.orderKey("A39", "OUT1")).block()); i++) Thread.sleep(2);
            assertThat(redis.hasKey(keys.orderKey("A39", "OUT1")).block()).isFalse();
            var confirm = new com.cardshop.trade.engine.application.recharge.BalanceReservationReleaseCommand(
                    "A39", "ORDER1", "OUT1", 100, "CONFIRM", "balance-confirm-ORDER1", "test");
            assertThat(redis.execute(loader.releaseBalanceReservationScript(), keys.releaseBalanceReservationKeys(confirm),
                    java.util.List.of("A39", "ORDER1", "OUT1", "100", "CONFIRM", "balance-confirm-ORDER1", "test", "1000")).next().block())
                    .contains("CONFIRMED");
            var release = new com.cardshop.trade.engine.application.recharge.BalanceReservationReleaseCommand(
                    "A39", "ORDER1", "OUT1", 100, "RELEASE", "balance-flush-release-ORDER1", "test");
            var releaseArgs = java.util.List.of("A39", "ORDER1", "OUT1", "100", "RELEASE", "balance-flush-release-ORDER1", "test", "1000", "CONSUMED", "0");
            assertThat(redis.execute(loader.releaseBalanceReservationScript(), keys.releaseBalanceReservationKeys(release), releaseArgs).next().block()).contains("RELEASED");
            assertThat(redis.execute(loader.releaseBalanceReservationScript(), keys.releaseBalanceReservationKeys(release), releaseArgs).next().block()).contains("ALREADY_RELEASED");
            assertThat(redis.opsForHash().get(keys.balanceKey("A39"), "available").block()).isEqualTo("1000");
            assertThat(redis.opsForHash().get(keys.balanceKey("A39"), "frozen").block()).isEqualTo("0");
            assertThat(redis.hasKey(keys.orderKey("A39", "OUT1")).block()).isFalse();
        } finally {
            if (factory!=null) factory.destroy();
            process.destroy();
            if (!process.waitFor(3,java.util.concurrent.TimeUnit.SECONDS)) process.destroyForcibly();
            Files.deleteIfExists(log);
        }
    }

    @Test
    void rejectsMismatchedOrderIdentityWithoutChangingReservationOrBalance() throws Exception {
        try (var fixture = new ReleaseFixture()) {
            var before = fixture.snapshot();
            var wrongOrder = new BalanceReservationReleaseCommand(RELEASE_ORG, "OTHER-ORDER", RELEASE_OUT_ORDER,
                    RESERVED_AMOUNT, "RELEASE", FROZEN_RELEASE_EVENT, "supplier failed");

            var result = fixture.adapter.release(wrongOrder);

            assertRejected(result, "RESERVATION_NOT_FOUND");
            fixture.assertFrozen(before);
            assertThat(fixture.redis.hasKey(fixture.keys.reservationKey(RELEASE_ORG, "OTHER-ORDER")).block()).isFalse();
            fixture.assertNoReleaseEvidence(wrongOrder);
        }
    }

    @Test
    void rejectsMismatchedReservationIdentityWithoutChangingReservationOrBalance() throws Exception {
        try (var fixture = new ReleaseFixture()) {
            fixture.redis.opsForHash().put(fixture.reservationKey, "order_id", "OTHER-ORDER").block();
            var before = fixture.snapshot();

            var result = fixture.adapter.release(fixture.command);

            assertRejected(result, "RESERVATION_ORDER_ID_MISMATCH");
            fixture.assertFrozen(before);
        }
    }

    @Test
    void rejectsAmount139ForReservation137WithoutChangingReservationOrBalance() throws Exception {
        try (var fixture = new ReleaseFixture()) {
            var before = fixture.snapshot();
            var wrongAmount = new BalanceReservationReleaseCommand(RELEASE_ORG, RELEASE_ORDER, RELEASE_OUT_ORDER,
                    139L, "RELEASE", FROZEN_RELEASE_EVENT, "supplier failed");

            var result = fixture.adapter.release(wrongAmount);

            assertRejected(result, "AMOUNT_MISMATCH");
            fixture.assertFrozen(before);
        }
    }

    @Test
    void realReleaseCredits137ExactlyOnceAcrossDuplicateAndChangedEventIds() throws Exception {
        try (var fixture = new ReleaseFixture()) {
            var before = fixture.snapshot();
            fixture.assertFrozen(before);

            assertThat(fixture.adapter.release(fixture.command)).isEqualTo(BalanceReservationReleaseResult.releasedSuccess());

            fixture.assertReleased(FROZEN_RELEASE_EVENT, "RELEASED");
            var marker = fixture.redis.opsForValue().get(fixture.releaseKeys.get(5)).block();
            assertThat(new com.fasterxml.jackson.databind.ObjectMapper().readTree(marker))
                    .isEqualTo(new com.fasterxml.jackson.databind.ObjectMapper().valueToTree(
                            List.of(RELEASE_ORG, RELEASE_ORDER, RELEASE_OUT_ORDER, "137", FROZEN_RELEASE_EVENT)));
            var after = fixture.snapshot();
            assertThat(fixture.adapter.release(fixture.command)).isEqualTo(BalanceReservationReleaseResult.alreadyReleasedSuccess());
            assertThat(fixture.snapshot()).isEqualTo(after);
            fixture.assertReleased(FROZEN_RELEASE_EVENT, "RELEASED");

            // 详细设计「6. Redis 受理与缓存层」：事件 ID 改变也必须由 reservation 状态挡住二次回补。
            var changedEvent = new BalanceReservationReleaseCommand(RELEASE_ORG, RELEASE_ORDER, RELEASE_OUT_ORDER,
                    RESERVED_AMOUNT, "RELEASE", "task17-replayed-different-event", "supplier failed");
            assertThat(fixture.adapter.release(changedEvent)).isEqualTo(BalanceReservationReleaseResult.alreadyReleasedSuccess());
            // dev独立资金协议保持首次凭证，不因后来事件ID补写退款标记；余额/快照/原许可全部不变。
            fixture.assertReleased(FROZEN_RELEASE_EVENT, "RELEASED");
            assertThat(fixture.snapshot()).isEqualTo(after);
            assertThat(fixture.redis.opsForValue().get(fixture.releaseKeys.get(5)).block()).isEqualTo(marker);
            assertThat(fixture.redis.opsForValue().get(fixture.keys.releaseBalanceReservationKeys(changedEvent).get(5)).block())
                    .isNull();
        }
    }

    private static void assertRejected(BalanceReservationReleaseResult result, String failCode) {
        assertThat(result.succeeded()).isFalse();
        assertThat(result.released()).isFalse();
        assertThat(result.alreadyReleased()).isFalse();
        assertThat(result.failCode()).isEqualTo(failCode);
    }

    /**
     * 每个负例独占回环 Redis，使用真实选品快照、受理 Lua 和生产会话释放资金；不以模拟结果证明资金合同。
     * 参见详细设计「6. Redis 受理与缓存层」「13. 测试与验收标准」。
     */
    private static final class ReleaseFixture implements AutoCloseable {
        private final Path log;
        private final Process process;
        private LettuceConnectionFactory factory;
        private ReactiveStringRedisTemplate redis;
        private final RechargeRedisKeyBuilder keys = new RechargeRedisKeyBuilder("miniso:task17", 128);
        private final BalanceReservationReleaseCommand command = new BalanceReservationReleaseCommand(
                RELEASE_ORG, RELEASE_ORDER, RELEASE_OUT_ORDER, RESERVED_AMOUNT, "RELEASE", FROZEN_RELEASE_EVENT, "supplier failed");
        private final List<String> releaseKeys = keys.releaseBalanceReservationKeys(command);
        private final String orderKey = keys.orderKey(RELEASE_ORG, RELEASE_OUT_ORDER);
        private final String reservationKey = keys.reservationKey(RELEASE_ORG, RELEASE_ORDER);
        private RedisBalanceReservationReleaseAdapter adapter;

        private ReleaseFixture() throws Exception {
            int port;
            try (var socket = new ServerSocket(0, 0, java.net.InetAddress.getLoopbackAddress())) {
                port = socket.getLocalPort();
            }
            log = Files.createTempFile("miniso-task17-redis-", ".log");
            process = new ProcessBuilder("redis-server", "--bind", "127.0.0.1", "--port", Integer.toString(port),
                    "--save", "", "--appendonly", "no").redirectErrorStream(true).redirectOutput(log.toFile()).start();
            try {
                boolean ready = false;
                for (int attempt = 0; attempt < 100; attempt++) {
                    try (var socket = new java.net.Socket("127.0.0.1", port)) {
                        ready = true;
                        break;
                    } catch (java.io.IOException ex) {
                        Thread.sleep(20);
                    }
                }
                assertThat(ready).as("ephemeral loopback Redis startup: %s", log).isTrue();
                factory = new LettuceConnectionFactory("127.0.0.1", port);
                factory.afterPropertiesSet();
                factory.start();
                redis = new ReactiveStringRedisTemplate(factory);
                var sessions = new LettuceRechargeRedisSessionFactory(factory);
                var loader = new RechargeRedisScriptLoader();
                adapter = new RedisBalanceReservationReleaseAdapter(sessions, keys, loader,
                        Clock.fixed(RELEASE_TIME.plusSeconds(1), ZoneOffset.UTC), Duration.ofSeconds(3));
                seedAcceptedReservation(sessions, loader);
            } catch (Exception | AssertionError ex) {
                close();
                throw ex;
            }
        }

        private void seedAcceptedReservation(RechargeRedisSessionFactory sessions, RechargeRedisScriptLoader loader) {
            var settings = new V1ProductCacheSettings(Duration.ofMinutes(25), Duration.ofMinutes(30), Duration.ofHours(1),
                    Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofSeconds(10));
            var cache = new RedisV1ProductSelectionCacheAdapter(redis, keys, settings, Duration.ofSeconds(3));
            var selection = V1ProductSelection.passed("P", "SKU", "MINISO", "测试直充", "codekey", 1,
                    RESERVED_AMOUNT, RESERVED_AMOUNT, 50L, 1L).withSupplierBatchId("100023");
            var token = cache.tryAcquireRefreshLock(RELEASE_ORG, "P").orElseThrow();
            assertThat(cache.writeSnapshotIfLockOwned(RELEASE_ORG, "P", token, selection, RELEASE_TIME)).isTrue();
            redis.opsForHash().putAll(keys.merchantConfigKey(RELEASE_ORG), Map.of("status", "ENABLED", "version", "1")).block();
            redis.opsForHash().putAll(keys.balanceKey(RELEASE_ORG), Map.of("available", "1000", "frozen", "0")).block();
            var accept = new AcceptRechargeOrderCommand(RELEASE_ORG, "app", RELEASE_OUT_ORDER, RELEASE_ORDER, "P", "SKU",
                    "13800138000", "", "task17nonceabcdefghijklmnop123456", 1, RESERVED_AMOUNT, "测试直充", RESERVED_AMOUNT, 1,
                    "codekey", "MINISO", "task17-hash", "v1", "SHA-256", ValidationStatus.PASSED, "", "", 1L, 1L,
                    selection.cacheFingerprint(), true, RELEASE_TIME.toEpochMilli(), RELEASE_TIME.plusSeconds(30).toEpochMilli())
                    .withSupplierBatchId("100023");
            var arguments = AcceptRechargeOrderLuaArguments.from(accept, keys, Duration.ofDays(7), Duration.ofMinutes(10));
            try (var session = sessions.openSession()) {
                assertThat(session.execute(loader.acceptRechargeOrderScript(), arguments.keys(), arguments.args())
                        .block(Duration.ofSeconds(3)).toString()).contains("ACCEPTED");
            }
            assertThat(hash(orderKey)).containsAllEntriesOf(Map.of("order_id", RELEASE_ORDER, "order_price", "137",
                    "balance_reserved", "1", "dispatch_allowed", "1", "order_status", "ACCEPTED", "supplier_batch_id", "100023"));
            // 详细设计独立资金协议：真实accept后还必须真实markDbPersisted，不能以PENDING_MQ冒充落库资格。
            assertThat(redis.execute(loader.markDbPersistedScript(), keys.markDbPersistedKeys(accept),
                    List.of(RELEASE_ORG, RELEASE_ORDER, "1", Long.toString(RELEASE_TIME.toEpochMilli()), "28800"))
                    .next().block()).contains("MARKED");
            assertThat(hash(reservationKey)).containsAllEntriesOf(Map.of("order_id", RELEASE_ORDER,
                    "out_order_sn", RELEASE_OUT_ORDER, "amount", "137", "status", "DB_PERSISTED", "funds_status", "RESERVED"));
            assertFrozen(snapshot());
        }

        private Map<Object, Object> hash(String key) {
            return redis.opsForHash().entries(key).collectMap(Map.Entry::getKey, Map.Entry::getValue).block();
        }

        private List<Map<Object, Object>> snapshot() {
            return List.of(hash(keys.balanceKey(RELEASE_ORG)), hash(orderKey), hash(reservationKey));
        }

        private void assertFrozen(List<Map<Object, Object>> before) {
            assertThat(hash(keys.balanceKey(RELEASE_ORG))).containsEntry("available", "863").containsEntry("frozen", "137");
            assertThat(snapshot()).isEqualTo(before);
            assertThat(hash(orderKey)).containsEntry("balance_reserved", "1").doesNotContainKey("release_event_id");
            assertThat(hash(reservationKey)).containsEntry("amount", "137").containsEntry("status", "DB_PERSISTED")
                    .doesNotContainKey("release_event_id");
            assertNoReleaseEvidence(command);
        }

        private void assertNoReleaseEvidence(BalanceReservationReleaseCommand release) {
            var evidenceKeys = keys.releaseBalanceReservationKeys(release);
            assertThat(redis.hasKey(evidenceKeys.get(5)).block()).isFalse();
            assertThat(redis.hasKey(evidenceKeys.get(4)).block()).isFalse();
            assertThat(redis.opsForZSet().score(evidenceKeys.get(3), "4").block()).isNull();
        }

        private void assertReleased(String releaseEventId, String releaseStatus) {
            assertThat(hash(keys.balanceKey(RELEASE_ORG))).containsEntry("available", "1000").containsEntry("frozen", "0");
            assertThat(hash(orderKey)).containsAllEntriesOf(Map.of("order_id", RELEASE_ORDER, "order_price", "137",
                    "balance_reserved", "0", "dispatch_allowed", "0", "order_status", "FAILED",
                    "release_type", "RELEASE", "release_event_id", releaseEventId, "release_reason", "supplier failed"));
            assertThat(hash(reservationKey)).containsAllEntriesOf(Map.of("order_id", RELEASE_ORDER,
                    "out_order_sn", RELEASE_OUT_ORDER, "amount", "137", "status", "RELEASED",
                    "release_type", "RELEASE", "release_event_id", releaseEventId, "release_reason", "supplier failed"));
            assertThat(hash(releaseKeys.get(4))).containsAllEntriesOf(Map.of("order_id", RELEASE_ORDER,
                    "version", "4", "release_event_id", releaseEventId, "release_type", "RELEASE", "release_status", releaseStatus,
                    "created_at", Long.toString(RELEASE_TIME.plusSeconds(1).toEpochMilli())));
            assertThat(redis.opsForZSet().score(releaseKeys.get(3), "4").block()).isEqualTo(4.0);
        }

        @Override
        public void close() throws Exception {
            if (factory != null) factory.destroy();
            process.destroy();
            if (!process.waitFor(3, java.util.concurrent.TimeUnit.SECONDS)) {
                process.destroyForcibly();
                assertThat(process.waitFor(3, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            }
            Files.deleteIfExists(log);
        }
    }
}
