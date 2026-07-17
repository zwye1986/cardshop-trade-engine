package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecord;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecordPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

/**
 * Redis 接单状态未知故障注入集成测试。
 *
 * <p>对应 {@code docs/集成测试方案.md}「Redis BUSY/Lua timeout」和
 * {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」。该测试显式启动本机
 * `redis-server`，默认不随普通 Maven 测试运行；需要本机安装 `redis-server` 和 `redis-cli`，并通过
 * `-DredisFaultInjection.enabled=true` 开启。
 */
@EnabledIfSystemProperty(named = "redisFaultInjection.enabled", matches = "true")
class RedisAcceptFaultInjectionIntegrationTest {

    private static final Path REDIS_SERVER = Path.of("/opt/homebrew/bin/redis-server");
    private static final Path REDIS_CLI = Path.of("/opt/homebrew/bin/redis-cli");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-11T08:00:00Z"), ZoneOffset.UTC);

    @Test
    void recordsLuaTimeoutAnomalyAndClosesHaGuardWhenRedisCommandTimesOut() throws Exception {
        try (RedisProcess redis = RedisProcess.start(10)) {
            try (FaultInjectionHarness harness = harness(redis.port(), Duration.ofMillis(250))) {
                Process sleeper = runRedisCli(redis.port(), "DEBUG", "SLEEP", "1");
                Thread.sleep(100L);

                AcceptRechargeOrderResult result = harness.adapter().accept(sampleCommand("TIMEOUT")).toCompletableFuture().join();

                assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.REDIS_UNAVAILABLE);
                assertThat(result.failReason()).contains("Redis Lua 执行超时");
                assertThat(harness.haGuard().isWriteAllowed()).isFalse();
                assertThat(harness.haGuard().lastCloseReason()).isEqualTo(AcceptAnomalyType.LUA_TIMEOUT.name());
                assertThat(harness.haGuard().lastCloseContext())
                        .containsEntry("failure_phase", "ACCEPT_LUA")
                        .containsEntry("accept_status", "UNKNOWN")
                        .containsEntry("redis_write_closed", true);
                assertThat(harness.anomalyPort().records()).hasSize(1);
                AcceptAnomalyRecord record = harness.anomalyPort().records().getFirst();
                assertThat(record.anomalyType()).isEqualTo(AcceptAnomalyType.LUA_TIMEOUT);
                assertThat(record.rawContext()).containsEntry("anomaly_type", AcceptAnomalyType.LUA_TIMEOUT.name());
                assertThat(record.rawContext()).doesNotContainKey("account");

                sleeper.waitFor(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void recordsRedisBusyAnomalyAndClosesHaGuardWhenRedisIsRunningLongLua() throws Exception {
        try (RedisProcess redis = RedisProcess.start(10)) {
            try (FaultInjectionHarness harness = harness(redis.port(), Duration.ofSeconds(1))) {
                Process busyScript = runRedisCli(redis.port(), "EVAL", "while true do end", "0");
                waitUntilRedisReportsBusy(redis.port());

                AcceptRechargeOrderResult result = harness.adapter().accept(sampleCommand("BUSY")).toCompletableFuture().join();

                assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.REDIS_UNAVAILABLE);
                assertThat(result.failReason()).contains("Redis BUSY");
                assertThat(harness.haGuard().isWriteAllowed()).isFalse();
                assertThat(harness.haGuard().lastCloseReason()).isEqualTo(AcceptAnomalyType.REDIS_BUSY.name());
                assertThat(harness.anomalyPort().records()).hasSize(1);
                AcceptAnomalyRecord record = harness.anomalyPort().records().getFirst();
                assertThat(record.anomalyType()).isEqualTo(AcceptAnomalyType.REDIS_BUSY);
                assertThat(record.rawContext())
                        .containsEntry("failure_phase", "ACCEPT_LUA")
                        .containsEntry("anomaly_type", AcceptAnomalyType.REDIS_BUSY.name())
                        .containsEntry("redis_write_closed", true);
                runRedisCli(redis.port(), "SCRIPT", "KILL").waitFor(2, TimeUnit.SECONDS);
                busyScript.waitFor(2, TimeUnit.SECONDS);
                busyScript.destroyForcibly();
            }
        }
    }

    private static FaultInjectionHarness harness(int port, Duration commandTimeout) {
        TradeEngineProperties properties = properties();
        InMemoryRedisHaGuard haGuard = new InMemoryRedisHaGuard(properties, CLOCK);
        RecordingAnomalyPort anomalyPort = new RecordingAnomalyPort();
        LettuceConnectionFactory connectionFactory = lettuceConnectionFactory(port, commandTimeout);
        RechargeRedisSession session = new LettuceRechargeRedisSessionFactory(connectionFactory).openSession();
        RedisSessionAcceptReliabilityAdapter adapter = new RedisSessionAcceptReliabilityAdapter(
                () -> session,
                new RechargeRedisKeyBuilder("cardshop:trade", 128),
                new RechargeRedisScriptLoader(),
                properties,
                haGuard,
                anomalyPort,
                Runnable::run);
        return new FaultInjectionHarness(adapter, haGuard, anomalyPort, connectionFactory, session);
    }

    private static LettuceConnectionFactory lettuceConnectionFactory(int port, Duration commandTimeout) {
        RedisStandaloneConfiguration redisConfiguration = new RedisStandaloneConfiguration("127.0.0.1", port);
        LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder()
                .commandTimeout(commandTimeout)
                .shutdownTimeout(Duration.ZERO)
                .build();
        LettuceConnectionFactory connectionFactory =
                new LettuceConnectionFactory(redisConfiguration, clientConfiguration);
        connectionFactory.afterPropertiesSet();
        return connectionFactory;
    }

    private static TradeEngineProperties properties() {
        TradeEngineProperties properties = new TradeEngineProperties();
        properties.getRedis().setWriteEnabled(true);
        properties.getRedis().setWaitReplicas(0);
        properties.getRedis().setWaitTimeoutMs(100);
        properties.getOrder().setPendingTtlDays(3);
        properties.getV1().setNonceReplayTtlSeconds(300);
        return properties;
    }

    private static AcceptRechargeOrderCommand sampleCommand(String suffix) {
        return new AcceptRechargeOrderCommand(
                "A36",
                "app-1",
                "REDIS-" + suffix,
                "20260711" + suffix.hashCode(),
                "73301280",
                "73301289",
                "18186222336",
                "http://merchant.example/callback",
                "nonce-" + suffix + "-123456789012345",
                1,
                5200L,
                "话费50元",
                5000L,
                1,
                "codekey",
                "TF_KA",
                "hash-" + suffix,
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

    private static void waitUntilRedisReportsBusy(int port) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < deadline) {
            Process process = runRedisCli(port, "PING");
            String output = readAll(process);
            process.waitFor(200, TimeUnit.MILLISECONDS);
            if (output.contains("BUSY")) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("Redis did not report BUSY within 3 seconds");
    }

    private static Process runRedisCli(int port, String... args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(REDIS_CLI.toString());
        command.add("-p");
        command.add(Integer.toString(port));
        command.addAll(List.of(args));
        return new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
    }

    private static String readAll(Process process) throws IOException {
        return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private record FaultInjectionHarness(
            RedisSessionAcceptReliabilityAdapter adapter,
            InMemoryRedisHaGuard haGuard,
            RecordingAnomalyPort anomalyPort,
            LettuceConnectionFactory connectionFactory,
            RechargeRedisSession session) implements AutoCloseable {

        @Override
        public void close() {
            try {
                session.close();
            } catch (Exception ignored) {
                // 故障注入测试的主断言已经完成，关闭会话失败不应遮蔽 Redis 行为证据。
            }
            try {
                connectionFactory.destroy();
            } catch (Exception ignored) {
                // 同上。
            }
        }
    }

    private static final class RecordingAnomalyPort implements AcceptAnomalyRecordPort {

        private final List<AcceptAnomalyRecord> records = new ArrayList<>();

        @Override
        public void record(AcceptAnomalyRecord record) {
            records.add(record);
        }

        private List<AcceptAnomalyRecord> records() {
            return records;
        }
    }

    private static final class RedisProcess implements AutoCloseable {

        private final int port;
        private final Path dir;
        private final Process process;

        private RedisProcess(int port, Path dir, Process process) {
            this.port = port;
            this.dir = dir;
            this.process = process;
        }

        private static RedisProcess start(int luaTimeLimitMillis) throws Exception {
            assertThat(REDIS_SERVER).exists();
            assertThat(REDIS_CLI).exists();
            int port = freePort();
            Path dir = Files.createTempDirectory("cardshop-redis-fault-");
            Process process = new ProcessBuilder(
                            REDIS_SERVER.toString(),
                            "--port", Integer.toString(port),
                            "--save", "",
                            "--appendonly", "no",
                            "--dir", dir.toString(),
                            "--enable-debug-command", "yes",
                            "--lua-time-limit", Integer.toString(luaTimeLimitMillis))
                    .redirectErrorStream(true)
                    .start();
            RedisProcess redis = new RedisProcess(port, dir, process);
            redis.waitUntilReady();
            return redis;
        }

        private int port() {
            return port;
        }

        private void waitUntilReady() throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            while (System.nanoTime() < deadline) {
                Process ping = runRedisCli(port, "PING");
                String output = readAll(ping);
                ping.waitFor(200, TimeUnit.MILLISECONDS);
                if (output.contains("PONG")) {
                    return;
                }
                if (!process.isAlive()) {
                    throw new AssertionError("redis-server exited early: " + readAll(process));
                }
                Thread.sleep(50L);
            }
            throw new AssertionError("redis-server did not become ready");
        }

        @Override
        public void close() throws Exception {
            runRedisCli(port, "SHUTDOWN", "NOSAVE").waitFor(1, TimeUnit.SECONDS);
            process.waitFor(1, TimeUnit.SECONDS);
            process.destroyForcibly();
            if (Files.exists(dir)) {
                try (var paths = Files.walk(dir)) {
                    paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                            // 测试临时目录清理失败不影响故障注入结论。
                        }
                    });
                }
            }
        }
    }
}
