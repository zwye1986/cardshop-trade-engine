package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.v1.CachedV1ProductCandidateSelector;
import com.cardshop.trade.engine.application.v1.V1ProductCacheObserver;
import com.cardshop.trade.engine.application.v1.V1ProductCacheSettings;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisCallback;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * V1 父商品选品缓存真实 Redis 并发与 fencing 测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。该测试证明同一
 * {@code orgCode + parentProductId} 冷缓存高并发只产生一次 MySQL 回源，并证明锁 TTL 到期后的旧刷新者
 * 无法覆盖后续刷新者已经原子写入的新价格。
 */
@Testcontainers
@EnabledIfSystemProperty(named = "redisTestcontainers.enabled", matches = "true")
class RedisV1ProductSelectionCacheIntegrationTest {

    private static final Duration REDIS_TIMEOUT = Duration.ofSeconds(3);
    private static final Instant NOW = Instant.parse("2026-07-15T03:00:00Z");
    private static final V1ProductCacheSettings SETTINGS = new V1ProductCacheSettings(
            Duration.ofSeconds(2),
            Duration.ofSeconds(3),
            Duration.ofSeconds(10),
            Duration.ofMillis(200),
            Duration.ofSeconds(1),
            Duration.ofSeconds(1));

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate redisTemplate;
    private static RechargeRedisKeyBuilder keyBuilder;
    private static RedisV1ProductSelectionCacheAdapter cacheAdapter;

    @BeforeAll
    static void wireRedis() {
        RedisStandaloneConfiguration redisConfiguration =
                new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379));
        LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder()
                .commandTimeout(REDIS_TIMEOUT)
                .shutdownTimeout(Duration.ZERO)
                .build();
        connectionFactory = new LettuceConnectionFactory(redisConfiguration, clientConfiguration);
        connectionFactory.afterPropertiesSet();
        redisTemplate = new ReactiveStringRedisTemplate(connectionFactory);
        keyBuilder = new RechargeRedisKeyBuilder("cardshop:trade", 128);
        cacheAdapter = new RedisV1ProductSelectionCacheAdapter(
                redisTemplate, keyBuilder, SETTINGS, REDIS_TIMEOUT);
    }

    @AfterAll
    static void closeRedis() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @BeforeEach
    void flushRedis() {
        redisTemplate.execute((ReactiveRedisCallback<String>) connection -> connection.serverCommands().flushAll())
                .next()
                .block(REDIS_TIMEOUT);
    }

    @Test
    void concurrentColdCacheLoadsMysqlExactlyOnceWhenSourceExceedsInitialLockTtl() throws Exception {
        int callers = 16;
        AtomicInteger mysqlLoads = new AtomicInteger();
        V1ProductSelection refreshed = selection("80100023", 12000L, 1L);
        CachedV1ProductCandidateSelector selector = new CachedV1ProductCandidateSelector(
                cacheAdapter,
                (orgCode, productId, account) -> {
                    mysqlLoads.incrementAndGet();
                    // 350ms 明确超过 200ms 初始锁 TTL，只有 token 续租生效时才能保持单飞。
                    sleep(Duration.ofMillis(350));
                    return refreshed;
                },
                SETTINGS,
                Clock.fixed(NOW, ZoneOffset.UTC),
                V1ProductCacheObserver.noop());
        ExecutorService pool = Executors.newFixedThreadPool(callers);
        CountDownLatch ready = new CountDownLatch(callers);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<V1ProductSelection>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < callers; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    return selector.select("A39", "80100022", "18100000000");
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            for (Future<V1ProductSelection> future : futures) {
                assertThat(future.get(5, TimeUnit.SECONDS)).isEqualTo(refreshed);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(mysqlLoads).hasValue(1);
        assertThat(readHash(keyBuilder.productSelectionKey("A39", "80100022")))
                .containsEntry("sale_price", "12000")
                .containsEntry("selected_product_id", "80100023");
        assertThat(readHash(keyBuilder.productConfigKey("A39", "80100023")))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "status", "ENABLED", "version", "1", "price", "12000", "product_type", "1"));
    }

    @Test
    void expiredLockOwnerCannotOverwriteNewerRefresh() {
        String firstToken = cacheAdapter.tryAcquireRefreshLock("A39", "80100022").orElseThrow();
        sleep(Duration.ofMillis(250));
        String secondToken = cacheAdapter.tryAcquireRefreshLock("A39", "80100022").orElseThrow();
        V1ProductSelection newer = selection("80100024", 13000L, 2L);

        assertThat(cacheAdapter.writeSnapshotIfLockOwned(
                "A39", "80100022", secondToken, newer, NOW.plusSeconds(1))).isTrue();
        assertThat(cacheAdapter.writeSnapshotIfLockOwned(
                "A39", "80100022", firstToken, selection("80100023", 10000L, 1L), NOW)).isFalse();

        assertThat(readHash(keyBuilder.productSelectionKey("A39", "80100022")))
                .containsEntry("selected_product_id", "80100024")
                .containsEntry("sale_price", "13000");
        assertThat(readHash(keyBuilder.productConfigKey("A39", "80100024")))
                .containsEntry("price", "13000")
                .containsEntry("version", "2");
    }

    private static V1ProductSelection selection(String selectedProductId, long price, long version) {
        return V1ProductSelection.passed(
                "80100022", selectedProductId, "MINISO", "名创直充100", "codekey",
                1, price, 10000L, 9000L, version);
    }

    private static Map<String, String> readHash(String key) {
        return redisTemplate.opsForHash().entries(key)
                .collectMap(entry -> String.valueOf(entry.getKey()), entry -> String.valueOf(entry.getValue()))
                .block(REDIS_TIMEOUT);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("test sleep interrupted", ex);
        }
    }
}
