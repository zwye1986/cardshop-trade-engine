package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.application.v1.V1ProductCacheSettings;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
 * V1 商品配置 Redis 比较修复与接单 Lua 的 Testcontainers 集成测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」：父商品可信快照存在时，
 * Java 在 accept Lua 前修复缺失或漂移的实际子商品配置；父快照已变化时旧请求不得反向覆盖。
 *
 * <p>该测试启动真实 Redis（{@code redis:7-alpine}），用真实 {@link ReactiveStringRedisTemplate} 驱动
 * {@link RedisV1ProductConfigPreheatAdapter} 内嵌的 compare-and-repair Lua 与
 * {@code accept_recharge_order.lua}，补足 {@link RedisV1ProductConfigPreheatAdapterTest} 用 Fake 替身
 * 无法证明的父快照校验、整体修复与并发证据。
 *
 * <p>默认不随 {@code mvn test} 运行，避免在没有 Docker 的环境破坏构建；需通过
 * {@code -DredisTestcontainers.enabled=true} 显式开启，并要求本机可用 Docker。该门控与
 * {@link RedisAcceptFaultInjectionIntegrationTest} 的 {@code redisFaultInjection.enabled} 约定一致。
 */
@Testcontainers
@EnabledIfSystemProperty(named = "redisTestcontainers.enabled", matches = "true")
class RedisV1ProductConfigPreheatIntegrationTest {

    private static final String KEY_PREFIX = "cardshop:trade";
    private static final int PENDING_ORG_BUCKET_COUNT = 128;
    private static final Duration REDIS_TIMEOUT = Duration.ofSeconds(3);
    private static final V1ProductCacheSettings CACHE_SETTINGS = new V1ProductCacheSettings(
            Duration.ofMinutes(25), Duration.ofMinutes(30), Duration.ofMinutes(60),
            Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofMinutes(1));
    private static final String ORG_CODE = "A39";

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate redisTemplate;
    private static RechargeRedisKeyBuilder keyBuilder;
    private static RedisV1ProductConfigPreheatAdapter preheatAdapter;
    private static RedisV1ProductSelectionCacheAdapter selectionCacheAdapter;
    private static RedisAcceptRechargeOrderAdapter acceptAdapter;

    @BeforeAll
    static void wireAdapters() {
        /*
         * Testcontainers JUnit 扩展在用户 @BeforeAll 之前启动 @Container 静态字段，因此此处可直接读取
         * redis.getHost()/getMappedPort()。连接工厂与 reactive 模板复用同一 Lettuce 客户端，
         * 保证补热 Lua 与接单 Lua 命中同一 Redis 实例。
         */
        RedisStandaloneConfiguration redisConfiguration =
                new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379));
        LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder()
                .commandTimeout(REDIS_TIMEOUT)
                .shutdownTimeout(Duration.ZERO)
                .build();
        connectionFactory = new LettuceConnectionFactory(redisConfiguration, clientConfiguration);
        connectionFactory.afterPropertiesSet();
        // ReactiveStringRedisTemplate 不是 InitializingBean，连接工厂已初始化即可直接使用。
        redisTemplate = new ReactiveStringRedisTemplate(connectionFactory);
        keyBuilder = new RechargeRedisKeyBuilder(KEY_PREFIX, PENDING_ORG_BUCKET_COUNT);
        preheatAdapter = new RedisV1ProductConfigPreheatAdapter(
                redisTemplate, keyBuilder, CACHE_SETTINGS.physicalTtl(), REDIS_TIMEOUT);
        selectionCacheAdapter = new RedisV1ProductSelectionCacheAdapter(
                redisTemplate, keyBuilder, CACHE_SETTINGS, REDIS_TIMEOUT);
        acceptAdapter = new RedisAcceptRechargeOrderAdapter(
                new SpringDataRechargeRedisScriptExecutor(redisTemplate),
                keyBuilder,
                new RechargeRedisScriptLoader(),
                properties());
    }

    @AfterAll
    static void closeConnectionFactory() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @BeforeEach
    void flushDatabase() {
        /*
         * 每个用例使用唯一 selectedProductId/outOrderSn，理论上不会互相污染；这里仍 FLUSHALL 保证
         * 并发用例的“key 缺失”前提在任何执行顺序下都成立。
         */
        redisTemplate
                .execute((ReactiveRedisCallback<String>) connection -> connection.serverCommands().flushAll())
                .next()
                .block(REDIS_TIMEOUT);
    }

    @Test
    void repairsMissingConfigKeyWithFourFieldsWhenParentSnapshotMatches() {
        String selectedProductId = "NEW-80100099";
        V1ProductSelection selection = passedSelection("80100099", selectedProductId, 1, 10000L, 7L);
        seedParentSnapshotThenDeleteChild(selection);

        preheatAdapter.refreshIfMissingOrStale(ORG_CODE, selection);

        Map<String, String> stored = readHash(keyBuilder.productConfigKey(ORG_CODE, selectedProductId));
        assertThat(stored).containsExactlyInAnyOrderEntriesOf(Map.of(
                "status", "ENABLED",
                "version", "7",
                "price", "10000",
                "product_type", "1"));
    }

    @Test
    void repairsExistingDriftedProductConfigFromMatchingParentSnapshot() {
        String selectedProductId = "OVW-80100088";
        String productConfigKey = keyBuilder.productConfigKey(ORG_CODE, selectedProductId);
        V1ProductSelection selection = passedSelection("80100088", selectedProductId, 1, 10000L, 7L);
        seedSnapshot(selection);
        Map<String, String> preseeded = new LinkedHashMap<>();
        preseeded.put("status", "DISABLED");
        preseeded.put("version", "999");
        preseeded.put("price", "88888");
        preseeded.put("product_type", "1");
        putHash(productConfigKey, preseeded);

        preheatAdapter.refreshIfMissingOrStale(ORG_CODE, selection);

        Map<String, String> stored = readHash(productConfigKey);
        assertThat(stored).containsExactlyInAnyOrderEntriesOf(Map.of(
                "status", "ENABLED", "version", "7", "price", "10000", "product_type", "1"));
    }

    @Test
    void skipsPreheatForFailedSelectionWithoutSnapshot() {
        /*
         * PRODUCT_NO_PERMISSION 没有从 MySQL 选出子商品快照（productSnapshotAvailable=false），
         * 补热端口必须跳过；此时接单 Lua 命中缺失商品配置会 fail closed 返回 CONFIG_CHANGED，
         * 这是高可用方案「16.1」第 1671 行的预期兜底，不在“有真实子商品快照”的范围内。
         */
        String productId = "NOPERM-80100100";
        String productConfigKey = keyBuilder.productConfigKey(ORG_CODE, productId);

        preheatAdapter.refreshIfMissingOrStale(
                ORG_CODE, V1ProductSelection.failed(productId, "PRODUCT_NO_PERMISSION", "商品无权限", 1L));

        assertThat(redisTemplate.hasKey(productConfigKey).block(REDIS_TIMEOUT)).isFalse();
    }

    @Test
    void preheatsResolvedChildSnapshotEvenWhenCandidateValidationFailed() {
        /*
         * NOT_RECHARGE_PRODUCT（product_type=0）候选校验失败，但已选出真实子商品快照
         * （productSnapshotAvailable=true）。补热必须照常写入实际子商品配置，让接单 Lua 走
         * ACCEPTED_FAILED 失败订单路径，而不是因为配置缺失退化成 CONFIG_CHANGED。
         */
        String selectedProductId = "RES-80100101";
        V1ProductSelection selection = new V1ProductSelection(
                "80100101", selectedProductId, "MINISO", "名创直充100", "codekey",
                0, 1L, 10000L, 1L, 1L,
                true, ValidationStatus.FAILED, "NOT_RECHARGE_PRODUCT", "该商品不是直充产品");
        seedParentSnapshotThenDeleteChild(selection);

        preheatAdapter.refreshIfMissingOrStale(ORG_CODE, selection);

        Map<String, String> stored = readHash(keyBuilder.productConfigKey(ORG_CODE, selectedProductId));
        assertThat(stored).containsEntry("status", "ENABLED");
        assertThat(stored).containsEntry("version", "1");
        assertThat(stored).containsEntry("product_type", "0");
    }

    @Test
    void concurrentRepairOnSameMissingKeyKeepsOneConsistentSnapshot() throws Exception {
        int threads = 16;
        String selectedProductId = "CONC-80100077";
        V1ProductSelection selection = passedSelection("80100077", selectedProductId, 1, 10000L, 7L);
        seedParentSnapshotThenDeleteChild(selection);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                    preheatAdapter.refreshIfMissingOrStale(ORG_CODE, selection);
                    return null;
                }));
            }
            ready.await(5, TimeUnit.SECONDS);
            start.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        Map<String, String> stored = readHash(keyBuilder.productConfigKey(ORG_CODE, selectedProductId));
        assertThat(stored).hasSize(4);
        assertThat(stored).containsOnlyKeys("status", "version", "price", "product_type");
        assertThat(stored).containsExactlyInAnyOrderEntriesOf(Map.of(
                "status", "ENABLED", "version", "7", "price", "10000", "product_type", "1"));

        V1ProductSelection latecomer = new V1ProductSelection(
                "80100077", selectedProductId, "MINISO", "late", "codekey",
                9, 99999L, 10000L, 1000L, 999L,
                true, ValidationStatus.PASSED, "", "");
        preheatAdapter.refreshIfMissingOrStale(ORG_CODE, latecomer);
        assertThat(readHash(keyBuilder.productConfigKey(ORG_CODE, selectedProductId))).isEqualTo(stored);
    }

    @Test
    void coldCachePreheatThenAcceptWritesFailedOrderForNotRechargeProduct() {
        /*
         * 端到端证明缺陷 #1 不成立：冷缓存 + NOT_RECHARGE_PRODUCT（有真实子商品快照）时，
         * 先补热再执行 accept_recharge_order.lua，必须落 ACCEPTED_FAILED 失败订单，而不是 CONFIG_CHANGED。
         * 该用例直接驱动真实 accept Lua，验证高可用方案「16.1」第 1668（先补热）、1680（候选失败仍写失败订单）行。
         */
        String selectedProductId = "ACC-80100102";
        long merchantConfigVersion = 7L;
        long productConfigVersion = 11L;
        long amount = 10000L;
        String orderId = "ORDER-NOTRECHARGE-1";
        V1ProductSelection selection = new V1ProductSelection(
                "80100102", selectedProductId, "MINISO", "名创直充100", "codekey",
                0, amount, 10000L, 1000L, productConfigVersion,
                true, ValidationStatus.FAILED, "NOT_RECHARGE_PRODUCT", "该商品不是直充产品");
        AcceptRechargeOrderCommand command = new AcceptRechargeOrderCommand(
                ORG_CODE, "app-1", "OUT-NOTRECHARGE-1", orderId,
                "80100102", selectedProductId, "18186222336", "http://merchant.example/callback",
                "nonce-notrecharge-123456789012345", 1, amount, "名创直充100", 10000L, 0, "codekey", "TF_KA",
                "hash-notrecharge", "v1", "SHA-256",
                ValidationStatus.FAILED, "NOT_RECHARGE_PRODUCT", "该商品不是直充产品",
                merchantConfigVersion, productConfigVersion,
                selection.cacheFingerprint(),
                true,
                1783224958000L, 1783225258000L);
        List<String> acceptKeys = keyBuilder.acceptKeys(command);

        /*
         * 商户配置 key 由 keyBuilder.acceptKeys 统一构造（索引 8），避免测试自行拼接造成 hash tag 漂移；
         * 预置 status=ENABLED、版本与 command.merchantConfigVersion 一致，让 Lua 的商户检查通过。
         */
        putHash(acceptKeys.get(8), Map.of(
                "status", "ENABLED",
                "version", String.valueOf(merchantConfigVersion)));

        seedParentSnapshotThenDeleteChild(selection);
        preheatAdapter.refreshIfMissingOrStale(ORG_CODE, selection);

        Map<String, String> productConfig = readHash(acceptKeys.get(9));
        assertThat(productConfig).containsEntry("version", String.valueOf(productConfigVersion));

        AcceptRechargeOrderResult result = acceptAdapter.accept(command).toCompletableFuture().join();

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.ACCEPTED_FAILED);
        assertThat(result.orderWritten()).isTrue();
        assertThat(result.failCode()).isEqualTo("NOT_RECHARGE_PRODUCT");
        assertThat(result.dispatchAllowed()).isFalse();
        assertThat(result.balanceReserved()).isFalse();

        Map<String, String> order = readHash(acceptKeys.get(2));
        assertThat(order).isNotEmpty();
        assertThat(order).containsEntry("validation_status", "FAILED");
        assertThat(order).containsEntry("fail_code", "NOT_RECHARGE_PRODUCT");
        assertThat(order).containsEntry("order_status", "VALIDATION_FAILED");
        assertThat(order).containsEntry("dispatch_allowed", "0");
        assertThat(order).containsEntry("balance_reserved", "0");
        assertThat(order).containsEntry("persist_status", "PENDING_MQ");
        assertThat(order).containsEntry("selected_product_id", selectedProductId);
        assertThat(order).containsEntry("product_type", "0");

        /*
         * 失败订单同样进入 pending ZSET（score=nextRetryTime），证明它落到可由补偿任务追平的恢复通道，
         * 与高可用方案「16.1」第 1680、1684-1689 行的“候选失败仍写 pending”语义一致。
         */
        Double pendingScore = redisTemplate.opsForZSet().score(acceptKeys.get(4), orderId).block(REDIS_TIMEOUT);
        assertThat(pendingScore).isNotNull();
    }

    @Test
    void parentSwitchAfterRepairRejectsOldRequestBeforeOrderWrite() {
        String productId = "80100103";
        V1ProductSelection oldSelection = passedSelection(productId, "OLD-80100103", 1, 10000L, 7L);
        V1ProductSelection newSelection = passedSelection(productId, "NEW-80100103", 1, 12000L, 8L);
        seedParentSnapshotThenDeleteChild(oldSelection);
        preheatAdapter.refreshIfMissingOrStale(ORG_CODE, oldSelection);

        AcceptRechargeOrderCommand oldCommand = acceptCommand(
                "OUT-PARENT-SWITCH-1",
                "ORDER-PARENT-SWITCH-1",
                oldSelection,
                ValidationStatus.PASSED,
                "",
                "");
        List<String> acceptKeys = keyBuilder.acceptKeys(oldCommand);
        seedMerchantConfig(acceptKeys, oldCommand.merchantConfigVersion());

        // compare-and-repair 已完成后父快照切换，旧请求仍持有旧 sibling 的命令快照。
        seedSnapshot(newSelection);

        AcceptRechargeOrderResult result = acceptAdapter.accept(oldCommand).toCompletableFuture().join();

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.CONFIG_CHANGED);
        assertThat(result.failCode()).isEqualTo("PRODUCT_SELECTION_STALE");
        assertThat(result.orderWritten()).isFalse();
        assertThat(readHash(acceptKeys.get(2))).isEmpty();
    }

    @Test
    void parentFingerprintChangeRejectsOldRequestWhenPriceVersionAndTypeStaySame() {
        String productId = "80100105";
        String selectedProductId = "SAME-80100105";
        V1ProductSelection oldSelection = new V1ProductSelection(
                productId, selectedProductId, "MINISO", "名创直充100", "codekey",
                1, 10000L, 10000L, 9000L, 7L,
                true, ValidationStatus.PASSED, "", "");
        V1ProductSelection newSelection = new V1ProductSelection(
                productId, selectedProductId, "MINISO", "名创直充100", "codekey",
                1, 10000L, 10000L, 10001L, 7L,
                true, ValidationStatus.FAILED, "PRICE_INVALID", "卡劵价格异常！");
        seedParentSnapshotThenDeleteChild(oldSelection);
        preheatAdapter.refreshIfMissingOrStale(ORG_CODE, oldSelection);

        AcceptRechargeOrderCommand oldCommand = acceptCommand(
                "OUT-PARENT-FINGERPRINT-1",
                "ORDER-PARENT-FINGERPRINT-1",
                oldSelection,
                ValidationStatus.PASSED,
                "",
                "");
        List<String> acceptKeys = keyBuilder.acceptKeys(oldCommand);
        seedMerchantConfig(acceptKeys, oldCommand.merchantConfigVersion());

        seedSnapshot(newSelection);

        AcceptRechargeOrderResult result = acceptAdapter.accept(oldCommand).toCompletableFuture().join();

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.CONFIG_CHANGED);
        assertThat(result.failCode()).isEqualTo("PRODUCT_SELECTION_STALE");
        assertThat(result.orderWritten()).isFalse();
    }

    @Test
    void expiredParentSnapshotIsRejectedInsideAcceptLua() {
        V1ProductSelection selection = passedSelection("80100106", "EXP-80100106", 1, 10000L, 7L);
        seedSnapshot(selection, Instant.parse("2026-07-05T03:45:00Z"));
        AcceptRechargeOrderCommand command = acceptCommand(
                "OUT-PARENT-EXPIRED-1",
                "ORDER-PARENT-EXPIRED-1",
                selection,
                ValidationStatus.PASSED,
                "",
                "");
        List<String> acceptKeys = keyBuilder.acceptKeys(command);
        seedMerchantConfig(acceptKeys, command.merchantConfigVersion());

        AcceptRechargeOrderResult result = acceptAdapter.accept(command).toCompletableFuture().join();

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.CONFIG_CHANGED);
        assertThat(result.failCode()).isEqualTo("PRODUCT_SELECTION_STALE");
        assertThat(result.orderWritten()).isFalse();
        assertThat(readHash(acceptKeys.get(2))).isEmpty();
    }

    @Test
    void mysqlConfirmedNoPermissionWritesFailedOrderWithoutParentOrChildCache() {
        V1ProductSelection denied = V1ProductSelection.failed(
                "80100104", "PRODUCT_NO_PERMISSION", "商品无权限", 1L);
        AcceptRechargeOrderCommand command = acceptCommand(
                "OUT-NO-PERMISSION-1",
                "ORDER-NO-PERMISSION-1",
                denied,
                ValidationStatus.FAILED,
                denied.failCode(),
                denied.failReason());
        List<String> acceptKeys = keyBuilder.acceptKeys(command);
        seedMerchantConfig(acceptKeys, command.merchantConfigVersion());

        AcceptRechargeOrderResult result = acceptAdapter.accept(command).toCompletableFuture().join();

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.ACCEPTED_FAILED);
        assertThat(result.failCode()).isEqualTo("PRODUCT_NO_PERMISSION");
        assertThat(result.orderWritten()).isTrue();
        assertThat(result.dispatchAllowed()).isFalse();
        assertThat(result.balanceReserved()).isFalse();
        assertThat(readHash(acceptKeys.get(2)))
                .containsEntry("validation_status", "FAILED")
                .containsEntry("fail_code", "PRODUCT_NO_PERMISSION")
                .containsEntry("balance_reserved", "0");
        assertThat(readHash(acceptKeys.get(9))).isEmpty();
        assertThat(readHash(acceptKeys.get(12))).isEmpty();
    }

    private static V1ProductSelection passedSelection(
            String productId, String selectedProductId, int productType, long salePriceCent, long version) {
        return new V1ProductSelection(
                productId, selectedProductId, "MINISO", "名创直充100", "codekey",
                productType, salePriceCent, 10000L, 1000L, version,
                true, ValidationStatus.PASSED, "", "");
    }

    private static TradeEngineProperties properties() {
        TradeEngineProperties properties = new TradeEngineProperties();
        properties.getRedis().setWriteEnabled(true);
        properties.getOrder().setPendingTtlDays(3);
        properties.getV1().setNonceReplayTtlSeconds(300);
        return properties;
    }

    private static AcceptRechargeOrderCommand acceptCommand(
            String outOrderSn,
            String orderId,
            V1ProductSelection selection,
            ValidationStatus validationStatus,
            String failCode,
            String failReason) {
        return new AcceptRechargeOrderCommand(
                ORG_CODE,
                "app-1",
                outOrderSn,
                orderId,
                selection.productId(),
                selection.selectedProductId(),
                "18186222336",
                "http://merchant.example/callback",
                "nonce-" + orderId + "-123456789012345",
                1,
                selection.salePriceCent(),
                selection.productName(),
                selection.faceValueCent(),
                selection.productType(),
                selection.couponType(),
                selection.supplierChannel(),
                "hash-" + orderId,
                "v1",
                "SHA-256",
                validationStatus,
                failCode,
                failReason,
                7L,
                selection.productConfigVersion(),
                selection.cacheFingerprint(),
                selection.productSnapshotAvailable(),
                1783224958000L,
                1783225258000L);
    }

    private void seedMerchantConfig(List<String> acceptKeys, long version) {
        putHash(acceptKeys.get(8), Map.of(
                "status", "ENABLED",
                "version", String.valueOf(version)));
    }

    private void seedParentSnapshotThenDeleteChild(V1ProductSelection selection) {
        seedSnapshot(selection);
        redisTemplate.delete(keyBuilder.productConfigKey(ORG_CODE, selection.selectedProductId()))
                .block(REDIS_TIMEOUT);
    }

    private void seedSnapshot(V1ProductSelection selection) {
        seedSnapshot(selection, Instant.parse("2026-07-15T03:00:00Z"));
    }

    private void seedSnapshot(V1ProductSelection selection, Instant loadedAt) {
        String token = selectionCacheAdapter
                .tryAcquireRefreshLock(ORG_CODE, selection.productId())
                .orElseThrow();
        boolean written = selectionCacheAdapter.writeSnapshotIfLockOwned(
                ORG_CODE,
                selection.productId(),
                token,
                selection,
                loadedAt);
        assertThat(written).isTrue();
    }

    private Map<String, String> readHash(String key) {
        return redisTemplate.opsForHash().entries(key)
                .collectMap(entry -> String.valueOf(entry.getKey()), entry -> String.valueOf(entry.getValue()))
                .block(REDIS_TIMEOUT);
    }

    private void putHash(String key, Map<String, String> fields) {
        redisTemplate.opsForHash().putAll(key, new LinkedHashMap<>(fields)).block(REDIS_TIMEOUT);
    }
}
