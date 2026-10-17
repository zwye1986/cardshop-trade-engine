package com.cardshop.trade.engine.infrastructure.redis;

import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.CHILD_FIELD_ORDER;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.CHILD_FIELD_PRICE;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.CHILD_FIELD_STATUS;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.CHILD_FIELD_VERSION;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_BASE_PRICE;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_CACHE_KIND;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_COUPON_TYPE;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_EXPIRE_AT;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_FACE_VALUE;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_FAIL_CODE;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_FAIL_REASON;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_LOADED_AT;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_PRODUCT_ID;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_PRODUCT_NAME;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_PRODUCT_SNAPSHOT_AVAILABLE;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_PRODUCT_STATUS;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_PRODUCT_TYPE;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_PRODUCT_VERSION;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_REFRESH_AFTER;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_SALE_PRICE;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_SELECTED_PRODUCT_ID;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_SELECTION_FINGERPRINT;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_SUPPLIER_CHANNEL;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_VALIDATION_STATUS;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.SNAPSHOT_FIELD_ORDER;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.STATUS_ENABLED;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.appendFields;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.positiveOrDefault;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.value;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.v1.V1ProductCacheSettings;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.application.v1.V1ProductSelectionCacheEntry;
import com.cardshop.trade.engine.application.v1.V1ProductSelectionCacheEntry.CacheKind;
import com.cardshop.trade.engine.application.v1.V1ProductSelectionCachePort;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * Redis 实现的 V1 父商品选品快照缓存端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。快照完成脚本先校验
 * 父商品刷新锁 token，再原子写父商品快照和实际子商品配置并释放锁；即使 MySQL 查询超过锁 TTL，
 * 过期刷新者也不能覆盖后续刷新者写入的新价格。
 */
public class RedisV1ProductSelectionCacheAdapter implements V1ProductSelectionCachePort {

    private static final Logger log = LoggerFactory.getLogger(RedisV1ProductSelectionCacheAdapter.class);
    private static final Duration DEFAULT_REDIS_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration WAIT_POLL_INTERVAL = Duration.ofMillis(20);
    private static final Duration MALFORMED_LOG_INTERVAL = Duration.ofSeconds(60);
    private static final String LOG_EVENT_MALFORMED_HASH = "v1-product-selection-cache.malformed-hash";
    private static final LogStormGuard MALFORMED_LOG_GUARD =
            LogStormGuard.atMostOncePer(MALFORMED_LOG_INTERVAL);

    private final CacheOperations operations;
    private final RechargeRedisKeyBuilder keyBuilder;
    private final V1ProductCacheSettings settings;

    public RedisV1ProductSelectionCacheAdapter(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            V1ProductCacheSettings settings,
            Duration redisTimeout) {
        this(
                new ReactiveCacheOperations(redisTemplate, positiveOrDefault(redisTimeout, DEFAULT_REDIS_TIMEOUT)),
                keyBuilder,
                settings);
    }

    RedisV1ProductSelectionCacheAdapter(
            CacheOperations operations,
            RechargeRedisKeyBuilder keyBuilder,
            V1ProductCacheSettings settings) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
        this.keyBuilder = Objects.requireNonNull(keyBuilder, "keyBuilder must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
    }

    @Override
    public Optional<V1ProductSelectionCacheEntry> find(String orgCode, String productId) {
        String key = keyBuilder.productSelectionKey(orgCode, productId);
        Map<String, String> fields = operations.readHash(key);
        if (fields == null || fields.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(toEntry(fields));
        } catch (RuntimeException ex) {
            logMalformedHash(orgCode, productId, ex);
            return Optional.empty();
        }
    }

    @Override
    public Optional<String> tryAcquireRefreshLock(String orgCode, String productId) {
        String token = UUID.randomUUID().toString();
        boolean acquired = operations.setIfAbsent(
                keyBuilder.productSelectionRefreshLockKey(orgCode, productId),
                token,
                settings.refreshLockTtl());
        return acquired ? Optional.of(token) : Optional.empty();
    }

    @Override
    public boolean renewRefreshLock(String orgCode, String productId, String lockToken) {
        return operations.renewIfOwned(
                keyBuilder.productSelectionRefreshLockKey(orgCode, productId),
                lockToken,
                settings.refreshLockTtl());
    }

    @Override
    public boolean writeSnapshotIfLockOwned(
            String orgCode,
            String productId,
            String lockToken,
            V1ProductSelection selection,
            Instant loadedAt) {
        Objects.requireNonNull(selection, "selection must not be null");
        Objects.requireNonNull(loadedAt, "loadedAt must not be null");
        return operations.completeSnapshot(
                keyBuilder.productSelectionKey(orgCode, productId),
                keyBuilder.productConfigKey(orgCode, selection.selectedProductId()),
                keyBuilder.productSelectionRefreshLockKey(orgCode, productId),
                lockToken,
                snapshotFields(selection, CacheKind.SNAPSHOT, loadedAt),
                childFields(selection),
                settings.physicalTtl());
    }

    @Override
    public boolean writeNegativeIfLockOwned(
            String orgCode,
            String productId,
            String lockToken,
            V1ProductSelection selection,
            Instant loadedAt) {
        Objects.requireNonNull(selection, "selection must not be null");
        Objects.requireNonNull(loadedAt, "loadedAt must not be null");
        return operations.completeNegative(
                keyBuilder.productSelectionKey(orgCode, productId),
                keyBuilder.productSelectionRefreshLockKey(orgCode, productId),
                lockToken,
                snapshotFields(selection, CacheKind.NEGATIVE, loadedAt),
                settings.negativeCacheTtl());
    }

    @Override
    public void releaseRefreshLock(String orgCode, String productId, String lockToken) {
        operations.releaseIfOwned(
                keyBuilder.productSelectionRefreshLockKey(orgCode, productId),
                lockToken);
    }

    @Override
    public Optional<V1ProductSelectionCacheEntry> awaitNewerEntry(
            String orgCode,
            String productId,
            Instant observedLoadedAt) {
        Instant baseline = observedLoadedAt == null ? Instant.EPOCH : observedLoadedAt;
        long deadlineNanos = System.nanoTime() + settings.refreshWaitTimeout().toNanos();
        do {
            Optional<V1ProductSelectionCacheEntry> current = find(orgCode, productId);
            if (current.filter(entry -> entry.loadedAt().isAfter(baseline)).isPresent()) {
                return current;
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException("V1 product cache refresh wait interrupted");
            }
            LockSupport.parkNanos(WAIT_POLL_INTERVAL.toNanos());
        } while (System.nanoTime() < deadlineNanos);
        return Optional.empty();
    }

    private Map<String, String> snapshotFields(
            V1ProductSelection selection,
            CacheKind cacheKind,
            Instant loadedAt) {
        Duration logicalTtl = cacheKind == CacheKind.NEGATIVE
                ? settings.negativeCacheTtl()
                : settings.maxStaleAge();
        Instant refreshAt = cacheKind == CacheKind.NEGATIVE
                ? loadedAt.plus(logicalTtl)
                : loadedAt.plus(settings.refreshAfter());
        Instant expireAt = loadedAt.plus(logicalTtl);
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("supplier_batch_id", selection.supplierBatchId());
        fields.put(FIELD_PRODUCT_ID, value(selection.productId()));
        fields.put(FIELD_SELECTED_PRODUCT_ID, value(selection.selectedProductId()));
        fields.put(FIELD_SUPPLIER_CHANNEL, value(selection.supplierChannel()));
        fields.put(FIELD_PRODUCT_NAME, value(selection.productName()));
        fields.put(FIELD_COUPON_TYPE, value(selection.couponType()));
        fields.put(FIELD_PRODUCT_TYPE, value(selection.productType()));
        fields.put(FIELD_PRODUCT_STATUS, STATUS_ENABLED);
        fields.put(FIELD_SALE_PRICE, value(selection.salePriceCent()));
        fields.put(FIELD_FACE_VALUE, value(selection.faceValueCent()));
        fields.put(FIELD_BASE_PRICE, value(selection.basePriceCent()));
        fields.put(FIELD_PRODUCT_VERSION, value(selection.productConfigVersion()));
        fields.put(FIELD_SELECTION_FINGERPRINT, selection.cacheFingerprint());
        fields.put(FIELD_PRODUCT_SNAPSHOT_AVAILABLE, String.valueOf(selection.productSnapshotAvailable()));
        fields.put(FIELD_VALIDATION_STATUS, selection.validationStatus().name());
        fields.put(FIELD_FAIL_CODE, value(selection.failCode()));
        fields.put(FIELD_FAIL_REASON, value(selection.failReason()));
        fields.put(FIELD_LOADED_AT, String.valueOf(loadedAt.toEpochMilli()));
        fields.put(FIELD_REFRESH_AFTER, String.valueOf(refreshAt.toEpochMilli()));
        fields.put(FIELD_EXPIRE_AT, String.valueOf(expireAt.toEpochMilli()));
        fields.put(FIELD_CACHE_KIND, cacheKind.name());
        return fields;
    }

    static Map<String, String> childFields(V1ProductSelection selection) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(CHILD_FIELD_STATUS, STATUS_ENABLED);
        fields.put(CHILD_FIELD_VERSION, value(selection.productConfigVersion()));
        fields.put(CHILD_FIELD_PRICE, value(selection.salePriceCent()));
        fields.put(FIELD_PRODUCT_TYPE, value(selection.productType()));
        return fields;
    }

    private static V1ProductSelectionCacheEntry toEntry(Map<String, String> fields) {
        CacheKind cacheKind = CacheKind.valueOf(required(fields, FIELD_CACHE_KIND));
        V1ProductSelection selection = new V1ProductSelection(
                required(fields, FIELD_PRODUCT_ID),
                required(fields, FIELD_SELECTED_PRODUCT_ID),
                required(fields, FIELD_SUPPLIER_CHANNEL),
                required(fields, FIELD_PRODUCT_NAME),
                required(fields, FIELD_COUPON_TYPE),
                integer(fields, FIELD_PRODUCT_TYPE),
                number(fields, FIELD_SALE_PRICE),
                number(fields, FIELD_FACE_VALUE),
                number(fields, FIELD_BASE_PRICE),
                number(fields, FIELD_PRODUCT_VERSION),
                Boolean.parseBoolean(required(fields, FIELD_PRODUCT_SNAPSHOT_AVAILABLE)),
                ValidationStatus.valueOf(required(fields, FIELD_VALIDATION_STATUS)),
                required(fields, FIELD_FAIL_CODE),
                required(fields, FIELD_FAIL_REASON)).withSupplierBatchId(fields.getOrDefault("supplier_batch_id", ""));
        String storedFingerprint = required(fields, FIELD_SELECTION_FINGERPRINT);
        if (!storedFingerprint.equals(selection.cacheFingerprint())) {
            throw new IllegalArgumentException("product selection cache fingerprint mismatch");
        }
        return new V1ProductSelectionCacheEntry(
                selection,
                cacheKind,
                instant(fields, FIELD_LOADED_AT),
                instant(fields, FIELD_REFRESH_AFTER),
                instant(fields, FIELD_EXPIRE_AT));
    }

    private static String required(Map<String, String> fields, String field) {
        String value = fields.get(field);
        if (value == null) {
            throw new IllegalArgumentException("missing product selection cache field: " + field);
        }
        return value;
    }

    private static long number(Map<String, String> fields, String field) {
        return Long.parseLong(required(fields, field));
    }

    private static int integer(Map<String, String> fields, String field) {
        return Integer.parseInt(required(fields, field));
    }

    private static Instant instant(Map<String, String> fields, String field) {
        return Instant.ofEpochMilli(number(fields, field));
    }

    private static void logMalformedHash(String orgCode, String productId, RuntimeException ex) {
        if (MALFORMED_LOG_GUARD.shouldLog(LOG_EVENT_MALFORMED_HASH).shouldLog()) {
            /*
             * 缓存解析位于下单热路径，脏 key 可能被并发重复命中，因此按 60 秒采样 warn 并保留异常栈；
             * 返回 miss 后仍由父商品刷新锁保护 MySQL，参见详细设计「12. 可观测性」。
             */
            log.warn("V1商品选品缓存字段损坏，按缓存缺失处理 orgCode={}, productId={}, sampleIntervalSeconds={}",
                    orgCode,
                    productId,
                    MALFORMED_LOG_INTERVAL.toSeconds(),
                    ex);
        }
    }

    interface CacheOperations {

        Map<String, String> readHash(String key);

        boolean setIfAbsent(String key, String token, Duration ttl);

        boolean renewIfOwned(String key, String token, Duration ttl);

        boolean completeSnapshot(
                String selectionKey,
                String childKey,
                String lockKey,
                String token,
                Map<String, String> snapshotFields,
                Map<String, String> childFields,
                Duration physicalTtl);

        boolean completeNegative(
                String selectionKey,
                String lockKey,
                String token,
                Map<String, String> snapshotFields,
                Duration ttl);

        void releaseIfOwned(String key, String token);
    }

    private static final class ReactiveCacheOperations implements CacheOperations {

        private static final String SNAPSHOT_FIELD_DECLARATIONS = """
                local FIELD_PRODUCT_ID = '%s'
                local FIELD_SELECTED_PRODUCT_ID = '%s'
                local FIELD_SUPPLIER_CHANNEL = '%s'
                local FIELD_PRODUCT_NAME = '%s'
                local FIELD_COUPON_TYPE = '%s'
                local FIELD_PRODUCT_TYPE = '%s'
                local FIELD_PRODUCT_STATUS = '%s'
                local FIELD_SALE_PRICE = '%s'
                local FIELD_FACE_VALUE = '%s'
                local FIELD_BASE_PRICE = '%s'
                local FIELD_PRODUCT_VERSION = '%s'
                local FIELD_SELECTION_FINGERPRINT = '%s'
                local FIELD_PRODUCT_SNAPSHOT_AVAILABLE = '%s'
                local FIELD_VALIDATION_STATUS = '%s'
                local FIELD_FAIL_CODE = '%s'
                local FIELD_FAIL_REASON = '%s'
                local FIELD_LOADED_AT = '%s'
                local FIELD_REFRESH_AFTER = '%s'
                local FIELD_EXPIRE_AT = '%s'
                local FIELD_CACHE_KIND = '%s'
                """.formatted(
                FIELD_PRODUCT_ID,
                FIELD_SELECTED_PRODUCT_ID,
                FIELD_SUPPLIER_CHANNEL,
                FIELD_PRODUCT_NAME,
                FIELD_COUPON_TYPE,
                FIELD_PRODUCT_TYPE,
                FIELD_PRODUCT_STATUS,
                FIELD_SALE_PRICE,
                FIELD_FACE_VALUE,
                FIELD_BASE_PRICE,
                FIELD_PRODUCT_VERSION,
                FIELD_SELECTION_FINGERPRINT,
                FIELD_PRODUCT_SNAPSHOT_AVAILABLE,
                FIELD_VALIDATION_STATUS,
                FIELD_FAIL_CODE,
                FIELD_FAIL_REASON,
                FIELD_LOADED_AT,
                FIELD_REFRESH_AFTER,
                FIELD_EXPIRE_AT,
                FIELD_CACHE_KIND);
        private static final String SNAPSHOT_HSET = """
                redis.call('HSET', KEYS[1],
                  FIELD_PRODUCT_ID, ARGV[2], FIELD_SELECTED_PRODUCT_ID, ARGV[3],
                  FIELD_SUPPLIER_CHANNEL, ARGV[4], FIELD_PRODUCT_NAME, ARGV[5],
                  FIELD_COUPON_TYPE, ARGV[6], FIELD_PRODUCT_TYPE, ARGV[7],
                  FIELD_PRODUCT_STATUS, ARGV[8], FIELD_SALE_PRICE, ARGV[9],
                  FIELD_FACE_VALUE, ARGV[10], FIELD_BASE_PRICE, ARGV[11],
                  FIELD_PRODUCT_VERSION, ARGV[12], FIELD_SELECTION_FINGERPRINT, ARGV[13],
                  FIELD_PRODUCT_SNAPSHOT_AVAILABLE, ARGV[14], FIELD_VALIDATION_STATUS, ARGV[15],
                  FIELD_FAIL_CODE, ARGV[16], FIELD_FAIL_REASON, ARGV[17], FIELD_LOADED_AT, ARGV[18],
                  FIELD_REFRESH_AFTER, ARGV[19], FIELD_EXPIRE_AT, ARGV[20],
                  FIELD_CACHE_KIND, ARGV[21], 'supplier_batch_id', ARGV[#ARGV])
                """;
        private static final String CHILD_FIELD_DECLARATIONS = """
                local CHILD_FIELD_STATUS = '%s'
                local CHILD_FIELD_VERSION = '%s'
                local CHILD_FIELD_PRICE = '%s'
                """.formatted(CHILD_FIELD_STATUS, CHILD_FIELD_VERSION, CHILD_FIELD_PRICE);
        private static final RedisScript<Long> COMPLETE_SNAPSHOT_SCRIPT = new DefaultRedisScript<>(
                SNAPSHOT_FIELD_DECLARATIONS + CHILD_FIELD_DECLARATIONS + """
                if redis.call('GET', KEYS[3]) ~= ARGV[1] then
                  return 0
                end
                """ + SNAPSHOT_HSET + """
                redis.call('HSET', KEYS[2],
                  CHILD_FIELD_STATUS, ARGV[22], CHILD_FIELD_VERSION, ARGV[23],
                  CHILD_FIELD_PRICE, ARGV[24], FIELD_PRODUCT_TYPE, ARGV[25])
                redis.call('PEXPIRE', KEYS[1], ARGV[26])
                redis.call('PEXPIRE', KEYS[2], ARGV[26])
                redis.call('DEL', KEYS[3])
                return 1
                """, Long.class);
        private static final RedisScript<Long> COMPLETE_NEGATIVE_SCRIPT = new DefaultRedisScript<>(
                SNAPSHOT_FIELD_DECLARATIONS + """
                if redis.call('GET', KEYS[2]) ~= ARGV[1] then
                  return 0
                end
                """ + SNAPSHOT_HSET + """
                redis.call('PEXPIRE', KEYS[1], ARGV[22])
                redis.call('DEL', KEYS[2])
                return 1
                """, Long.class);
        private static final RedisScript<Long> RELEASE_LOCK_SCRIPT = new DefaultRedisScript<>("""
                if redis.call('GET', KEYS[1]) == ARGV[1] then
                  return redis.call('DEL', KEYS[1])
                end
                return 0
                """, Long.class);
        private static final RedisScript<Long> RENEW_LOCK_SCRIPT = new DefaultRedisScript<>("""
                if redis.call('GET', KEYS[1]) == ARGV[1] then
                  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
                end
                return 0
                """, Long.class);

        private final ReactiveStringRedisTemplate redisTemplate;
        private final Duration timeout;

        private ReactiveCacheOperations(ReactiveStringRedisTemplate redisTemplate, Duration timeout) {
            this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
            this.timeout = timeout;
        }

        @Override
        public Map<String, String> readHash(String key) {
            Map<String, String> result = redisTemplate.opsForHash()
                    .entries(key)
                    .collectMap(
                            entry -> String.valueOf(entry.getKey()),
                            entry -> String.valueOf(entry.getValue()))
                    .block(timeout);
            return result == null ? Map.of() : result;
        }

        @Override
        public boolean setIfAbsent(String key, String token, Duration ttl) {
            return Boolean.TRUE.equals(redisTemplate.opsForValue().setIfAbsent(key, token, ttl).block(timeout));
        }

        @Override
        public boolean renewIfOwned(String key, String token, Duration ttl) {
            Long result = redisTemplate.execute(
                            RENEW_LOCK_SCRIPT,
                            List.of(key),
                            List.of(token, String.valueOf(ttl.toMillis())))
                    .next()
                    .block(timeout);
            return Long.valueOf(1L).equals(result);
        }

        @Override
        public boolean completeSnapshot(
                String selectionKey,
                String childKey,
                String lockKey,
                String token,
                Map<String, String> snapshotFields,
                Map<String, String> childFields,
                Duration physicalTtl) {
            List<String> args = new ArrayList<>();
            args.add(token);
            appendFields(args, snapshotFields, SNAPSHOT_FIELD_ORDER);
            appendFields(args, childFields, CHILD_FIELD_ORDER);
            args.add(String.valueOf(physicalTtl.toMillis()));
            args.add(value(snapshotFields.get("supplier_batch_id")));
            Long result = redisTemplate.execute(
                            COMPLETE_SNAPSHOT_SCRIPT,
                            List.of(selectionKey, childKey, lockKey),
                            args)
                    .next()
                    .block(timeout);
            return Long.valueOf(1L).equals(result);
        }

        @Override
        public boolean completeNegative(
                String selectionKey,
                String lockKey,
                String token,
                Map<String, String> snapshotFields,
                Duration ttl) {
            List<String> args = new ArrayList<>();
            args.add(token);
            appendFields(args, snapshotFields, SNAPSHOT_FIELD_ORDER);
            args.add(String.valueOf(ttl.toMillis()));
            // 批次追加末尾，不移位既有父快照/子商品/TTL位置合同。
            args.add(value(snapshotFields.get("supplier_batch_id")));
            Long result = redisTemplate.execute(
                            COMPLETE_NEGATIVE_SCRIPT,
                            List.of(selectionKey, lockKey),
                            args)
                    .next()
                    .block(timeout);
            return Long.valueOf(1L).equals(result);
        }

        @Override
        public void releaseIfOwned(String key, String token) {
            redisTemplate.execute(RELEASE_LOCK_SCRIPT, List.of(key), List.of(token))
                    .next()
                    .block(timeout);
        }

    }
}
