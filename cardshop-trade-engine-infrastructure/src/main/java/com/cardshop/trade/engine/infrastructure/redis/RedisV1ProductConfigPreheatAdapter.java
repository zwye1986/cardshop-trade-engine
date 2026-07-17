package com.cardshop.trade.engine.infrastructure.redis;

import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.CHILD_FIELD_ORDER;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.CHILD_FIELD_PRICE;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.CHILD_FIELD_STATUS;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.CHILD_FIELD_VERSION;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_PRODUCT_TYPE;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_PRODUCT_VERSION;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_SALE_PRICE;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_SELECTED_PRODUCT_ID;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.FIELD_SELECTION_FINGERPRINT;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.PARENT_MATCH_FIELD_ORDER;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.appendFields;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.positiveOrDefault;
import static com.cardshop.trade.engine.infrastructure.redis.V1ProductSelectionRedisContract.value;

import com.cardshop.trade.engine.application.v1.V1ProductCacheUnavailableException;
import com.cardshop.trade.engine.application.v1.V1ProductConfigPreheatPort;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * 基于 Redis 的 V1 实际子商品配置比较修复适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。每次接单前允许按
 * 父商品可信快照修复缺失或漂移的子商品配置，但 Lua 必须先校验父快照中的子商品、售价、版本和类型
 * 仍与当前请求一致，防止旧请求把刚刷新完成的新价格覆盖回旧值。
 */
public class RedisV1ProductConfigPreheatAdapter implements V1ProductConfigPreheatPort {

    private static final Logger log = LoggerFactory.getLogger(RedisV1ProductConfigPreheatAdapter.class);
    private static final Duration DEFAULT_REDIS_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration DEFAULT_PHYSICAL_TTL = Duration.ofMinutes(60);

    private final RedisProductConfigOperations operations;
    private final RechargeRedisKeyBuilder keyBuilder;
    private final Duration physicalTtl;

    public RedisV1ProductConfigPreheatAdapter(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            Duration redisTimeout) {
        this(redisTemplate, keyBuilder, DEFAULT_PHYSICAL_TTL, redisTimeout);
    }

    public RedisV1ProductConfigPreheatAdapter(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            Duration physicalTtl,
            Duration redisTimeout) {
        this(
                new ReactiveRedisProductConfigOperations(redisTemplate, positiveOrDefault(redisTimeout, DEFAULT_REDIS_TIMEOUT)),
                keyBuilder,
                positiveOrDefault(physicalTtl, DEFAULT_PHYSICAL_TTL));
    }

    RedisV1ProductConfigPreheatAdapter(
            RedisProductConfigOperations operations,
            RechargeRedisKeyBuilder keyBuilder,
            Duration physicalTtl) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
        this.keyBuilder = Objects.requireNonNull(keyBuilder, "keyBuilder must not be null");
        this.physicalTtl = positiveOrDefault(physicalTtl, DEFAULT_PHYSICAL_TTL);
    }

    @Override
    public void refreshIfMissingOrStale(String orgCode, V1ProductSelection selection) {
        if (selection == null || !selection.productSnapshotAvailable()) {
            return;
        }
        try {
            RepairOutcome outcome = operations.repairIfParentMatches(
                    keyBuilder.productSelectionKey(orgCode, selection.productId()),
                    keyBuilder.productConfigKey(orgCode, selection.selectedProductId()),
                    expectedParentFields(selection),
                    RedisV1ProductSelectionCacheAdapter.childFields(selection),
                    physicalTtl);
            if (outcome == RepairOutcome.REPAIRED) {
                log.info("V1商品配置Redis比较修复完成 orgCode={}, productId={}, selectedProductId={}, version={}",
                        orgCode,
                        selection.productId(),
                        selection.selectedProductId(),
                        selection.productConfigVersion());
            }
        } catch (RuntimeException ex) {
            throw new V1ProductCacheUnavailableException("V1实际子商品配置比较修复失败", ex);
        }
    }

    private static Map<String, String> expectedParentFields(V1ProductSelection selection) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(FIELD_SELECTED_PRODUCT_ID, value(selection.selectedProductId()));
        fields.put(FIELD_SALE_PRICE, value(selection.salePriceCent()));
        fields.put(FIELD_PRODUCT_VERSION, value(selection.productConfigVersion()));
        fields.put(FIELD_PRODUCT_TYPE, value(selection.productType()));
        fields.put(FIELD_SELECTION_FINGERPRINT, selection.cacheFingerprint());
        return fields;
    }

    enum RepairOutcome {
        PARENT_CHANGED,
        UNCHANGED,
        REPAIRED
    }

    interface RedisProductConfigOperations {

        RepairOutcome repairIfParentMatches(
                String parentKey,
                String childKey,
                Map<String, String> expectedParent,
                Map<String, String> desiredChild,
                Duration physicalTtl);
    }

    private static final class ReactiveRedisProductConfigOperations implements RedisProductConfigOperations {

        private static final RedisScript<Long> REPAIR_SCRIPT = new DefaultRedisScript<>("""
                local FIELD_SELECTED_PRODUCT_ID = '%s'
                local FIELD_SALE_PRICE = '%s'
                local FIELD_PRODUCT_VERSION = '%s'
                local FIELD_PRODUCT_TYPE = '%s'
                local FIELD_SELECTION_FINGERPRINT = '%s'
                local CHILD_FIELD_STATUS = '%s'
                local CHILD_FIELD_VERSION = '%s'
                local CHILD_FIELD_PRICE = '%s'
                if redis.call('HGET', KEYS[1], FIELD_SELECTED_PRODUCT_ID) ~= ARGV[1]
                    or redis.call('HGET', KEYS[1], FIELD_SALE_PRICE) ~= ARGV[2]
                    or redis.call('HGET', KEYS[1], FIELD_PRODUCT_VERSION) ~= ARGV[3]
                    or redis.call('HGET', KEYS[1], FIELD_PRODUCT_TYPE) ~= ARGV[4]
                    or redis.call('HGET', KEYS[1], FIELD_SELECTION_FINGERPRINT) ~= ARGV[5] then
                  return -1
                end
                local changed = redis.call('HGET', KEYS[2], CHILD_FIELD_STATUS) ~= ARGV[6]
                    or redis.call('HGET', KEYS[2], CHILD_FIELD_VERSION) ~= ARGV[7]
                    or redis.call('HGET', KEYS[2], CHILD_FIELD_PRICE) ~= ARGV[8]
                    or redis.call('HGET', KEYS[2], FIELD_PRODUCT_TYPE) ~= ARGV[9]
                if changed then
                  redis.call('HSET', KEYS[2],
                    CHILD_FIELD_STATUS, ARGV[6], CHILD_FIELD_VERSION, ARGV[7],
                    CHILD_FIELD_PRICE, ARGV[8], FIELD_PRODUCT_TYPE, ARGV[9])
                end
                redis.call('PEXPIRE', KEYS[2], ARGV[10])
                if changed then
                  return 1
                end
                return 0
                """.formatted(
                FIELD_SELECTED_PRODUCT_ID,
                FIELD_SALE_PRICE,
                FIELD_PRODUCT_VERSION,
                FIELD_PRODUCT_TYPE,
                FIELD_SELECTION_FINGERPRINT,
                CHILD_FIELD_STATUS,
                CHILD_FIELD_VERSION,
                CHILD_FIELD_PRICE), Long.class);

        private final ReactiveStringRedisTemplate redisTemplate;
        private final Duration redisTimeout;

        private ReactiveRedisProductConfigOperations(
                ReactiveStringRedisTemplate redisTemplate,
                Duration redisTimeout) {
            this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
            this.redisTimeout = redisTimeout;
        }

        @Override
        public RepairOutcome repairIfParentMatches(
                String parentKey,
                String childKey,
                Map<String, String> expectedParent,
                Map<String, String> desiredChild,
                Duration physicalTtl) {
            List<String> args = new ArrayList<>();
            appendFields(args, expectedParent, PARENT_MATCH_FIELD_ORDER);
            appendFields(args, desiredChild, CHILD_FIELD_ORDER);
            args.add(String.valueOf(physicalTtl.toMillis()));
            Long result = redisTemplate.execute(REPAIR_SCRIPT, List.of(parentKey, childKey), args)
                    .next()
                    .block(redisTimeout);
            if (Long.valueOf(1L).equals(result)) {
                return RepairOutcome.REPAIRED;
            }
            if (Long.valueOf(0L).equals(result)) {
                return RepairOutcome.UNCHANGED;
            }
            return RepairOutcome.PARENT_CHANGED;
        }
    }
}
