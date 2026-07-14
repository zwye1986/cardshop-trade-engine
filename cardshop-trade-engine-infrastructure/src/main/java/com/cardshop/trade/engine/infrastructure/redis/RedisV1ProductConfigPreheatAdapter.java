package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.v1.V1ProductConfigPreheatPort;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 基于 Redis 的 V1 商品配置补热适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * 商品选择器已经从 MySQL 读取旧卡管授权行和实际子商品快照，本适配器只把该快照补写到 Redis
 * 商品动态配置 key；接单 Lua 仍负责最终版本、价格和类型校验。
 */
public class RedisV1ProductConfigPreheatAdapter implements V1ProductConfigPreheatPort {

    private static final Logger log = LoggerFactory.getLogger(RedisV1ProductConfigPreheatAdapter.class);
    private static final String PRODUCT_STATUS_ENABLED = "ENABLED";
    private static final String FIELD_STATUS = "status";
    private static final String FIELD_VERSION = "version";
    private static final String FIELD_PRICE = "price";
    private static final String FIELD_PRODUCT_TYPE = "product_type";
    private static final Duration DEFAULT_REDIS_TIMEOUT = Duration.ofSeconds(3);

    private final RedisProductConfigOperations operations;
    private final RechargeRedisKeyBuilder keyBuilder;

    public RedisV1ProductConfigPreheatAdapter(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            Duration redisTimeout) {
        this(
                new ReactiveRedisProductConfigOperations(redisTemplate, normalizeRedisTimeout(redisTimeout)),
                keyBuilder);
    }

    RedisV1ProductConfigPreheatAdapter(
            RedisProductConfigOperations operations,
            RechargeRedisKeyBuilder keyBuilder) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
        this.keyBuilder = Objects.requireNonNull(keyBuilder, "keyBuilder must not be null");
    }

    @Override
    public void preheatIfMissing(String orgCode, V1ProductSelection selection) {
        if (selection == null || !selection.productSnapshotAvailable()) {
            return;
        }

        String productConfigKey = keyBuilder.productConfigKey(orgCode, selection.selectedProductId());
        if (operations.putIfAbsent(productConfigKey, productConfigFields(selection))) {
            log.info("V1商品配置Redis补热完成 orgCode={}, productId={}, selectedProductId={}, version={}",
                    orgCode,
                    selection.productId(),
                    selection.selectedProductId(),
                    selection.productConfigVersion());
        }
    }

    private static Map<String, String> productConfigFields(V1ProductSelection selection) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(FIELD_STATUS, PRODUCT_STATUS_ENABLED);
        fields.put(FIELD_VERSION, String.valueOf(selection.productConfigVersion()));
        fields.put(FIELD_PRICE, String.valueOf(selection.salePriceCent()));
        fields.put(FIELD_PRODUCT_TYPE, String.valueOf(selection.productType()));
        return fields;
    }

    private static Duration normalizeRedisTimeout(Duration redisTimeout) {
        if (redisTimeout == null || redisTimeout.isZero() || redisTimeout.isNegative()) {
            return DEFAULT_REDIS_TIMEOUT;
        }
        return redisTimeout;
    }

    interface RedisProductConfigOperations {

        boolean putIfAbsent(String key, Map<String, String> fields);
    }

    private static final class ReactiveRedisProductConfigOperations implements RedisProductConfigOperations {

        private static final RedisScript<Long> PUT_IF_ABSENT_SCRIPT = new DefaultRedisScript<>("""
                if redis.call('EXISTS', KEYS[1]) == 1 then
                  return 0
                end
                redis.call('HSET', KEYS[1],
                  'status', ARGV[1],
                  'version', ARGV[2],
                  'price', ARGV[3],
                  'product_type', ARGV[4])
                return 1
                """, Long.class);

        private final ReactiveStringRedisTemplate redisTemplate;
        private final Duration redisTimeout;

        private ReactiveRedisProductConfigOperations(
                ReactiveStringRedisTemplate redisTemplate,
                Duration redisTimeout) {
            this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
            this.redisTimeout = redisTimeout;
        }

        @Override
        public boolean putIfAbsent(String key, Map<String, String> fields) {
            Long inserted = redisTemplate.execute(
                            PUT_IF_ABSENT_SCRIPT,
                            List.of(key),
                            List.of(
                                    fields.get(FIELD_STATUS),
                                    fields.get(FIELD_VERSION),
                                    fields.get(FIELD_PRICE),
                                    fields.get(FIELD_PRODUCT_TYPE)))
                    .next()
                    .block(redisTimeout);
            return Long.valueOf(1L).equals(inserted);
        }
    }
}
