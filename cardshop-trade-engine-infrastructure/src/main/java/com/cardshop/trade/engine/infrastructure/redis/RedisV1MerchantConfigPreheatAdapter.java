package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.v1.V1MerchantConfigPreheatPort;
import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 基于 Redis 的 V1 商户配置启动预热适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。下单 Lua 会校验
 * {@code merchant:config.status/version} 与 Java 命令中的商户版本一致；因此启动预热只写门禁字段，
 * 不写 appSecret、aesKey 等敏感资料。
 */
public class RedisV1MerchantConfigPreheatAdapter implements V1MerchantConfigPreheatPort {

    private static final Duration DEFAULT_REDIS_TIMEOUT = Duration.ofSeconds(3);

    private final MerchantConfigOperations operations;
    private final RechargeRedisKeyBuilder keyBuilder;

    public RedisV1MerchantConfigPreheatAdapter(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            Duration redisTimeout) {
        this(new ReactiveMerchantConfigOperations(
                redisTemplate,
                normalizeRedisTimeout(redisTimeout)),
                keyBuilder);
    }

    RedisV1MerchantConfigPreheatAdapter(
            MerchantConfigOperations operations,
            RechargeRedisKeyBuilder keyBuilder) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
        this.keyBuilder = Objects.requireNonNull(keyBuilder, "keyBuilder must not be null");
    }

    @Override
    public void preheat(V1MerchantProfile profile) {
        Objects.requireNonNull(profile, "profile must not be null");
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("status", profile.enabled() ? "ENABLED" : "DISABLED");
        fields.put("version", String.valueOf(profile.configVersion()));
        operations.write(keyBuilder.merchantConfigKey(profile.orgCode()), fields);
    }

    interface MerchantConfigOperations {

        void write(String key, Map<String, String> fields);
    }

    private static final class ReactiveMerchantConfigOperations implements MerchantConfigOperations {

        private final ReactiveStringRedisTemplate redisTemplate;
        private final Duration redisTimeout;

        private ReactiveMerchantConfigOperations(
                ReactiveStringRedisTemplate redisTemplate,
                Duration redisTimeout) {
            this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
            this.redisTimeout = redisTimeout;
        }

        @Override
        public void write(String key, Map<String, String> fields) {
            redisTemplate.opsForHash()
                    .putAll(key, fields)
                    .block(redisTimeout);
        }
    }

    private static Duration normalizeRedisTimeout(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            return DEFAULT_REDIS_TIMEOUT;
        }
        return timeout;
    }
}
