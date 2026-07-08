package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.logging.LogStormGuard.LogDecision;
import com.cardshop.trade.engine.application.v1.V1TokenStorePort;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Optional;

/**
 * 基于 Redis 的 V1 Token 共享存储适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.1.1 `/v1/cardapi` 路由兼容设计」。
 * 新旧系统共享 {@code token:app_id:{appId}} key，保证灰度期间新服务签发的 token 能按旧规则校验。
 */
public class RedisV1TokenStoreAdapter implements V1TokenStorePort {

    private static final Logger log = LoggerFactory.getLogger(RedisV1TokenStoreAdapter.class);
    private static final Duration OPERATION_TIMEOUT = Duration.ofMillis(500);
    private static final String LOG_EVENT_STORE_FAILED = "v1-token-store.store.failed";
    private static final String LOG_EVENT_READ_FAILED = "v1-token-store.read.failed";

    private final RedisV1TokenOperations tokenOperations;
    private final TradeEngineProperties properties;
    private final LogStormGuard logStormGuard = LogStormGuard.defaultGuard();

    public RedisV1TokenStoreAdapter(ReactiveStringRedisTemplate redisTemplate, TradeEngineProperties properties) {
        this.tokenOperations = new ReactiveRedisV1TokenOperations(redisTemplate);
        this.properties = properties;
    }

    RedisV1TokenStoreAdapter(RedisV1TokenOperations tokenOperations, TradeEngineProperties properties) {
        this.tokenOperations = tokenOperations;
        this.properties = properties;
    }

    @Override
    public boolean storeIssuedToken(String appId, String token, Duration ttl) {
        if (!StringUtils.hasText(appId) || !StringUtils.hasText(token)) {
            return false;
        }
        try {
            return tokenOperations.set(key(appId), token, ttl);
        } catch (Exception ex) {
            LogDecision decision = logStormGuard.shouldLog(LOG_EVENT_STORE_FAILED);
            if (decision.shouldLog()) {
                log.error("store V1 token failed, appId={}, occurrence={}, sampled={}",
                        appId,
                        decision.occurrence(),
                        decision.sampledOccurrence(),
                        ex);
            }
            return false;
        }
    }

    @Override
    public Optional<String> findIssuedToken(String appId) {
        if (!StringUtils.hasText(appId)) {
            return Optional.empty();
        }
        try {
            return tokenOperations.get(key(appId));
        } catch (Exception ex) {
            LogDecision decision = logStormGuard.shouldLog(LOG_EVENT_READ_FAILED);
            if (decision.shouldLog()) {
                log.error("read V1 token failed, appId={}, occurrence={}, sampled={}",
                        appId,
                        decision.occurrence(),
                        decision.sampledOccurrence(),
                        ex);
            }
            return Optional.empty();
        }
    }

    private String key(String appId) {
        return properties.getV1().getTokenRedisKeyPrefix() + appId;
    }

    interface RedisV1TokenOperations {
        boolean set(String key, String token, Duration ttl);

        Optional<String> get(String key);
    }

    private static final class ReactiveRedisV1TokenOperations implements RedisV1TokenOperations {

        private final ReactiveStringRedisTemplate redisTemplate;

        private ReactiveRedisV1TokenOperations(ReactiveStringRedisTemplate redisTemplate) {
            this.redisTemplate = redisTemplate;
        }

        @Override
        public boolean set(String key, String token, Duration ttl) {
            Boolean stored = redisTemplate.opsForValue()
                    .set(key, token, ttl)
                    .block(OPERATION_TIMEOUT);
            return Boolean.TRUE.equals(stored);
        }

        @Override
        public Optional<String> get(String key) {
            return Optional.ofNullable(redisTemplate.opsForValue()
                    .get(key)
                    .block(OPERATION_TIMEOUT));
        }
    }
}
