package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Redis V1 Token 存储适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.1.1 `/v1/cardapi` 路由兼容设计」。
 * 适配器必须沿用旧系统共享 key {@code token:app_id:{appId}}，并按签发 TTL 设置 Redis 过期时间。
 */
class RedisV1TokenStoreAdapterTest {

    @Test
    void storesTokenWithLegacyAppIdKeyAndTtl() {
        RecordingTokenOperations tokenOperations = new RecordingTokenOperations();
        RedisV1TokenStoreAdapter adapter = new RedisV1TokenStoreAdapter(tokenOperations, new TradeEngineProperties());

        boolean stored = adapter.storeIssuedToken("app-gray", "jwt-token", Duration.ofHours(3));

        assertThat(stored).isTrue();
        assertThat(tokenOperations.key).isEqualTo("token:app_id:app-gray");
        assertThat(tokenOperations.token).isEqualTo("jwt-token");
        assertThat(tokenOperations.ttl).isEqualTo(Duration.ofHours(3));
    }

    @Test
    void readsTokenByLegacyAppIdKey() {
        RecordingTokenOperations tokenOperations = new RecordingTokenOperations();
        tokenOperations.storedToken = "jwt-token";
        RedisV1TokenStoreAdapter adapter = new RedisV1TokenStoreAdapter(tokenOperations, new TradeEngineProperties());

        assertThat(adapter.findIssuedToken("app-gray")).contains("jwt-token");
        assertThat(tokenOperations.key).isEqualTo("token:app_id:app-gray");
    }

    private static final class RecordingTokenOperations implements RedisV1TokenStoreAdapter.RedisV1TokenOperations {

        private String key;
        private String token;
        private Duration ttl;
        private String storedToken;

        @Override
        public boolean set(String key, String token, Duration ttl) {
            this.key = key;
            this.token = token;
            this.ttl = ttl;
            this.storedToken = token;
            return true;
        }

        @Override
        public Optional<String> get(String key) {
            this.key = key;
            return Optional.ofNullable(storedToken);
        }
    }
}
