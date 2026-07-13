package com.cardshop.trade.engine.api.v1.usecase;

import com.cardshop.trade.engine.api.v1.security.V1TokenAuthenticator;
import com.cardshop.trade.engine.api.v1.security.V1TokenSettings;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.application.v1.V1TokenStorePort;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V1 Token 生成用例测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」
 * 和「3. API/V1 接入层」。D3 收口要求新链路 `/auth/getToken` 不能再返回 fallback，
 * 必须按旧系统 app_id/app_secret 语义生成 HS512 JWT，并写入共享 Redis token key 空间。
 */
class V1IssueTokenUseCaseServiceTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-06T02:00:00Z"), ZoneOffset.UTC);
    private static final V1TokenSettings SETTINGS = new V1TokenSettings(
            "test-secret",
            "order_token_key:",
            Duration.ofSeconds(30),
            "token:app_id:",
            Duration.ofHours(3));

    @Test
    void issuesLegacyCompatibleTokenAndStoresItByAppId() {
        RecordingTokenStore tokenStore = new RecordingTokenStore();
        V1IssueTokenUseCaseService service = new V1IssueTokenUseCaseService(
                merchantPort(true),
                tokenStore,
                SETTINGS,
                CLOCK);

        V1CardApiResult result = service.issueToken("""
                {"app_id":"app-gray","app_secret":"secret-1"}
                """);

        assertThat(result.code()).isEqualTo("200");
        assertThat(result.msg()).isEqualTo("操作成功");
        assertThat(result.data()).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) result.data();
        String token = String.valueOf(data.get("token"));
        assertThat(token).isNotBlank();
        assertThat(tokenStore.appId).isEqualTo("app-gray");
        assertThat(tokenStore.token).isEqualTo(token);
        assertThat(tokenStore.ttl).isEqualTo(Duration.ofHours(3));

        V1TokenAuthenticator authenticator = new V1TokenAuthenticator(SETTINGS, CLOCK, Optional.of(tokenStore));
        assertThat(authenticator.authenticate("Bearer " + token))
                .contains(new com.cardshop.trade.engine.api.v1.security.AuthenticatedMerchant("app-gray", "A36"));
    }

    @Test
    void rejectsInvalidCredentialsWithOldInterfaceCode() {
        V1IssueTokenUseCaseService service = new V1IssueTokenUseCaseService(
                (appId, orgCode) -> Optional.empty(),
                new RecordingTokenStore(),
                SETTINGS,
                CLOCK);

        V1CardApiResult result = service.issueToken("""
                {"app_id":"app-gray","app_secret":"bad-secret"}
                """);

        assertThat(result.code()).isEqualTo("401");
        assertThat(result.msg()).isEqualTo("凭证错误");
        assertThat(result.data()).isNull();
    }

    @Test
    void returnsSystemErrorWhenRedisTokenStoreFails() {
        RecordingTokenStore tokenStore = new RecordingTokenStore();
        tokenStore.storeResult = false;
        V1IssueTokenUseCaseService service = new V1IssueTokenUseCaseService(
                merchantPort(true),
                tokenStore,
                SETTINGS,
                CLOCK);

        V1CardApiResult result = service.issueToken("""
                {"app_id":"app-gray","app_secret":"secret-1"}
                """);

        assertThat(result.code()).isEqualTo("502");
        assertThat(result.msg()).isEqualTo("系统异常");
        assertThat(result.data()).isNull();
    }

    private static V1MerchantProfilePort merchantPort(boolean enabled) {
        return new V1MerchantProfilePort() {
            @Override
            public Optional<V1MerchantProfile> findByAppIdAndOrgCode(String appId, String orgCode) {
                return Optional.empty();
            }

            @Override
            public Optional<V1MerchantProfile> findByAppIdAndAppSecret(String appId, String appSecret) {
                if ("app-gray".equals(appId) && "secret-1".equals(appSecret)) {
                    return Optional.of(new V1MerchantProfile(appId, "A36", "AES-KEY-12345678", enabled, 7L));
                }
                return Optional.empty();
            }
        };
    }

    private static final class RecordingTokenStore implements V1TokenStorePort {

        private String appId;
        private String token;
        private Duration ttl;
        private boolean storeResult = true;

        @Override
        public boolean storeIssuedToken(String appId, String token, Duration ttl) {
            this.appId = appId;
            this.token = token;
            this.ttl = ttl;
            return storeResult;
        }

        @Override
        public Optional<String> findIssuedToken(String appId) {
            return Optional.ofNullable(this.token);
        }
    }
}
