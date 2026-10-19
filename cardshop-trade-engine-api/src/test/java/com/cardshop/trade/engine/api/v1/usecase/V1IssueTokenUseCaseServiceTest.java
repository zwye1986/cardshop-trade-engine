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
    private final com.cardshop.trade.engine.api.v1.V12TestCredentials.Store credentials = new com.cardshop.trade.engine.api.v1.V12TestCredentials.Store();

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-06T02:00:00Z"), ZoneOffset.UTC);
    private static final V1TokenSettings SETTINGS = new V1TokenSettings(
            "test-secret",
            "order_token_key:",
            Duration.ofSeconds(30),
            "token:app_id:",
            Duration.ofHours(3));

    @Test
    void sameSecondRefreshChangesOnlyTokenAndReusesMerchantKey() {
        RecordingTokenStore tokenStore = new RecordingTokenStore();
        var service = new V1IssueTokenUseCaseService(merchantPort(true), tokenStore, credentials, SETTINGS, CLOCK);
        String request = "{\"app_id\":\"app-gray\",\"app_secret\":\"secret-1\"}";
        Map<?, ?> first = (Map<?, ?>) service.issueToken(request).data();
        var original = credentials.findCurrent("A36").orElseThrow();
        Map<?, ?> second = (Map<?, ?>) service.issueToken(request).data();
        assertThat(first.keySet()).isEqualTo(java.util.Set.of("token", "signKey"));
        assertThat(second.keySet()).isEqualTo(java.util.Set.of("token", "signKey"));
        assertThat(credentials.findCurrent("A36")).contains(original);
        assertThat(first.get("token")).isNotEqualTo(second.get("token"));
        assertThat(first.get("signKey")).isEqualTo(second.get("signKey"));
        var auth = new V1TokenAuthenticator(SETTINGS, CLOCK, Optional.of(tokenStore), Optional.of(credentials));
        assertThat(auth.authenticate((String) first.get("token"))).isEmpty();
        assertThat(auth.authenticate((String) second.get("token"))).isPresent();
        assertThat(credentials.findForCallback("A36", original.keyId()))
                .get().extracting(com.cardshop.trade.engine.application.v1.crypto.V12SigningCredential::signKey)
                .isEqualTo(first.get("signKey"));
        var expired = new V1TokenAuthenticator(SETTINGS, Clock.offset(CLOCK, Duration.ofHours(3)),
                Optional.of(tokenStore), Optional.of(credentials));
        assertThat(expired.authenticate((String) second.get("token"))).isEmpty();
        assertThat(credentials.findForCallback("A36", original.keyId())).isPresent();
        credentials.revoke("A36", original.keyId());
        assertThat(auth.authenticate((String) second.get("token"))).isEmpty();
    }

    @Test
    void currentKeyRotationAppliesToExistingTokenWithoutBindingJwtToKeyId() throws Exception {
        var tokenStore = new RecordingTokenStore();
        var service = new V1IssueTokenUseCaseService(merchantPort(true), tokenStore, credentials, SETTINGS, CLOCK);
        Map<?, ?> first = (Map<?, ?>) service.issueToken(
                "{\"app_id\":\"app-gray\",\"app_secret\":\"secret-1\"}").data();
        var original = credentials.findCurrent("A36").orElseThrow();
        String token = (String) first.get("token");
        var claims = new com.fasterxml.jackson.databind.ObjectMapper().readTree(
                java.util.Base64.getUrlDecoder().decode(token.split("\\.")[1]));
        assertThat(claims.has("keyId")).isFalse();
        assertThat(claims.has("signKey")).isFalse();
        assertThat(claims.path("jti").asText()).isNotBlank();
        var rotated = credentials.rotate("A36");
        var auth = new V1TokenAuthenticator(SETTINGS, CLOCK, Optional.of(tokenStore), Optional.of(credentials));
        assertThat(auth.authenticate(token)).get()
                .extracting(com.cardshop.trade.engine.api.v1.security.AuthenticatedMerchant::signingCredential)
                .isEqualTo(rotated);
        assertThat(credentials.findForCallback("A36", original.keyId())).isPresent();
        Map<?, ?> refreshed = (Map<?, ?>) service.issueToken(
                "{\"app_id\":\"app-gray\",\"app_secret\":\"secret-1\"}").data();
        assertThat(refreshed.keySet()).isEqualTo(java.util.Set.of("token", "signKey"));
        assertThat(refreshed.get("signKey")).isEqualTo(rotated.signKey());
    }

    @Test
    void revokedCurrentKeyRejectsOrdinaryGetTokenUntilExplicitRotation() {
        var tokenStore = new RecordingTokenStore();
        var service = new V1IssueTokenUseCaseService(merchantPort(true), tokenStore, credentials, SETTINGS, CLOCK);
        String request = "{\"app_id\":\"app-gray\",\"app_secret\":\"secret-1\"}";
        Map<?, ?> first = (Map<?, ?>) service.issueToken(request).data();
        var original = credentials.findCurrent("A36").orElseThrow();
        credentials.revoke("A36", original.keyId());
        assertThat(service.issueToken(request).code()).isEqualTo("502");
        assertThat(tokenStore.token).isEqualTo(first.get("token"));
        var rotated = credentials.rotate("A36");
        Map<?, ?> restored = (Map<?, ?>) service.issueToken(request).data();
        assertThat(restored.keySet()).isEqualTo(java.util.Set.of("token", "signKey"));
        assertThat(restored.get("signKey")).isEqualTo(rotated.signKey());
        assertThat(credentials.findCurrent("A36")).contains(rotated);
    }

    @Test
    void differentApplicationsOfSameMerchantShareSigningKeyButOtherMerchantsDoNot() {
        var profiles = new V1MerchantProfilePort() {
            public Optional<V1MerchantProfile> findByAppIdAndOrgCode(String appId, String orgCode) {
                return Optional.empty();
            }
            public Optional<V1MerchantProfile> findByAppIdAndAppSecret(String appId, String secret) {
                return Optional.of(new V1MerchantProfile(appId,
                        "app-other-merchant".equals(appId) ? "OTHER" : "A36", "AES-KEY-12345678", true, 7L));
            }
        };
        var service = new V1IssueTokenUseCaseService(profiles, new RecordingTokenStore(), credentials, SETTINGS, CLOCK);
        Map<?, ?> first = (Map<?, ?>) service.issueToken("{\"app_id\":\"app-one\",\"app_secret\":\"secret\"}").data();
        Map<?, ?> second = (Map<?, ?>) service.issueToken("{\"app_id\":\"app-two\",\"app_secret\":\"secret\"}").data();
        Map<?, ?> other = (Map<?, ?>) service.issueToken("{\"app_id\":\"app-other-merchant\",\"app_secret\":\"secret\"}").data();
        assertThat(first.keySet()).isEqualTo(java.util.Set.of("token", "signKey"));
        assertThat(second.keySet()).isEqualTo(java.util.Set.of("token", "signKey"));
        assertThat(other.keySet()).isEqualTo(java.util.Set.of("token", "signKey"));
        assertThat(first.get("signKey")).isEqualTo(second.get("signKey")).isNotEqualTo(other.get("signKey"));
    }

    @Test
    void issuesLegacyCompatibleTokenAndStoresItByAppId() {
        RecordingTokenStore tokenStore = new RecordingTokenStore();
        V1IssueTokenUseCaseService service = new V1IssueTokenUseCaseService(
                merchantPort(true),
                tokenStore,
                credentials,
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
        // 详细设计「交行开放平台通知认证修订 2026-10-19」：只交付token/signKey，版本标识仅内部使用。
        assertThat(data).containsOnlyKeys("token", "signKey");
        var current = credentials.findCurrent("A36").orElseThrow();
        assertThat(current.keyId()).isNotBlank();
        assertThat(data.get("signKey")).isEqualTo(current.signKey());
        String token = String.valueOf(data.get("token"));
        assertThat(token).isNotBlank();
        assertThat(tokenStore.appId).isEqualTo("app-gray");
        assertThat(tokenStore.token).isEqualTo(token);
        assertThat(tokenStore.ttl).isEqualTo(Duration.ofHours(3));

        V1TokenAuthenticator authenticator = new V1TokenAuthenticator(SETTINGS, CLOCK, Optional.of(tokenStore), Optional.of(credentials));
        assertThat(authenticator.authenticate("Bearer " + token))
                .get().extracting(com.cardshop.trade.engine.api.v1.security.AuthenticatedMerchant::orgCode).isEqualTo("A36");
    }

    @Test
    void rejectsInvalidCredentialsWithOldInterfaceCode() {
        V1IssueTokenUseCaseService service = new V1IssueTokenUseCaseService(
                (appId, orgCode) -> Optional.empty(),
                new RecordingTokenStore(),
                credentials,
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
                credentials,
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
