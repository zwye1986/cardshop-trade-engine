package com.cardshop.trade.engine.api.v1.security;

import com.cardshop.trade.engine.application.v1.V1TokenStorePort;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class V1TokenAuthenticatorTest {

    @Test
    void acceptsBearerAndBareJwtTokens() {
        V1TokenSettings settings = new V1TokenSettings(
                "test-secret",
                "order_token_key:",
                Duration.ofSeconds(30)
        );

        Clock clock = Clock.fixed(Instant.parse("2026-07-05T04:00:00Z"), ZoneOffset.UTC);
        V1TokenAuthenticator authenticator = new V1TokenAuthenticator(settings, clock);
        String jwt = jwt("test-secret", """
                {"alg":"HS512","typ":"JWT"}
                """, """
                {"order_token_key:":"app-gray","orgCode":"A36","exp":1783230000}
                """);

        assertThat(authenticator.authenticate("Bearer " + jwt))
                .contains(new AuthenticatedMerchant("app-gray", "A36"));
        assertThat(authenticator.authenticate(jwt))
                .contains(new AuthenticatedMerchant("app-gray", "A36"));
    }

    @Test
    void rejectsTokensWithInvalidSignature() {
        V1TokenSettings settings = new V1TokenSettings(
                "right-secret",
                "order_token_key:",
                Duration.ofSeconds(30)
        );

        V1TokenAuthenticator authenticator = new V1TokenAuthenticator(
                settings,
                Clock.fixed(Instant.parse("2026-07-05T04:00:00Z"), ZoneOffset.UTC)
        );
        String jwt = jwt("wrong-secret", "{\"alg\":\"HS512\",\"typ\":\"JWT\"}", """
                {"order_token_key:":"app-gray","orgCode":"A36","exp":1783230000}
                """);

        assertThat(authenticator.authenticate(jwt)).isEmpty();
    }

    @Test
    void rejectsTokenWhenRedisStoredValueDoesNotMatchJwt() {
        V1TokenSettings settings = new V1TokenSettings(
                "test-secret",
                "order_token_key:",
                Duration.ofSeconds(30)
        );
        Clock clock = Clock.fixed(Instant.parse("2026-07-05T04:00:00Z"), ZoneOffset.UTC);
        V1TokenStorePort tokenStore = new V1TokenStorePort() {
            @Override
            public boolean storeIssuedToken(String appId, String token, Duration ttl) {
                return true;
            }

            @Override
            public Optional<String> findIssuedToken(String appId) {
                return Optional.of("different-token");
            }
        };
        V1TokenAuthenticator authenticator = new V1TokenAuthenticator(settings, clock, Optional.of(tokenStore));
        String jwt = jwt("test-secret", """
                {"alg":"HS512","typ":"JWT"}
                """, """
                {"order_token_key:":"app-gray","orgCode":"A36","exp":1783230000}
                """);

        assertThat(authenticator.authenticate(jwt)).isEmpty();
    }

    private static String jwt(String secret, String headerJson, String payloadJson) {
        String header = base64Url(headerJson.getBytes(StandardCharsets.UTF_8));
        String payload = base64Url(payloadJson.getBytes(StandardCharsets.UTF_8));
        String signingInput = header + "." + payload;
        String signature = V1TokenAuthenticator.hmacSha512Base64Url(secret, signingInput);
        return signingInput + "." + signature;
    }

    private static String base64Url(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
