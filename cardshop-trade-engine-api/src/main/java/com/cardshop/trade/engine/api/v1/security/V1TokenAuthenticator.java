package com.cardshop.trade.engine.api.v1.security;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.cardshop.trade.engine.application.v1.V1TokenStorePort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

/**
 * 原卡管 `/v1/cardapi` JWT Token 兼容解析器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.4 Token 兼容校验」。
 * 兼容 Bearer/bare JWT、HS512 签名、时间窗校验，并在 Redis token 存储端口存在时复用旧系统
 * {@code token:app_id:{appId}} 规则做“最后签发 token”一致性校验。
 */
@Component
public class V1TokenAuthenticator {

    private static final Logger log = LoggerFactory.getLogger(V1TokenAuthenticator.class);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };
    private static final String HMAC_SHA_512 = "HmacSHA512";

    private final V1TokenSettings settings;
    private final Clock clock;
    private final Optional<V1TokenStorePort> tokenStorePort;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public V1TokenAuthenticator(V1TokenSettings settings, ObjectProvider<V1TokenStorePort> tokenStorePort) {
        this(settings, Clock.systemUTC(), Optional.ofNullable(tokenStorePort.getIfAvailable()));
    }

    V1TokenAuthenticator(V1TokenSettings settings, Clock clock) {
        this(settings, clock, Optional.empty());
    }

    public V1TokenAuthenticator(V1TokenSettings settings, Clock clock, Optional<V1TokenStorePort> tokenStorePort) {
        this.settings = settings;
        this.clock = clock;
        this.tokenStorePort = tokenStorePort == null ? Optional.empty() : tokenStorePort;
    }

    /**
     * 解析兼容旧系统的 Bearer 或裸 JWT；任何解析失败都返回 empty，避免异常穿透入口层。
     */
    public Optional<AuthenticatedMerchant> authenticate(String authorization) {
        String token = normalizeToken(authorization);
        if (!StringUtils.hasText(token) || !StringUtils.hasText(settings.jwtSecret())) {
            return Optional.empty();
        }
        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            return Optional.empty();
        }

        String signingInput = parts[0] + "." + parts[1];
        String expectedSignature = hmacSha512Base64Url(settings.jwtSecret(), signingInput);
        if (!MessageDigest.isEqual(expectedSignature.getBytes(StandardCharsets.US_ASCII),
                parts[2].getBytes(StandardCharsets.US_ASCII))) {
            return Optional.empty();
        }

        try {
            Map<String, Object> header = readJson(parts[0]);
            if (!"HS512".equals(String.valueOf(header.get("alg")))) {
                return Optional.empty();
            }
            Map<String, Object> payload = readJson(parts[1]);
            if (!isWithinTimeWindow(payload)) {
                return Optional.empty();
            }
            Object appIdValue = payload.get(settings.tokenClaimKey());
            String appId = appIdValue == null ? null : String.valueOf(appIdValue).trim();
            if (!StringUtils.hasText(appId)) {
                return Optional.empty();
            }
            if (tokenStorePort.isPresent()
                    && tokenStorePort.get().findIssuedToken(appId).filter(token::equals).isEmpty()) {
                return Optional.empty();
            }
            String orgCode = optionalText(payload.get("orgCode"));
            if (!StringUtils.hasText(orgCode)) {
                orgCode = optionalText(payload.get("org_code"));
            }
            return Optional.of(new AuthenticatedMerchant(appId, orgCode));
        } catch (Exception ex) {
            log.error("V1 token解析失败 tokenLength={}", token.length(), ex);
            return Optional.empty();
        }
    }

    private boolean isWithinTimeWindow(Map<String, Object> payload) {
        long now = clock.instant().getEpochSecond();
        long skewSeconds = settings.clockSkew().toSeconds();
        Long exp = longValue(payload.get("exp"));
        if (exp != null && exp + skewSeconds < now) {
            return false;
        }
        Long nbf = longValue(payload.get("nbf"));
        return nbf == null || nbf - skewSeconds <= now;
    }

    private Map<String, Object> readJson(String base64Url) throws Exception {
        byte[] bytes = Base64.getUrlDecoder().decode(base64Url);
        return objectMapper.readValue(bytes, MAP_TYPE);
    }

    private static Long longValue(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String text && StringUtils.hasText(text)) {
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException ex) {
                log.error("V1 token数值claim解析失败 valueLength={}", text.length(), ex);
                return null;
            }
        }
        return null;
    }

    private static String optionalText(Object value) {
        return value == null ? null : String.valueOf(value).trim();
    }

    private static String normalizeToken(String authorization) {
        if (!StringUtils.hasText(authorization)) {
            return null;
        }
        String token = authorization.trim();
        if (token.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())) {
            return token.substring("Bearer ".length()).trim();
        }
        return token;
    }

    public static String hmacSha512Base64Url(String secret, String signingInput) {
        try {
            Mac mac = Mac.getInstance(HMAC_SHA_512);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_SHA_512));
            byte[] signature = mac.doFinal(signingInput.getBytes(StandardCharsets.US_ASCII));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(signature);
        } catch (Exception ex) {
            throw new IllegalStateException("HS512 signing failed", ex);
        }
    }
}
