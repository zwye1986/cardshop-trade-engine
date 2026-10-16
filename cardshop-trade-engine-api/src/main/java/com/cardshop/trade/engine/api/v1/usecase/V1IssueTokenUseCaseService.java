package com.cardshop.trade.engine.api.v1.usecase;

import com.cardshop.trade.engine.api.v1.dto.V1IssueTokenRequest;
import com.cardshop.trade.engine.api.v1.security.V1TokenAuthenticator;
import com.cardshop.trade.engine.api.v1.security.V1TokenSettings;
import com.cardshop.trade.engine.application.v1.IssueV1TokenUseCase;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.application.v1.V1CardApiResultStatus;
import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.application.v1.V1TokenStorePort;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredential;
import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort;
import com.cardshop.trade.engine.application.logging.LogStormGuard;

/**
 * V1.2 Token签发与商户当前签名密钥领取；对应详细设计3.1。
 * 首次初始化商户密钥，后续领取复用当前版本，不与 token 或订单建立绑定。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」
 * 和「3. API/V1 接入层」。实现遵循旧系统 `TokenUtils` 的兼容语义：HS512 JWT、
 * claim key {@code order_token_key:}、Redis key {@code token:app_id:{appId}}。
 */
public class V1IssueTokenUseCaseService implements IssueV1TokenUseCase {

    private static final Logger log = LoggerFactory.getLogger(V1IssueTokenUseCaseService.class);

    private final V1MerchantProfilePort merchantProfilePort;
    private final V1TokenStorePort tokenStorePort;
    private final V12SigningCredentialStorePort credentialStore;
    private static final LogStormGuard LOG_GUARD = LogStormGuard.defaultGuard();
    private final V1TokenSettings settings;
    private final Clock clock;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public V1IssueTokenUseCaseService(
            V1MerchantProfilePort merchantProfilePort,
            V1TokenStorePort tokenStorePort,
            V12SigningCredentialStorePort credentialStore,
            V1TokenSettings settings,
            Clock clock) {
        this.merchantProfilePort = merchantProfilePort;
        this.tokenStorePort = tokenStorePort;
        this.credentialStore = credentialStore;
        this.settings = settings;
        this.clock = clock;
    }

    @Override
    public V1CardApiResult issueToken(String rawBody) {
        V1IssueTokenRequest request = decode(rawBody);
        if (request == null
                || !StringUtils.hasText(request.appId())
                || !StringUtils.hasText(request.appSecret())) {
            return V1CardApiResultStatus.PARAM_ERROR.result();
        }
        if (!StringUtils.hasText(settings.jwtSecret())) {
            return V1CardApiResultStatus.SYSTEM_ERROR.result();
        }

        Optional<V1MerchantProfile> merchant = merchantProfilePort.findByAppIdAndAppSecret(
                request.appId(),
                request.appSecret());
        if (merchant.isEmpty() || !merchant.get().enabled()) {
            return V1CardApiResultStatus.AUTH_FAILED.result();
        }

        try {
            // 详细设计3.1：密钥按商户原子首次初始化，刷新只更换token，不创建或延长密钥。
            V12SigningCredential credential = credentialStore.getOrCreateCurrent(merchant.get().orgCode());
            String token = createToken(merchant.get());
            if (!tokenStorePort.storeIssuedToken(merchant.get().appId(), token, settings.tokenTtl())) {
                return V1CardApiResultStatus.SYSTEM_ERROR.result();
            }
            return V1CardApiResultStatus.SUCCESS.result(Map.of("token", token,
                    "keyId", credential.keyId(), "signKey", credential.signKey()));
        } catch (Exception ex) {
            // 签发属于高频入口，依赖故障时采样error，禁止打印凭证或请求体。
            if (LOG_GUARD.shouldLog("v12-credential.issue.failed").shouldLog()) {
                log.error("V1.2凭证签发失败 appId={}, orgCode={}", merchant.get().appId(), merchant.get().orgCode(), ex);
            }
            return V1CardApiResultStatus.SYSTEM_ERROR.result();
        }
    }

    private V1IssueTokenRequest decode(String rawBody) {
        if (!StringUtils.hasText(rawBody)) {
            return null;
        }
        try {
            return objectMapper.readValue(rawBody, V1IssueTokenRequest.class);
        } catch (Exception ex) {
            // 外部坏报文为预期拒绝；采样warn避免入口日志风暴，参见详细设计3.1。
            if (LOG_GUARD.shouldLog("v12-credential.decode.failed").shouldLog()) {
                log.warn("V1 Token生成请求解码失败 rawBodyLength={}", rawBody.length(), ex);
            }
            return null;
        }
    }

    private String createToken(V1MerchantProfile merchant) {
        long now = clock.instant().getEpochSecond();
        long exp = now + settings.tokenTtl().toSeconds();
        Map<String, Object> header = Map.of("alg", "HS512", "typ", "JWT");
        Map<String, Object> payload = Map.of(
                settings.tokenClaimKey(), merchant.appId(),
                // keyId不再充当token唯一标识；同秒刷新以独立jti区分，密钥也不进入JWT。
                "jti", UUID.randomUUID().toString(),
                "orgCode", merchant.orgCode(),
                "org_code", merchant.orgCode(),
                "iat", now,
                "nbf", now,
                "exp", exp);
        String signingInput = base64Json(header) + "." + base64Json(payload);
        return signingInput + "." + V1TokenAuthenticator.hmacSha512Base64Url(settings.jwtSecret(), signingInput);
    }

    private String base64Json(Map<String, Object> value) {
        try {
            byte[] json = objectMapper.writeValueAsBytes(value);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(json);
        } catch (Exception ex) {
            throw new IllegalStateException("V1 token json encode failed", ex);
        }
    }
}
