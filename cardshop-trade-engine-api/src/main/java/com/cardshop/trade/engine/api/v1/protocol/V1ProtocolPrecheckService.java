package com.cardshop.trade.engine.api.v1.protocol;

import com.cardshop.trade.engine.api.v1.codec.V1RequestBodyCodec;
import com.cardshop.trade.engine.api.v1.codec.V1RequestBodyDecodeException;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeCreateOrderRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderInvalidRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderQueryRequest;
import com.cardshop.trade.engine.api.v1.security.V1SignatureVerifyResult;
import com.cardshop.trade.engine.api.v1.security.V12SignatureVerifier;
import com.cardshop.trade.engine.api.v1.security.V1TimestampValidator;
import com.cardshop.trade.engine.api.v1.security.V1TimestampVerifyResult;
import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.logging.LogStormGuard.LogDecision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * `/v1/cardapi` 协议预校验服务。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * 该服务只处理 Redis/Lua 前的确定性协议校验：请求体解码、timestamp 偏差和 HMAC-SM3签名。
 *
 * <p>V1.2已移除压测签名旁路，任何验签失败都拒绝。
 */
@Component
public class V1ProtocolPrecheckService {

    private static final Logger log = LoggerFactory.getLogger(V1ProtocolPrecheckService.class);
    private static final String LOG_EVENT_DECODE_FAILED = "v1-protocol-precheck.decode.failed";

    private final V1RequestBodyCodec bodyCodec;
    private final V1TimestampValidator timestampValidator;
    private final V12SignatureVerifier signatureVerifier;
    private final V1ProtocolSettings settings;
    private final LogStormGuard logStormGuard = LogStormGuard.defaultGuard();

    public V1ProtocolPrecheckService(
            V1RequestBodyCodec bodyCodec,
            V1TimestampValidator timestampValidator,
            V12SignatureVerifier signatureVerifier,
            V1ProtocolSettings settings) {
        this.bodyCodec = bodyCodec;
        this.timestampValidator = timestampValidator;
        this.signatureVerifier = signatureVerifier;
        this.settings = settings;
    }

    /**
     * 创建直充订单的协议预校验（无压测 Header）。
     */
    public V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> precheckCreate(
            String rawBody,
            V1MerchantSecurityProfile merchant) {
        return precheckCreate(rawBody, merchant, null);
    }

    /**
     * 创建直充订单的协议预校验。
     *
     * <p>参见详细设计3.1；loadTestHeader仅保留调用兼容性，不再影响签名结果。
     */
    public V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> precheckCreate(
            String rawBody,
            V1MerchantSecurityProfile merchant,
            String loadTestHeader) {
        return precheck(
                rawBody,
                merchant,
                V1RechargeCreateOrderRequest.class,
                V1RechargeCreateOrderRequest::timestamp,
                (payload, signKey) -> signatureVerifier.verifyCreate(payload, signKey));
    }

    /**
     * 查单接口的协议预校验。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」
     * 和「3. API/V1 接入层」。查单只做身份、解密、时间戳和签名确认，不在协议层访问供应商或修改订单。
     * 压测签名旁路不适用于查单。
     */
    public V1ProtocolPrecheckResult<V1RechargeOrderQueryRequest> precheckQuery(
            String rawBody,
            V1MerchantSecurityProfile merchant) {
        return precheck(
                rawBody,
                merchant,
                V1RechargeOrderQueryRequest.class,
                V1RechargeOrderQueryRequest::timestamp,
                (payload, signKey) -> signatureVerifier.verifyQuery(payload, signKey));
    }

    /**
     * 作废接口的协议预校验。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」
     * 和「9.2 作废设计」。作废状态机和未落库订单保护属于后续应用用例，不在协议预校验内完成。
     * 压测签名旁路不适用于作废。
     */
    public V1ProtocolPrecheckResult<V1RechargeOrderInvalidRequest> precheckInvalid(
            String rawBody,
            V1MerchantSecurityProfile merchant) {
        return precheck(
                rawBody,
                merchant,
                V1RechargeOrderInvalidRequest.class,
                V1RechargeOrderInvalidRequest::timestamp,
                (payload, signKey) -> signatureVerifier.verifyInvalid(payload, signKey));
    }

    private <T> V1ProtocolPrecheckResult<T> precheck(
            String rawBody,
            V1MerchantSecurityProfile merchant,
            Class<T> payloadType,
            Function<T, String> timestampExtractor,
            BiFunction<T, String, V1SignatureVerifyResult> signatureVerifierFunction) {
        T payload;
        try {
            payload = settings.requestSm4Enabled()
                    ? bodyCodec.decodeSm4(rawBody, merchant.aesKey(), payloadType)
                    : bodyCodec.decodePlain(rawBody, payloadType);
        } catch (V1RequestBodyDecodeException ex) {
            LogDecision decision = logStormGuard.shouldLog(LOG_EVENT_DECODE_FAILED);
            if (decision.shouldLog()) {
                log.error("V1协议请求体解码失败 payloadType={}, sm4Enabled={}, appId={}, orgCode={}, occurrence={}, sampled={}",
                        payloadType.getSimpleName(),
                        settings.requestSm4Enabled(),
                        merchant.appId(),
                        merchant.orgCode(),
                        decision.occurrence(),
                        decision.sampledOccurrence(),
                        ex);
            }
            return V1ProtocolPrecheckResult.invalid(settings.requestSm4Enabled()
                    ? V1ProtocolFailureReason.DECRYPT_FAILED
                    : V1ProtocolFailureReason.REQUEST_BODY_INVALID);
        }

        V1TimestampVerifyResult timestampResult = timestampValidator.validate(timestampExtractor.apply(payload));
        if (!timestampResult.valid()) {
            return V1ProtocolPrecheckResult.invalid(timestampResult.reason());
        }

        // V1.2只信任认证上下文提供的成组凭证，aesKey仅交给解密器。任何旧压测头都不能旁路验签。
        var credential = merchant.signingCredential();
        if (credential == null || !merchant.appId().equals(credential.appId())
                || !merchant.orgCode().equals(credential.orgCode())) {
            return V1ProtocolPrecheckResult.invalid(V1ProtocolFailureReason.SIGN_FAILED);
        }
        V1SignatureVerifyResult signatureResult = signatureVerifierFunction.apply(payload, credential.signKey());
        if (!signatureResult.valid()) {
            return V1ProtocolPrecheckResult.invalid(V1ProtocolFailureReason.SIGN_FAILED);
        }
        return V1ProtocolPrecheckResult.ok(payload);
    }

}
