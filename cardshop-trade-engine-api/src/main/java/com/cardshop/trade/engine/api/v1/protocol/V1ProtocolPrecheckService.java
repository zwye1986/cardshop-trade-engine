package com.cardshop.trade.engine.api.v1.protocol;

import com.cardshop.trade.engine.api.v1.codec.V1RequestBodyCodec;
import com.cardshop.trade.engine.api.v1.codec.V1RequestBodyDecodeException;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeCreateOrderRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderInvalidRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderQueryRequest;
import com.cardshop.trade.engine.api.v1.security.V1SignatureVerifyResult;
import com.cardshop.trade.engine.api.v1.security.V1Sm3SignatureVerifier;
import com.cardshop.trade.engine.api.v1.security.V1TimestampValidator;
import com.cardshop.trade.engine.api.v1.security.V1TimestampVerifyResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * `/v1/cardapi` 协议预校验服务。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.3 SM4 解密与 SM3 验签」。
 * 该服务只处理 Redis/Lua 前的确定性协议校验：请求体解码、timestamp 偏差和 SM3 签名。
 */
@Component
public class V1ProtocolPrecheckService {

    private static final Logger log = LoggerFactory.getLogger(V1ProtocolPrecheckService.class);

    private final V1RequestBodyCodec bodyCodec;
    private final V1TimestampValidator timestampValidator;
    private final V1Sm3SignatureVerifier signatureVerifier;
    private final V1ProtocolSettings settings;

    public V1ProtocolPrecheckService(
            V1RequestBodyCodec bodyCodec,
            V1TimestampValidator timestampValidator,
            V1Sm3SignatureVerifier signatureVerifier,
            V1ProtocolSettings settings) {
        this.bodyCodec = bodyCodec;
        this.timestampValidator = timestampValidator;
        this.signatureVerifier = signatureVerifier;
        this.settings = settings;
    }

    /**
     * 创建直充订单的协议预校验。
     */
    public V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> precheckCreate(
            String rawBody,
            V1MerchantSecurityProfile merchant) {
        return precheck(
                rawBody,
                merchant,
                V1RechargeCreateOrderRequest.class,
                V1RechargeCreateOrderRequest::timestamp,
                (payload, aesKey) -> signatureVerifier.verifyCreate(payload, aesKey));
    }

    /**
     * 查单接口的协议预校验。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」
     * 和「20.13.10 查单设计」。查单只做身份、解密、时间戳和签名确认，不在协议层访问供应商或修改订单。
     */
    public V1ProtocolPrecheckResult<V1RechargeOrderQueryRequest> precheckQuery(
            String rawBody,
            V1MerchantSecurityProfile merchant) {
        return precheck(
                rawBody,
                merchant,
                V1RechargeOrderQueryRequest.class,
                V1RechargeOrderQueryRequest::timestamp,
                (payload, aesKey) -> signatureVerifier.verifyQuery(payload, aesKey));
    }

    /**
     * 作废接口的协议预校验。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」
     * 和「20.13.11 作废设计」。作废状态机和未落库订单保护属于后续应用用例，不在协议预校验内完成。
     */
    public V1ProtocolPrecheckResult<V1RechargeOrderInvalidRequest> precheckInvalid(
            String rawBody,
            V1MerchantSecurityProfile merchant) {
        return precheck(
                rawBody,
                merchant,
                V1RechargeOrderInvalidRequest.class,
                V1RechargeOrderInvalidRequest::timestamp,
                (payload, aesKey) -> signatureVerifier.verifyInvalid(payload, aesKey));
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
            log.error("V1协议请求体解码失败 payloadType={}, sm4Enabled={}, appId={}, orgCode={}",
                    payloadType.getSimpleName(),
                    settings.requestSm4Enabled(),
                    merchant.appId(),
                    merchant.orgCode(),
                    ex);
            return V1ProtocolPrecheckResult.invalid(settings.requestSm4Enabled() ? "DECRYPT_FAILED" : "REQUEST_BODY_INVALID");
        }

        V1TimestampVerifyResult timestampResult = timestampValidator.validate(timestampExtractor.apply(payload));
        if (!timestampResult.valid()) {
            return V1ProtocolPrecheckResult.invalid(timestampResult.reason());
        }

        V1SignatureVerifyResult signatureResult = signatureVerifierFunction.apply(payload, merchant.aesKey());
        if (!signatureResult.valid()) {
            return V1ProtocolPrecheckResult.invalid("SIGN_FAILED");
        }
        return V1ProtocolPrecheckResult.ok(payload);
    }
}
