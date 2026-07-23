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
import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.logging.LogStormGuard.LogDecision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * `/v1/cardapi` 协议预校验服务。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * 该服务只处理 Redis/Lua 前的确定性协议校验：请求体解码、timestamp 偏差和 SM3 签名。
 *
 * <p>创建订单支持压测签名结果旁路：见
 * {@code docs/superpowers/specs/2026-07-23-v1-load-test-sign-bypass-design.md}。
 * 旁路仍调用 {@link V1Sm3SignatureVerifier#verifyCreate} 计算 SM3，仅在比对失败且
 * {@code X-Load-Test} 与配置口令匹配时视为通过；查单/作废路径不旁路。
 */
@Component
public class V1ProtocolPrecheckService {

    private static final Logger log = LoggerFactory.getLogger(V1ProtocolPrecheckService.class);
    private static final String LOG_EVENT_DECODE_FAILED = "v1-protocol-precheck.decode.failed";
    /** 压测签名旁路命中事件；高频路径使用 LogStormGuard 限流 warn。 */
    private static final String LOG_EVENT_SIGN_BYPASS = "v1-protocol-precheck.sign-bypass";

    private final V1RequestBodyCodec bodyCodec;
    private final V1TimestampValidator timestampValidator;
    private final V1Sm3SignatureVerifier signatureVerifier;
    private final V1ProtocolSettings settings;
    private final LogStormGuard logStormGuard = LogStormGuard.defaultGuard();

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
     * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」与
     * {@code docs/superpowers/specs/2026-07-23-v1-load-test-sign-bypass-design.md}：
     * 始终执行 SM3；签名比对失败时，若 {@code loadTestHeader} 与已配置非空口令常量时间相等，
     * 则旁路比对结果并返回通过（禁止在日志中打印口令、aesKey、sign）。
     *
     * @param loadTestHeader 请求头 {@code X-Load-Test} 的值；空白或未配置口令时不旁路
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
                (payload, aesKey) -> signatureVerifier.verifyCreate(payload, aesKey),
                true,
                loadTestHeader);
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
                (payload, aesKey) -> signatureVerifier.verifyQuery(payload, aesKey),
                false,
                null);
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
                (payload, aesKey) -> signatureVerifier.verifyInvalid(payload, aesKey),
                false,
                null);
    }

    private <T> V1ProtocolPrecheckResult<T> precheck(
            String rawBody,
            V1MerchantSecurityProfile merchant,
            Class<T> payloadType,
            Function<T, String> timestampExtractor,
            BiFunction<T, String, V1SignatureVerifyResult> signatureVerifierFunction,
            boolean createSignBypassEnabled,
            String loadTestHeader) {
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

        // 始终执行 SM3（含压测旁路路径），不跳过摘要计算成本。
        V1SignatureVerifyResult signatureResult = signatureVerifierFunction.apply(payload, merchant.aesKey());
        if (!signatureResult.valid()) {
            if (createSignBypassEnabled && allowCreateSignBypass(loadTestHeader)) {
                // 高频压测路径：限流 warn，仅脱敏商户标识，禁止回显口令/密钥/签名。
                LogDecision decision = logStormGuard.shouldLog(LOG_EVENT_SIGN_BYPASS);
                if (decision.shouldLog()) {
                    log.warn("V1创建订单压测签名旁路命中 appId={}, orgCode={}, occurrence={}, sampled={}",
                            merchant.appId(),
                            merchant.orgCode(),
                            decision.occurrence(),
                            decision.sampledOccurrence());
                }
                return V1ProtocolPrecheckResult.ok(payload);
            }
            return V1ProtocolPrecheckResult.invalid(V1ProtocolFailureReason.SIGN_FAILED);
        }
        return V1ProtocolPrecheckResult.ok(payload);
    }

    /**
     * 是否允许创建订单在签名比对失败时旁路。
     *
     * <p>参见 {@code docs/superpowers/specs/2026-07-23-v1-load-test-sign-bypass-design.md}：
     * 配置口令空白或 Header 空白则关闭；否则对 UTF-8 字节做常量时间比较。
     */
    private boolean allowCreateSignBypass(String loadTestHeader) {
        String configured = settings.loadTestToken();
        if (!StringUtils.hasText(configured) || !StringUtils.hasText(loadTestHeader)) {
            return false;
        }
        return MessageDigest.isEqual(
                configured.getBytes(StandardCharsets.UTF_8),
                loadTestHeader.getBytes(StandardCharsets.UTF_8));
    }
}
