package com.cardshop.trade.engine.api.v1.usecase;

import com.cardshop.trade.engine.api.v1.dto.V1RechargeCreateOrderRequest;
import com.cardshop.trade.engine.api.v1.protocol.V1MerchantSecurityProfile;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckResult;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckService;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import com.cardshop.trade.engine.application.v1.CreateRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.RechargeOrderIdGenerator;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.application.v1.V1MerchantCommand;
import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.application.v1.V1ProductCandidateSelector;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.domain.service.RequestHashCanonicalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;

/**
 * V1 创建直充订单新链路用例。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」、
 * 「20.13.5 商品选择规则」和「20.13.6 Redis Lua 下单结果」。该用例完成协议预检、request_hash、
 * Java 商品候选校验和 Redis-first 原子接单；WAIT、MQ confirm 和 MySQL 落库属于 D5/D6 后续职责。
 */
@Component
@Primary
@ConditionalOnBean({AcceptRechargeOrderPort.class, V1MerchantProfilePort.class, V1ProductCandidateSelector.class})
public class V1CreateRechargeOrderUseCaseService implements CreateRechargeOrderUseCase {

    private static final Logger log = LoggerFactory.getLogger(V1CreateRechargeOrderUseCaseService.class);

    private final V1ProtocolPrecheckService precheckService;
    private final RequestHashCanonicalizer requestHashCanonicalizer;
    private final V1MerchantProfilePort merchantProfilePort;
    private final V1ProductCandidateSelector productCandidateSelector;
    private final AcceptRechargeOrderPort acceptRechargeOrderPort;
    private final RechargeOrderIdGenerator orderIdGenerator;
    private final Clock clock;
    private final V1RechargeAcceptSettings settings;

    public V1CreateRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            RequestHashCanonicalizer requestHashCanonicalizer,
            V1MerchantProfilePort merchantProfilePort,
            V1ProductCandidateSelector productCandidateSelector,
            AcceptRechargeOrderPort acceptRechargeOrderPort,
            RechargeOrderIdGenerator orderIdGenerator,
            Clock clock,
            V1RechargeAcceptSettings settings) {
        this.precheckService = precheckService;
        this.requestHashCanonicalizer = requestHashCanonicalizer;
        this.merchantProfilePort = merchantProfilePort;
        this.productCandidateSelector = productCandidateSelector;
        this.acceptRechargeOrderPort = acceptRechargeOrderPort;
        this.orderIdGenerator = orderIdGenerator;
        this.clock = clock;
        this.settings = settings;
    }

    @Override
    public V1CardApiResult createRechargeOrder(V1MerchantCommand command) {
        Optional<V1MerchantProfile> maybeMerchant = merchantProfilePort.findByAppIdAndOrgCode(command.appId(), command.orgCode());
        if (maybeMerchant.isEmpty() || !StringUtils.hasText(maybeMerchant.get().aesKey())) {
            log.warn("V1直充下单商户资料缺失 appId={}, orgCode={}", command.appId(), command.orgCode());
            return new V1CardApiResult("401", "凭证错误", null);
        }

        V1MerchantProfile merchant = maybeMerchant.get();
        V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> precheck = precheckService.precheckCreate(
                command.rawBody(),
                new V1MerchantSecurityProfile(merchant.appId(), merchant.orgCode(), merchant.aesKey()));
        if (!precheck.valid()) {
            return protocolFailure(precheck.reason());
        }

        V1RechargeCreateOrderRequest request = precheck.payload();
        String syntaxError = validatePayload(request);
        if (syntaxError != null) {
            return new V1CardApiResult("506", syntaxError, null);
        }

        V1ProductSelection productSelection = productCandidateSelector.select(
                merchant.orgCode(),
                request.productId(),
                request.account());
        BusinessFailure businessFailure = firstBusinessFailure(merchant, request, productSelection);
        ValidationStatus validationStatus = businessFailure == null ? productSelection.validationStatus() : ValidationStatus.FAILED;
        String candidateFailCode = businessFailure == null ? productSelection.failCode() : businessFailure.failCode();
        String candidateFailReason = businessFailure == null ? productSelection.failReason() : businessFailure.failReason();

        var requestHash = requestHashCanonicalizer.canonicalizeRechargeCreate(
                new RequestHashCanonicalizer.RechargeCreateFields(
                        merchant.orgCode(),
                        request.productId(),
                        request.outOrderSn(),
                        request.account(),
                        request.notifyUrl(),
                        request.num()));
        long now = clock.instant().toEpochMilli();
        AcceptRechargeOrderCommand acceptCommand = new AcceptRechargeOrderCommand(
                merchant.orgCode(),
                merchant.appId(),
                request.outOrderSn(),
                orderIdGenerator.nextOrderId(),
                request.productId(),
                productSelection.selectedProductId(),
                request.account(),
                request.notifyUrl(),
                request.nonceStr(),
                request.num(),
                productSelection.salePriceCent(),
                productSelection.productName(),
                productSelection.faceValueCent(),
                productSelection.productType(),
                productSelection.couponType(),
                productSelection.supplierChannel(),
                requestHash.value(),
                requestHash.version(),
                requestHash.algorithm(),
                validationStatus,
                candidateFailCode,
                candidateFailReason,
                merchant.configVersion(),
                productSelection.productConfigVersion(),
                now,
                now + settings.firstPersistRetryDelay().toMillis());
        AcceptRechargeOrderResult acceptResult = acceptRechargeOrderPort.accept(acceptCommand).toCompletableFuture().join();
        return toV1Result(request.outOrderSn(), acceptResult);
    }

    private static BusinessFailure firstBusinessFailure(
            V1MerchantProfile merchant,
            V1RechargeCreateOrderRequest request,
            V1ProductSelection productSelection) {
        if (!merchant.enabled()) {
            return new BusinessFailure("MERCHANT_DISABLED", "商户已停用");
        }
        if (!Integer.valueOf(1).equals(request.num())) {
            return new BusinessFailure("INVALID_NUM", "直充产品仅支持单数量下单！");
        }
        if (productSelection.validationStatus() == ValidationStatus.FAILED) {
            return new BusinessFailure(productSelection.failCode(), productSelection.failReason());
        }
        return null;
    }

    private static String validatePayload(V1RechargeCreateOrderRequest request) {
        if (!StringUtils.hasText(request.outOrderSn())
                || !StringUtils.hasText(request.productId())
                || !StringUtils.hasText(request.account())
                || !StringUtils.hasText(request.nonceStr())
                || request.num() == null) {
            return "参数错误";
        }
        if (request.nonceStr().length() != 32) {
            return "nonce_str长度不正确";
        }
        if (!StringUtils.hasText(request.notifyUrl()) || !isHttpUrl(request.notifyUrl())) {
            return "notify_url格式不正确";
        }
        return null;
    }

    private static boolean isHttpUrl(String value) {
        try {
            URI uri = URI.create(value);
            return "http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme());
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    private static V1CardApiResult protocolFailure(String reason) {
        return switch (reason) {
            case "SIGN_FAILED" -> new V1CardApiResult("501", "验签不合规！", null);
            case "DECRYPT_FAILED" -> new V1CardApiResult("501", "验签失败！", null);
            case "TIMESTAMP_OUT_OF_RANGE" -> new V1CardApiResult("506", "timestamp超出允许范围", null);
            default -> new V1CardApiResult("506", "参数错误", null);
        };
    }

    private static V1CardApiResult toV1Result(String outOrderSn, AcceptRechargeOrderResult result) {
        AcceptRechargeOrderStatus status = result.status();
        if (status == AcceptRechargeOrderStatus.ACCEPTED) {
            return new V1CardApiResult(
                    "200",
                    "操作成功",
                    Map.of("order_sn", result.orderId(), "out_order_sn", outOrderSn));
        }
        if (status == AcceptRechargeOrderStatus.ACCEPTED_FAILED) {
            return new V1CardApiResult(businessCode(result.failCode()), result.failReason(), null);
        }
        if (status == AcceptRechargeOrderStatus.DUPLICATE) {
            return new V1CardApiResult("505", defaultText(result.failReason(), "订单号重复"), null);
        }
        if (status == AcceptRechargeOrderStatus.NONCE_REPLAY) {
            return new V1CardApiResult("501", defaultText(result.failReason(), "请求随机串重复"), null);
        }
        return new V1CardApiResult("500", defaultText(result.failReason(), "系统繁忙，请稍后重试"), null);
    }

    private static String businessCode(String failCode) {
        return switch (defaultText(failCode, "")) {
            case "BALANCE_NOT_ENOUGH", "MERCHANT_DISABLED" -> "504";
            default -> "503";
        };
    }

    private static String defaultText(String value, String fallback) {
        return StringUtils.hasText(value) ? value : fallback;
    }

    private record BusinessFailure(String failCode, String failReason) {
    }
}
