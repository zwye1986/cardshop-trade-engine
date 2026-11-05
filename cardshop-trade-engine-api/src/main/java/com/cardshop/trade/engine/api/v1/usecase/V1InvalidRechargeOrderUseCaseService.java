package com.cardshop.trade.engine.api.v1.usecase;

import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderInvalidRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderResponse;
import com.cardshop.trade.engine.api.v1.protocol.V1MerchantSecurityProfile;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolFailureReason;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckResult;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckService;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderContext;
import com.cardshop.trade.engine.application.recharge.RechargeOrderInvalidationService;
import com.cardshop.trade.engine.application.recharge.RechargeOrderInvalidationState;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxCommand;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.application.v1.V1CardApiResultStatus;
import com.cardshop.trade.engine.application.v1.V1MerchantCommand;
import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * V1 作废订单用例。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.3 作废设计」「9.6 交行对外券状态修订」。
 * 验签后受理并立即推进原Outbox，完成返回200、未知结果202；首次与重复请求共用持久资金完成谓词。
 * 发券成功后的作废保留订单12，接口不直接调用供应商，HTTP等待与执行生命周期分离。
 */
public class V1InvalidRechargeOrderUseCaseService implements InvalidRechargeOrderUseCase {

    private static final Logger log = LoggerFactory.getLogger(V1InvalidRechargeOrderUseCaseService.class);

    private final V1ProtocolPrecheckService precheckService;
    private final V1MerchantProfilePort merchantProfilePort;
    private final RechargeOrderQueryPort orderQueryPort;
    private final RechargeOrderInvalidationService invalidationService;

    public V1InvalidRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            V1MerchantProfilePort merchantProfilePort,
            RechargeOrderQueryPort orderQueryPort,
            RechargeOrderInvalidationService invalidationService) {
        this.precheckService = precheckService;
        this.merchantProfilePort = merchantProfilePort;
        this.orderQueryPort = orderQueryPort;
        this.invalidationService = invalidationService;
    }

    @Override
    public V1CardApiResult invalidRechargeOrder(V1MerchantCommand command, InvalidRechargeOrderContext context) {
        if (!context.preparing()) return context.response();
        Optional<V1MerchantProfile> maybeMerchant = merchantProfilePort.findByAppIdAndOrgCode(command.appId(), command.orgCode());
        if (maybeMerchant.isEmpty()) {
            log.warn("V1作废商户资料缺失 appId={}, orgCode={}", command.appId(), command.orgCode());
            return context.finish(V1CardApiResultStatus.AUTH_FAILED.result());
        }

        V1MerchantProfile merchant = maybeMerchant.get();
        V1ProtocolPrecheckResult<V1RechargeOrderInvalidRequest> precheck = precheckService.precheckInvalid(
                command.rawBody(),
                new V1MerchantSecurityProfile(merchant.appId(), merchant.orgCode(), merchant.aesKey(), command.signingCredential()));
        if (!precheck.valid()) {
            return context.finish(protocolFailure(precheck.reason()));
        }
        V1RechargeOrderInvalidRequest request = precheck.payload();
        if (!validRequest(request)) {
            return context.finish(V1CardApiResultStatus.PARAM_ERROR.result());
        }

        return orderQueryPort.findByOrgCodeAndOutOrderSn(merchant.orgCode(), request.outOrderSn())
                .map(snapshot -> toInvalidResult(merchant, request, snapshot, context))
                .orElseGet(() -> context.finish(V1CardApiResultStatus.ORDER_NOT_FOUND.result()));
    }

    private V1CardApiResult toInvalidResult(
            V1MerchantProfile merchant,
            V1RechargeOrderInvalidRequest request,
            RechargeOrderSnapshot snapshot,
            InvalidRechargeOrderContext context) {
        if (!context.beginAdmission(merchant.orgCode(), request.outOrderSn(), pending(snapshot))) return context.response();
        var state = invalidationService.invalidate(new InvalidRechargeOrderOutboxCommand(
                merchant.orgCode(),
                snapshot.orderId(),
                request.outOrderSn(),
                defaultText(request.reason(), "")), prepared -> {
                    if (prepared.status() == RechargeOrderInvalidationState.Status.PENDING
                            || prepared.status() == RechargeOrderInvalidationState.Status.MANUAL_REQUIRED) {
                        context.prepared(toResult(prepared));
                    } else context.finish(toResult(prepared));
                });
        var result = toResult(state);
        if (state.status() == RechargeOrderInvalidationState.Status.PENDING
                || state.status() == RechargeOrderInvalidationState.Status.MANUAL_REQUIRED) {
            context.prepared(result);
            return context.response();
        }
        return context.finish(result);
    }

    @Override
    public Optional<V1CardApiResult> observeInvalidRechargeOrder(InvalidRechargeOrderContext context) {
        var view = context.view();
        if (view.phase() != InvalidRechargeOrderContext.Phase.ADMITTING
                && view.phase() != InvalidRechargeOrderContext.Phase.OBSERVING) return Optional.empty();
        return invalidationService.observe(view.orgCode(), view.outOrderSn())
                .filter(state -> state.status() == RechargeOrderInvalidationState.Status.COMPLETED)
                .map(state -> context.finish(toResult(state)));
    }

    private static V1CardApiResult toResult(RechargeOrderInvalidationState state) {
        return switch (state.status()) {
            case COMPLETED -> V1QueryRechargeOrderUseCaseService.snapshotResult(state.order());
            case PENDING, MANUAL_REQUIRED -> pending(state.order());
            case NOT_ACCEPTED -> V1CardApiResultStatus.BUSINESS_FAILED.result(state.reason());
        };
    }

    private static V1CardApiResult pending(RechargeOrderSnapshot snapshot) {
        if (snapshot == null) return V1CardApiResultStatus.INVALIDATION_PENDING.result();
        var queried = (V1RechargeOrderResponse) V1QueryRechargeOrderUseCaseService.snapshotResult(snapshot).data();
        return V1CardApiResultStatus.INVALIDATION_PENDING.result(new V1RechargeOrderResponse(
                snapshot.orderId(), snapshot.outOrderSn(), queried.orderStatus(), "作废结果待确认"));
    }

    private static boolean validRequest(V1RechargeOrderInvalidRequest request) {
        return request != null
                && StringUtils.hasText(request.outOrderSn())
                && StringUtils.hasText(request.nonceStr())
                && request.nonceStr().length() == 32;
    }

    private static V1CardApiResult protocolFailure(String reason) {
        return switch (reason) {
            case V1ProtocolFailureReason.SIGN_FAILED -> V1CardApiResultStatus.SIGN_FAILED.result("验签不合规！");
            case V1ProtocolFailureReason.DECRYPT_FAILED -> V1CardApiResultStatus.SIGN_FAILED.result();
            case V1ProtocolFailureReason.TIMESTAMP_OUT_OF_RANGE ->
                    V1CardApiResultStatus.PARAM_ERROR.result("timestamp超出允许范围");
            default -> V1CardApiResultStatus.PARAM_ERROR.result();
        };
    }

    private static String defaultText(String value, String fallback) {
        return StringUtils.hasText(value) ? value : fallback;
    }
}
