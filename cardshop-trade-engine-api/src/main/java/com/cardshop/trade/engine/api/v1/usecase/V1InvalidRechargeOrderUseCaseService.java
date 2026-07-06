package com.cardshop.trade.engine.api.v1.usecase;

import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderInvalidRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderResponse;
import com.cardshop.trade.engine.api.v1.protocol.V1MerchantSecurityProfile;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckResult;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckService;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderUseCase;
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
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.11 作废设计」。D4 只完成入口闭环和
 * 未落库保护：订单仍处于 Redis pending/MQ/Consumer 窗口时返回处理中，不直接调用供应商或修改旧库。
 * 真正的 DB_PERSISTED 作废 outbox 会在 D10 作废任务中接入。
 */
public class V1InvalidRechargeOrderUseCaseService implements InvalidRechargeOrderUseCase {

    private static final Logger log = LoggerFactory.getLogger(V1InvalidRechargeOrderUseCaseService.class);

    private final V1ProtocolPrecheckService precheckService;
    private final V1MerchantProfilePort merchantProfilePort;
    private final RechargeOrderQueryPort orderQueryPort;

    public V1InvalidRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            V1MerchantProfilePort merchantProfilePort,
            RechargeOrderQueryPort orderQueryPort) {
        this.precheckService = precheckService;
        this.merchantProfilePort = merchantProfilePort;
        this.orderQueryPort = orderQueryPort;
    }

    @Override
    public V1CardApiResult invalidRechargeOrder(V1MerchantCommand command) {
        Optional<V1MerchantProfile> maybeMerchant = merchantProfilePort.findByAppIdAndOrgCode(command.appId(), command.orgCode());
        if (maybeMerchant.isEmpty() || !StringUtils.hasText(maybeMerchant.get().aesKey())) {
            log.warn("V1作废商户资料缺失 appId={}, orgCode={}", command.appId(), command.orgCode());
            return V1CardApiResultStatus.AUTH_FAILED.result();
        }

        V1MerchantProfile merchant = maybeMerchant.get();
        V1ProtocolPrecheckResult<V1RechargeOrderInvalidRequest> precheck = precheckService.precheckInvalid(
                command.rawBody(),
                new V1MerchantSecurityProfile(merchant.appId(), merchant.orgCode(), merchant.aesKey()));
        if (!precheck.valid()) {
            return protocolFailure(precheck.reason());
        }
        V1RechargeOrderInvalidRequest request = precheck.payload();
        if (!validRequest(request)) {
            return V1CardApiResultStatus.PARAM_ERROR.result();
        }

        return orderQueryPort.findByOrgCodeAndOutOrderSn(merchant.orgCode(), request.outOrderSn())
                .map(this::toInvalidResult)
                .orElseGet(V1CardApiResultStatus.ORDER_NOT_FOUND::result);
    }

    private V1CardApiResult toInvalidResult(RechargeOrderSnapshot snapshot) {
        if (!"DB_PERSISTED".equals(defaultText(snapshot.persistStatus(), ""))) {
            return processing(snapshot);
        }
        // TODO(D10): DB_PERSISTED 订单应写入 INVALID_ORDER outbox 并由供应商作废 Worker 处理；参见 docs/交行直充业务系统详细设计.md「20.13.11 作废设计」。
        return processing(snapshot);
    }

    private static V1CardApiResult processing(RechargeOrderSnapshot snapshot) {
        return V1CardApiResultStatus.SUCCESS.result(new V1RechargeOrderResponse(
                snapshot.orderId(),
                snapshot.outOrderSn(),
                "2",
                "处理中"));
    }

    private static boolean validRequest(V1RechargeOrderInvalidRequest request) {
        return request != null
                && StringUtils.hasText(request.outOrderSn())
                && StringUtils.hasText(request.nonceStr())
                && request.nonceStr().length() == 32;
    }

    private static V1CardApiResult protocolFailure(String reason) {
        return switch (reason) {
            case "SIGN_FAILED" -> V1CardApiResultStatus.SIGN_FAILED.result("验签不合规！");
            case "DECRYPT_FAILED" -> V1CardApiResultStatus.SIGN_FAILED.result();
            case "TIMESTAMP_OUT_OF_RANGE" -> V1CardApiResultStatus.PARAM_ERROR.result("timestamp超出允许范围");
            default -> V1CardApiResultStatus.PARAM_ERROR.result();
        };
    }

    private static String defaultText(String value, String fallback) {
        return StringUtils.hasText(value) ? value : fallback;
    }
}
