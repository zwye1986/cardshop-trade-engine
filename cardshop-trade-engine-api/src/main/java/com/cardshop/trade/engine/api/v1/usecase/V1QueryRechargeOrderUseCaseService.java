package com.cardshop.trade.engine.api.v1.usecase;

import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderQueryRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderResponse;
import com.cardshop.trade.engine.api.v1.protocol.V1MerchantSecurityProfile;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckResult;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckService;
import com.cardshop.trade.engine.application.v1.QueryRechargeOrderUseCase;
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
 * V1 查单用例。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.10 查单设计」。D4 阶段先实现 Redis
 * 快照优先查询，未落库或 MQ/Consumer 未追平的订单统一映射为旧接口“处理中”，且查单不触发供应商状态修改。
 */
public class V1QueryRechargeOrderUseCaseService implements QueryRechargeOrderUseCase {

    private static final Logger log = LoggerFactory.getLogger(V1QueryRechargeOrderUseCaseService.class);

    private final V1ProtocolPrecheckService precheckService;
    private final V1MerchantProfilePort merchantProfilePort;
    private final RechargeOrderQueryPort orderQueryPort;

    public V1QueryRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            V1MerchantProfilePort merchantProfilePort,
            RechargeOrderQueryPort orderQueryPort) {
        this.precheckService = precheckService;
        this.merchantProfilePort = merchantProfilePort;
        this.orderQueryPort = orderQueryPort;
    }

    @Override
    public V1CardApiResult queryRechargeOrder(V1MerchantCommand command) {
        Optional<V1MerchantProfile> maybeMerchant = merchantProfilePort.findByAppIdAndOrgCode(command.appId(), command.orgCode());
        if (maybeMerchant.isEmpty() || !StringUtils.hasText(maybeMerchant.get().aesKey())) {
            log.warn("V1查单商户资料缺失 appId={}, orgCode={}", command.appId(), command.orgCode());
            return V1CardApiResultStatus.AUTH_FAILED.result();
        }

        V1MerchantProfile merchant = maybeMerchant.get();
        V1ProtocolPrecheckResult<V1RechargeOrderQueryRequest> precheck = precheckService.precheckQuery(
                command.rawBody(),
                new V1MerchantSecurityProfile(merchant.appId(), merchant.orgCode(), merchant.aesKey()));
        if (!precheck.valid()) {
            return protocolFailure(precheck.reason());
        }
        V1RechargeOrderQueryRequest request = precheck.payload();
        if (!validRequest(request)) {
            return V1CardApiResultStatus.PARAM_ERROR.result();
        }

        return orderQueryPort.findByOrgCodeAndOutOrderSn(merchant.orgCode(), request.outOrderSn())
                .map(V1QueryRechargeOrderUseCaseService::snapshotResult)
                .orElseGet(V1CardApiResultStatus.ORDER_NOT_FOUND::result);
    }

    static V1CardApiResult snapshotResult(RechargeOrderSnapshot snapshot) {
        OrderView view = toOrderView(snapshot);
        return V1CardApiResultStatus.SUCCESS.result(new V1RechargeOrderResponse(
                snapshot.orderId(),
                snapshot.outOrderSn(),
                view.orderStatus(),
                view.orderStatusDesc()));
    }

    private static OrderView toOrderView(RechargeOrderSnapshot snapshot) {
        String orderStatus = defaultText(snapshot.orderStatus(), "");
        String persistStatus = defaultText(snapshot.persistStatus(), "");
        String validationStatus = defaultText(snapshot.validationStatus(), "");
        /*
         * 查单响应必须使用 V1.1 接口文档「1.4.2 订单状态码」和详细设计「20.13.10 查单设计」中的
         * order_status 码表。这里特别避免把旧后台推送状态或核销 use_state 的 3 当成订单失败：
         * 失败订单对外是 6，核销作废才使用 use_state=3。
         */
        if ("FAILED".equals(validationStatus)
                || "VALIDATION_FAILED".equals(orderStatus)
                || "ACCEPT_REJECTED".equals(orderStatus)) {
            return new OrderView("6", defaultText(snapshot.failReason(), "订单失败"));
        }
        if ("SUCCESS".equals(orderStatus)) {
            return new OrderView("12", "成功");
        }
        if ("SUPPLIER_ACCEPTED".equals(orderStatus)) {
            return new OrderView("13", "充值中");
        }
        if ("REFUNDED".equals(orderStatus)) {
            return new OrderView("11", "已退款");
        }
        if ("FAILED".equals(orderStatus) || "SUPPLIER_FAILED".equals(orderStatus)) {
            return new OrderView("6", defaultText(snapshot.failReason(), "订单失败"));
        }
        if (!"DB_PERSISTED".equals(persistStatus)
                || "ACCEPTED".equals(orderStatus)
                || "PERSISTING".equals(orderStatus)
                || "WAIT".equals(orderStatus)
                || "WAIT_DISPATCH".equals(orderStatus)
                || "FREQUENCY_WAIT".equals(orderStatus)
                || "DISPATCHING".equals(orderStatus)) {
            return new OrderView("2", "处理中");
        }
        /*
         * 对未知内部态保持保守处理中，避免查单接口越权推进供应商查询或把未识别状态误报为终态。
         * 后续若新增内部状态，应在详细设计「20.13.10 查单设计」状态映射表先补充，再扩展这里。
         */
        return new OrderView("2", "处理中");
    }

    private static boolean validRequest(V1RechargeOrderQueryRequest request) {
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

    private record OrderView(String orderStatus, String orderStatusDesc) {
    }
}
