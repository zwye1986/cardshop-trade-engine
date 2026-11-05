package com.cardshop.trade.engine.application.v1;

/**
 * 交行 V1 作废直充订单用例。
 */
public interface InvalidRechargeOrderUseCase {

    V1CardApiResult invalidRechargeOrder(V1MerchantCommand command, InvalidRechargeOrderContext context);

    default V1CardApiResult invalidRechargeOrder(V1MerchantCommand command) {
        return invalidRechargeOrder(command, new InvalidRechargeOrderContext(InvalidRechargeOrderContext.DEFAULT_WAIT_TIMEOUT));
    }

    /** 只观察已验签且已开始受理的同一订单；仅可信完成可提前结束响应。参见详细设计9.3.4。 */
    default java.util.Optional<V1CardApiResult> observeInvalidRechargeOrder(InvalidRechargeOrderContext context) {
        return java.util.Optional.empty();
    }
}
