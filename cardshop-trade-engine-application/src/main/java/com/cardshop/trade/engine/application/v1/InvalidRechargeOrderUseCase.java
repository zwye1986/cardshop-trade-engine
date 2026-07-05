package com.cardshop.trade.engine.application.v1;

/**
 * 交行 V1 作废直充订单用例。
 */
public interface InvalidRechargeOrderUseCase {

    V1CardApiResult invalidRechargeOrder(V1MerchantCommand command);
}
