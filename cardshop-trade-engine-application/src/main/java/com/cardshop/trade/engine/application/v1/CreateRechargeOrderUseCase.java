package com.cardshop.trade.engine.application.v1;

/**
 * 交行 V1 创建直充订单用例。
 */
public interface CreateRechargeOrderUseCase {

    V1CardApiResult createRechargeOrder(V1MerchantCommand command);
}
