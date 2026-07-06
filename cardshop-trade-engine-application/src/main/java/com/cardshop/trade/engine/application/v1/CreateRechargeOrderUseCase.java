package com.cardshop.trade.engine.application.v1;

import java.util.concurrent.CompletionStage;

/**
 * 交行 V1 创建直充订单用例。
 *
 * <p>返回 {@link CompletionStage} 以避免 Redis 接单调用阻塞 WebFlux 事件循环线程。
 */
public interface CreateRechargeOrderUseCase {

    CompletionStage<V1CardApiResult> createRechargeOrder(V1MerchantCommand command);
}
