package com.cardshop.trade.engine.infrastructure.supplier.miniso;

import java.util.Optional;

/** 读取新系统已落库订单，保持名创协议与SQL隔离；参见详细设计名创实施约束。 */
@FunctionalInterface
public interface MinisoOrderSnapshotReader {
    Optional<MinisoOrderSnapshot> find(String orgCode, String orderId);

    /** 作废前读取持久凭据核对券码，缺失时保守拒绝；参见详细设计名创作废审查补充。 */
    default Optional<com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt> localReceipt(String orgCode, String orderId) {
        return Optional.empty();
    }
}
