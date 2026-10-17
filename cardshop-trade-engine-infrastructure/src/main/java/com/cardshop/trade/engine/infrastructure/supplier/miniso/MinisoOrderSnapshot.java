package com.cardshop.trade.engine.infrastructure.supplier.miniso;

/** 可信已落库订单的名创请求快照；参见详细设计名创MINISO实施约束。批次来自扩展订单，不读取实时商品配置。 */
public record MinisoOrderSnapshot(String orgCode, String orderId, String account, String batchId, int num) {
    @Override public String toString() { return "MinisoOrderSnapshot[orderId=" + orderId + "]"; }
}
