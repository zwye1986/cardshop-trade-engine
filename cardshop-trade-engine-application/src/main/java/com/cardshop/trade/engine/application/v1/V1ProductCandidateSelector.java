package com.cardshop.trade.engine.application.v1;

/**
 * V1 直充商品候选选择端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 * `/v1/cardapi/order/createRechargeOrder` 的 {@code product_id} 沿用旧卡管父产品号语义：先按
 * {@code mc_depart_product.parent_id} 读取商户授权候选，再冻结一个实际子商品作为价格快照和供应商派单目标。
 */
@FunctionalInterface
public interface V1ProductCandidateSelector {

    /**
     * 按商户、请求父产品和充值账号选择实际子商品。
     */
    V1ProductSelection select(String orgCode, String productId, String account);
}
