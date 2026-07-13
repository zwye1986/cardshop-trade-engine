package com.cardshop.trade.engine.application.v1;

/**
 * V1 直充商品候选选择端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 * `/v1/cardapi/order/createRechargeOrder` 接口文档将 {@code product_id} 定义为“平台提供的产品编号”；
 * 新链路必须严格按这个编号匹配商户授权、商品配置、价格快照和供应商派单目标，不能再按旧卡管父商品
 * 语义在同父商品下另选子商品。
 */
@FunctionalInterface
public interface V1ProductCandidateSelector {

    /**
     * 按商户、请求商品和充值账号选择候选商品。
     */
    V1ProductSelection select(String orgCode, String productId, String account);
}
