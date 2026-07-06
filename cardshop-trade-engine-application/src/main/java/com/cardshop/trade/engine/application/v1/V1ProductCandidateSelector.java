package com.cardshop.trade.engine.application.v1;

/**
 * V1 直充商品候选选择端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.5 商品选择规则」。
 * 调用方传入的 {@code product_id} 是父商品编码，选择器负责按旧卡管商品权限关系找到实际子商品。
 */
@FunctionalInterface
public interface V1ProductCandidateSelector {

    /**
     * 按商户、父商品和充值账号选择候选商品。
     */
    V1ProductSelection select(String orgCode, String parentProductId, String account);
}
