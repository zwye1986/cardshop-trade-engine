package com.cardshop.trade.engine.api.v1.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * 交行 V1 创建直充订单请求 DTO。
 *
 * <p>字段来源对应 {@code docs/交行直充业务系统详细设计.md} 的「14.1 V1 DTO 字段」。
 * `orgCode` 只作为兼容字段承载，实际路由和商户身份必须以后续 token 解析出的 `app_id/orgCode` 为准。
 *
 * @param orgCode 商户编码，兼容旧协议字段
 * @param outOrderSn 商户订单号，对应旧表 `mc_order.out_orderId`
 * @param productId 产品编号（平台提供），对应旧表 `mc_order.parent_id`，新链路按该值精确匹配商品
 * @param parentId 旧协议兼容字段，新链路不信任调用方传入的父子关系，也不参与商品匹配
 * @param account 充值账号
 * @param num 购买数量，首期直充只允许 1
 * @param orderPrice 订单金额，单位分
 * @param productType 商品类型，首期只处理直充 `1`
 * @param notifyUrl 商户回调地址
 * @param nonceStr 防重放随机串
 * @param timestamp 请求时间戳
 * @param sign 请求签名
 */
public record V1RechargeCreateOrderRequest(
        @JsonProperty("org_code") @JsonAlias("orgCode") String orgCode,
        @JsonProperty("out_order_sn") @JsonAlias("outOrderSn") @NotBlank String outOrderSn,
        @JsonProperty("product_id") @JsonAlias("productId") @NotBlank String productId,
        @JsonProperty("parent_id") @JsonAlias("parentId") String parentId,
        @NotBlank String account,
        @NotNull @Min(1) Integer num,
        @JsonProperty("order_price") @JsonAlias("orderPrice") @NotNull @Min(0) Long orderPrice,
        @JsonProperty("product_type") @JsonAlias("productType") @NotNull Integer productType,
        @JsonProperty("notify_url") @JsonAlias("notifyUrl") String notifyUrl,
        @JsonProperty("nonce_str") @JsonAlias("nonceStr") @NotBlank String nonceStr,
        @NotBlank String timestamp,
        @NotBlank String sign) {
}
