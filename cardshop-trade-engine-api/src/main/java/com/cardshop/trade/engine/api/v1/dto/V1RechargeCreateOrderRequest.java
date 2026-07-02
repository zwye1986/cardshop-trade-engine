package com.cardshop.trade.engine.api.v1.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * 交行 V1 创建直充订单请求 DTO。
 *
 * <p>字段来源对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.2 V1 请求 DTO 和字段来源」。
 * `orgCode` 只作为兼容字段承载，实际路由和商户身份必须以后续 token 解析出的 `app_id/orgCode` 为准。
 *
 * @param orgCode 商户编码，兼容旧协议字段
 * @param outOrderSn 商户订单号，对应旧表 `mc_order.out_orderId`
 * @param productId 子产品编码，对应旧表 `mc_order.produt_code`
 * @param parentId 父产品编码，对应旧表 `mc_order.parent_id`
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
        String orgCode,
        @NotBlank String outOrderSn,
        @NotBlank String productId,
        String parentId,
        @NotBlank String account,
        @NotNull @Min(1) Integer num,
        @NotNull @Min(0) Long orderPrice,
        @NotNull Integer productType,
        String notifyUrl,
        @NotBlank String nonceStr,
        @NotBlank String timestamp,
        @NotBlank String sign) {
}
