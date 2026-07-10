package com.cardshop.trade.engine.api.v1.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

/**
 * 交行 V1 查单请求 DTO。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * 查单后续实现必须 Redis 优先、MySQL 兜底，且不得同步调用供应商修改订单状态。
 *
 * @param outOrderSn 商户订单号
 * @param nonceStr 防重放随机串
 * @param timestamp 请求时间戳
 * @param sign 请求签名
 */
public record V1RechargeOrderQueryRequest(
        @JsonProperty("out_order_sn") @JsonAlias("outOrderSn") @NotBlank String outOrderSn,
        @JsonProperty("nonce_str") @JsonAlias("nonceStr") @NotBlank String nonceStr,
        @NotBlank String timestamp,
        @NotBlank String sign) {
}
