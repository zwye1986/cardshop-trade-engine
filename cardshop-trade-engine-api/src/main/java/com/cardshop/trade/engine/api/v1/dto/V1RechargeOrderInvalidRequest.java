package com.cardshop.trade.engine.api.v1.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

/**
 * 交行 V1 作废订单请求 DTO。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「9.2 作废设计」。
 * 未落 MySQL 或 `persist_status != DB_PERSISTED` 的订单后续实现必须返回处理中，不直接调用供应商。
 *
 * @param outOrderSn 商户订单号
 * @param reason 作废原因
 * @param nonceStr 防重放随机串
 * @param timestamp 请求时间戳
 * @param sign 请求签名
 */
public record V1RechargeOrderInvalidRequest(
        @JsonProperty("out_order_sn") @JsonAlias("outOrderSn") @NotBlank String outOrderSn,
        String reason,
        @JsonProperty("nonce_str") @JsonAlias("nonceStr") @NotBlank String nonceStr,
        @NotBlank String timestamp,
        @NotBlank String sign) {
}
