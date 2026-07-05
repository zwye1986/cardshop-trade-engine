package com.cardshop.trade.engine.api.v1.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 交行 V1 订单响应 DTO。
 *
 * <p>字段含义对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.10 查单设计」。
 * `orderStatus` 必须是旧接口数字状态，不能直接暴露 Redis 内部持久化状态。
 *
 * @param orderId 平台订单号，对应旧表 `mc_order.order_Id`
 * @param outOrderSn 商户订单号，对应旧表 `mc_order.out_orderId`
 * @param orderStatus 旧接口数字订单状态
 * @param orderStatusDesc 订单状态说明
 */
public record V1RechargeOrderResponse(
        @JsonProperty("order_id") String orderId,
        @JsonProperty("out_order_sn") String outOrderSn,
        @JsonProperty("order_status") String orderStatus,
        @JsonProperty("order_status_desc") String orderStatusDesc) {
}
