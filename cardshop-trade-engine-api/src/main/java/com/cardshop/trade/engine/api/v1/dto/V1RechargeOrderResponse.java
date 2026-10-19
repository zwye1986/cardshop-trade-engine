package com.cardshop.trade.engine.api.v1.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 交行 V1 订单响应 DTO。
 *
 * <p>字段含义对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * `orderStatus` 必须是旧接口数字状态，不能直接暴露 Redis 内部持久化状态。
 *
 * @param orderId 平台订单号，对应旧表 `mc_order.order_Id`
 * @param outOrderSn 商户订单号，对应旧表 `mc_order.out_orderId`
 * @param orderStatus 旧接口数字订单状态
 * @param orderStatusDesc 订单状态说明
 * @param stateType 券状态类型，名创3、其他直充2；参见详细设计9.6
 * @param useState 交行券状态
 * @param useTime 核销时间
 * @param invalidTime 作废完成时间
 */
public record V1RechargeOrderResponse(
        @JsonProperty("order_id") String orderId,
        @JsonProperty("out_order_sn") String outOrderSn,
        @JsonProperty("order_status") String orderStatus,
        @JsonProperty("order_status_desc") String orderStatusDesc,
        @JsonProperty("state_type") String stateType,
        @JsonProperty("use_state") String useState,
        @JsonProperty("use_time") String useTime,
        @JsonProperty("invalid_time") String invalidTime) {
    public V1RechargeOrderResponse(String orderId, String outOrderSn, String orderStatus,
            String orderStatusDesc) {
        this(orderId, outOrderSn, orderStatus, orderStatusDesc, "", "", "", "");
    }
}
