package com.cardshop.trade.engine.application.v1;

/**
 * V1 查单/作废使用的订单快照。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「14.5 Redis/MQ/MySQL 三方幂等协议」。
 * 该对象只承载接口响应和作废前置判断所需的最小字段，避免 API 层直接理解 Redis hash 或 MySQL 表结构。
 *
 * @param orderId 平台订单号
 * @param outOrderSn 商户订单号
 * @param orderStatus 订单业务状态
 * @param persistStatus Redis/MySQL 持久化状态
 * @param validationStatus 下单业务校验状态
 * @param failCode 失败码
 * @param failReason 失败原因
 * @param dispatchAllowed 是否允许后续派单
 * @param stateType 券状态类型（名创3，其他直充2），参见详细设计9.6
 * @param useState 交行券状态
 * @param useTime 核销时间
 * @param invalidTime 作废完成时间
 */
public record RechargeOrderSnapshot(
        String orderId,
        String outOrderSn,
        String orderStatus,
        String persistStatus,
        String validationStatus,
        String failCode,
        String failReason,
        boolean dispatchAllowed,
        String stateType,
        String useState,
        String useTime,
        String invalidTime) {

    /** 受理期尚无券事实，空字段不能被当作作废或核销；参见详细设计9.6。 */
    public RechargeOrderSnapshot(String orderId, String outOrderSn, String orderStatus,
            String persistStatus, String validationStatus, String failCode, String failReason,
            boolean dispatchAllowed) {
        this(orderId, outOrderSn, orderStatus, persistStatus, validationStatus, failCode, failReason,
                dispatchAllowed, "", "", "", "");
    }
}
