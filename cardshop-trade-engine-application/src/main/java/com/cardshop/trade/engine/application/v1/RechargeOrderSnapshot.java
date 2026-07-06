package com.cardshop.trade.engine.application.v1;

/**
 * V1 查单/作废使用的订单快照。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.10 查单设计」和
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
 */
public record RechargeOrderSnapshot(
        String orderId,
        String outOrderSn,
        String orderStatus,
        String persistStatus,
        String validationStatus,
        String failCode,
        String failReason,
        boolean dispatchAllowed) {
}
