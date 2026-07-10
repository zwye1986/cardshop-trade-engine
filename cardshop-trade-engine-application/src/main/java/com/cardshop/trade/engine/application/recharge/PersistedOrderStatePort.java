package com.cardshop.trade.engine.application.recharge;

/**
 * MySQL 已落库订单状态确认端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」
 * 的“补投与原始 Consumer 的竞态处理”。Redis pending 补投在重新发布 MQ 前必须做 MySQL 二次确认；
 * application 层只表达“指定订单版本是否已经持久化”，避免 Redis 补投适配器复用面向接口响应的查单 DTO。
 */
@FunctionalInterface
public interface PersistedOrderStatePort {

    /**
     * 判断 MySQL 是否已持久化到 Redis 订单版本。
     *
     * @param orgCode 商户编码
     * @param outOrderSn 商户订单号
     * @param orderId 平台订单号
     * @param orderVersion Redis 订单版本
     * @return true 表示 MySQL 已经存在该订单且 persisted_version 不小于 Redis 订单版本
     */
    boolean isPersistedAtLeast(String orgCode, String outOrderSn, String orderId, long orderVersion);

    /**
     * MySQL 二次确认能力未装配时的空实现；Redis pending 任务会保守继续补投 MQ。
     */
    static PersistedOrderStatePort noop() {
        return (orgCode, outOrderSn, orderId, orderVersion) -> false;
    }
}
