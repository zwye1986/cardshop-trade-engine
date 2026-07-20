package com.cardshop.trade.engine.application.recharge;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * MySQL 已落库订单状态确认端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」
 * 的“补投与原始 Consumer 的竞态处理”以及「8.3 异步落库 Consumer」。Redis pending 补投在重新发布 MQ 前、
 * 批 Consumer 在写库前都必须做 MySQL 二次确认；application 层只表达“指定订单版本是否已经持久化”，
 * 避免 Redis 补投适配器或 Consumer 复用面向接口响应的查单 DTO。
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
     * 批量判断 MySQL 是否已覆盖指定订单版本。
     *
     * <p>参见详细设计「8.3 异步落库 Consumer」。批消费高频路径必须优先使用基础设施层批查询，避免一批内逐条
     * 查库抵消批写收益；默认实现仅作为兼容兜底，真实 MyBatis adapter 会覆盖为单次 `order_id in (...)` 查询。
     *
     * @param probes 订单版本探针集合
     * @return 已经确认落库到目标版本的探针集合
     */
    default Set<PersistedOrderProbe> findPersistedAtLeast(Collection<PersistedOrderProbe> probes) {
        Objects.requireNonNull(probes, "probes must not be null");
        Set<PersistedOrderProbe> persisted = new HashSet<>();
        for (PersistedOrderProbe probe : probes) {
            Objects.requireNonNull(probe, "probes must not contain null");
            if (isPersistedAtLeast(
                    probe.orgCode(),
                    probe.outOrderSn(),
                    probe.orderId(),
                    probe.orderVersion())) {
                persisted.add(probe);
            }
        }
        return persisted;
    }

    /**
     * MySQL 二次确认能力未装配时的空实现；Redis pending 任务会保守继续补投 MQ。
     */
    static PersistedOrderStatePort noop() {
        return (orgCode, outOrderSn, orderId, orderVersion) -> false;
    }
}
