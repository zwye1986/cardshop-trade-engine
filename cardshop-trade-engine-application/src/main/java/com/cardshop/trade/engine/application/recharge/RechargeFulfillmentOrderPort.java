package com.cardshop.trade.engine.application.recharge;

import java.util.Optional;

/**
 * 订单履约写库端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。实现需在同一
 * MySQL 事务中更新 {@code mc_order}、{@code recharge_order_ext} 和供应商流水，确保状态可追溯。
 */
public interface RechargeFulfillmentOrderPort {

    Optional<RechargeFulfillmentOrder> findByOrderId(String orderId);

    /**
     * 读取同订单最近一条供应商调用流水。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。DISPATCH 恢复时如果已存在
     * {@code mc_order_supplier_flow}，说明请求已经可能发出；后续动作必须先查单确认，不能再盲目派单。
     */
    Optional<SupplierFlowSnapshot> findLatestSupplierFlowByOrderId(String orderId);

    long insertSupplierFlowStart(SupplierFlowStartCommand command);

    void updateSupplierFlow(SupplierFlowUpdateCommand command);

    void updateFulfillmentStatus(OrderFulfillmentStatusUpdate update);

    /**
     * 读取MINISO持久释放许可；只有三字段全空才返回empty，损坏的部分授权必须失败关闭。
     * 参见 {@code docs/交行直充业务系统详细设计.md}「9.8」。非MINISO实现不提供资金许可。
     */
    default Optional<MinisoBalanceReleaseAuthorization> findMinisoReleaseAuthorization(String orderId) {
        throw new UnsupportedOperationException("MINISO release authorization requires persistent adapter");
    }

    /**
     * 在同一主事务中裁决MINISO终态及首次释放许可；empty仅表示可信订单原本未预扣。
     * 参见 {@code docs/交行直充业务系统详细设计.md}「9.8」。已有许可返回首次冻结值，不重新裁决。
     */
    default Optional<MinisoBalanceReleaseAuthorization> authorizeMinisoTerminalRelease(
            OrderFulfillmentStatusUpdate update, String sourceEventId) {
        throw new UnsupportedOperationException("MINISO release authorization requires persistent adapter");
    }

    /**
     * Worker确认Redis RELEASED/ALREADY_RELEASED后调用；同主事务核对RELEASE delta并置释放标记。
     * 完成标记不表示delta已刷入旧余额；重放仍须验证既有delta。
     * 参见 {@code docs/交行直充业务系统详细设计.md}「9.8」。
     */
    default void completeMinisoBalanceRelease(MinisoBalanceReleaseAuthorization authorization) {
        throw new UnsupportedOperationException("MINISO release completion requires persistent adapter");
    }

    /**
     * 标记 MySQL 扩展订单中的 Redis 预扣已经释放。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「14.6 订单最终失败/作废退款协议」。
     * 该动作必须在 Redis release 成功且反向 {@code BALANCE_DELTA_FLUSH} outbox 已写入后执行；
     * 若提前置位，后续 outbox 重试会失去继续补资金动作的判断依据。
     * MINISO必须改用{@link #completeMinisoBalanceRelease}，不能走此分步入口（详细设计9.8）。
     */
    void markBalanceReleased(String orderId);
}
