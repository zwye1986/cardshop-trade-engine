package com.cardshop.trade.engine.application.recharge;

import java.util.Optional;

/**
 * 订单履约写库端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」。实现需在同一
 * MySQL 事务中更新 {@code mc_order}、{@code recharge_order_ext} 和供应商流水，确保状态可追溯。
 */
public interface RechargeFulfillmentOrderPort {

    Optional<RechargeFulfillmentOrder> findByOrderId(String orderId);

    long insertSupplierFlowStart(SupplierFlowStartCommand command);

    void updateSupplierFlow(SupplierFlowUpdateCommand command);

    void updateFulfillmentStatus(OrderFulfillmentStatusUpdate update);
}
