package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.RechargeOrderIdentityConflictException;
import com.cardshop.trade.engine.domain.order.PersistStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 单条、批量、独立writer及跳过路径共用的事实身份裁决，不负责开启事务。
 * 批量每张表一次读取，调用者保持主单→扩展锁序；参见详细设计8.2.1。
 */
final class RechargeOrderIdentityVerifier {
    private final McOrderMapper orders;
    private final RechargeOrderExtMapper extensions;

    RechargeOrderIdentityVerifier(McOrderMapper orders, RechargeOrderExtMapper extensions) {
        this.orders = Objects.requireNonNull(orders);
        this.extensions = Objects.requireNonNull(extensions);
    }

    Map<String, McOrderDO> readOrders(List<OrderPersistAggregate> aggregates) {
        return orders.selectPersistIdentities(aggregates.stream().map(a -> a.snapshot().orderId()).distinct().toList())
                .stream().collect(Collectors.toMap(McOrderDO::getOrderId, Function.identity()));
    }

    void verifyOrders(List<OrderPersistAggregate> aggregates) {
        var rows = readOrders(aggregates);
        for (var aggregate : aggregates) {
            requireOrder(aggregate.snapshot(), rows.get(aggregate.snapshot().orderId()));
        }
    }

    void verifyExtensions(List<OrderPersistAggregate> aggregates) {
        verifyExtensions(aggregates, true);
    }

    void verifyExistingExtensions(List<OrderPersistAggregate> aggregates) {
        verifyExtensions(aggregates, false);
    }

    private void verifyExtensions(List<OrderPersistAggregate> aggregates, boolean requireVersion) {
        if (aggregates.isEmpty()) {
            return;
        }
        var rows = extensions.selectByOrderIds(aggregates.stream().map(a -> a.snapshot().orderId()).distinct().toList())
                .stream().collect(Collectors.toMap(RechargeOrderExtDO::getOrderId, Function.identity()));
        for (var aggregate : aggregates) {
            var snapshot = aggregate.snapshot();
            requireExtension(snapshot, rows.get(snapshot.orderId()), requireVersion);
        }
    }

    void verifyCommitted(List<OrderPersistAggregate> aggregates) {
        verifyOrders(aggregates);
        verifyExtensions(aggregates);
    }

    static void requireOrder(PersistOrderSnapshot expected, McOrderDO actual) {
        if (actual == null || !Objects.equals(expected.orderId(), actual.getOrderId())
                || !Objects.equals(expected.orgCode(), actual.getOrgCode())
                || !Objects.equals(expected.outOrderSn(), actual.getOutOrderId())
                || !Objects.equals(McOrderDO.TRADE_ENGINE_OWNED, actual.getTradeEngineOwned())
                || !Objects.equals(McOrderDO.DIRECT_RECHARGE_PRODUCT_TYPE, actual.getProductType())
                || expected.productType() != McOrderDO.DIRECT_RECHARGE_PRODUCT_TYPE
                || !Objects.equals(expected.orderPrice(), actual.getOrderPrice())) {
            throw conflict(expected.orderId());
        }
    }

    static void requireExtension(PersistOrderSnapshot expected, RechargeOrderExtDO actual) {
        requireExtension(expected, actual, true);
    }

    private static void requireExtension(PersistOrderSnapshot expected, RechargeOrderExtDO actual,
            boolean requireVersion) {
        if (actual == null || !Objects.equals(expected.orderId(), actual.getOrderId())
                || !Objects.equals(expected.orgCode(), actual.getOrgCode())
                || !Objects.equals(expected.outOrderSn(), actual.getOutOrderSn())
                || !Objects.equals(expected.requestHash(), actual.getRequestHash())
                || !Objects.equals(expected.requestHashVersion(), actual.getRequestHashVersion())
                || !Objects.equals(expected.requestHashAlg(), actual.getRequestHashAlg())
                || (requireVersion && (!PersistStatus.DB_PERSISTED.name().equals(actual.getPersistStatus())
                    || actual.getPersistedVersion() == null || actual.getPersistedVersion() < expected.orderVersion()))) {
            throw conflict(expected.orderId());
        }
    }

    static RechargeOrderIdentityConflictException conflict(String orderId) {
        return new RechargeOrderIdentityConflictException("持久订单身份冲突 orderId=" + orderId);
    }
}
