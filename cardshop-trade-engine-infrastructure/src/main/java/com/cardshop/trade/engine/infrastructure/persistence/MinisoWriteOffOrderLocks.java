package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoWriteOffEventMapper.OrderFacts;

import java.time.LocalDateTime;
import java.util.Objects;

/** 主单/扩展/凭据/券码guard的统一锁序与归属复核；参见docs/交行直充业务系统详细设计.md「9.8」。 */
final class MinisoWriteOffOrderLocks {
    private final MinisoWriteOffEventMapper events;
    private final MinisoReceiptMapper receipts;
    private final MinisoCouponGuardMapper guards;

    MinisoWriteOffOrderLocks(MinisoWriteOffEventMapper events, MinisoReceiptMapper receipts,
            MinisoCouponGuardMapper guards) {
        this.events = events;
        this.receipts = receipts;
        this.guards = guards;
    }

    Owned lock(String id, LocalDateTime now) {
        return lock(id, null, now);
    }

    /**
     * 候选只定位订单；锁后券码已变时不锁另一券的guard，也不绑定原候选订单。
     * 接收方只能继续按原通知券码锁guard及事件，不在持guard后寻找其它主单，参见详细设计9.8。
     */
    Owned lock(String id, String expectedCoupon, LocalDateTime now) {
        var order = events.lockOrder(id);
        var ext = receipts.lockFrozen(id);
        var receipt = receipts.lockReceipt(id);
        if (order == null
                || ext == null
                || receipt == null
                || (expectedCoupon != null && !expectedCoupon.equals(receipt.couponCode()))
                || !"MINISO".equals(order.channel())
                || order.num() != 1
                || !"DB_PERSISTED".equals(ext.persistStatus())
                || !Objects.equals(order.orgCode(), ext.orgCode())
                || !Objects.equals(order.outOrderSn(), ext.outOrderSn())
                || !Objects.equals(order.orderId(), ext.orderId())
                || !Objects.equals(receipt.orgCode(), order.orgCode())
                || !Objects.equals(receipt.phone(), order.account())
                || !sameBatch(receipt.batchId(), ext.batchId())) return null;
        guards.lockInTransaction(receipt.couponCode(), now);
        return new Owned(order, receipt);
    }

    private static boolean sameBatch(int batch, String frozen) {
        return frozen != null
                && frozen.length() <= 10
                && frozen.matches("[0-9]+")
                && Long.parseLong(frozen) == batch;
    }

    record Owned(OrderFacts order, MinisoFulfillmentReceipt receipt) {}
}
