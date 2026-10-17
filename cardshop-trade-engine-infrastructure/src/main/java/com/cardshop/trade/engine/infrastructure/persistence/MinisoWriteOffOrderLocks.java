package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoWriteOffEventMapper.OrderFacts;

import java.util.Objects;

/** 主单/扩展/凭据的统一锁序与归属复核；参见docs/交行直充业务系统详细设计.md「9.5」。 */
final class MinisoWriteOffOrderLocks {
    private final MinisoWriteOffEventMapper events;
    private final MinisoReceiptMapper receipts;

    MinisoWriteOffOrderLocks(MinisoWriteOffEventMapper events, MinisoReceiptMapper receipts) {
        this.events = events;
        this.receipts = receipts;
    }

    Owned lock(String id) {
        var order = events.lockOrder(id);
        var ext = receipts.lockFrozen(id);
        var receipt = receipts.lockReceipt(id);
        if (order == null
                || ext == null
                || receipt == null
                || !"MINISO".equals(order.channel())
                || order.num() != 1
                || !"DB_PERSISTED".equals(ext.persistStatus())
                || !Objects.equals(order.orgCode(), ext.orgCode())
                || !Objects.equals(order.outOrderSn(), ext.outOrderSn())
                || !Objects.equals(order.orderId(), ext.orderId())
                || !Objects.equals(receipt.orgCode(), order.orgCode())
                || !Objects.equals(receipt.phone(), order.account())
                || !sameBatch(receipt.batchId(), ext.batchId())) return null;
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
