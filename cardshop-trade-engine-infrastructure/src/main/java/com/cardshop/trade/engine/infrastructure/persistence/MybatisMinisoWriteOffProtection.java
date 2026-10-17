package com.cardshop.trade.engine.infrastructure.persistence;

import static com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason.*;

import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;

import org.springframework.transaction.*;
import org.springframework.transaction.support.*;

import java.time.*;

/** 永久作废/退款保护，不依赖核销处理开关、key2或商户签名密钥。 参见docs/交行直充业务系统详细设计.md「9.5」；处理暂停仍须保留已接收事件的资金门禁。 */
public final class MybatisMinisoWriteOffProtection implements MinisoWriteOffProtection {
    private static final int USED = 1, VOID = 3, PENDING = 13;
    private final MinisoWriteOffEventMapper events;
    private final MinisoReceiptMapper receipts;
    private final MinisoWriteOffOrderLocks orderLocks;
    private final TransactionTemplate tx;
    private final Clock clock;

    public MybatisMinisoWriteOffProtection(
            MinisoWriteOffEventMapper events,
            MinisoReceiptMapper receipts,
            PlatformTransactionManager tm,
            Clock clock,
            int timeoutSeconds) {
        this.events = events;
        this.receipts = receipts;
        this.orderLocks = new MinisoWriteOffOrderLocks(events, receipts);
        this.clock = clock;
        tx = new TransactionTemplate(tm);
        tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        tx.setTimeout(timeoutSeconds);
    }

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(clock.instant(), ZoneId.of("Asia/Shanghai"));
    }

    @Override
    public boolean blockInvalidation(String eventId, String orderId) {
        try {
            return Boolean.TRUE.equals(
                    tx.execute(
                            s -> {
                                var owned = orderLocks.lock(orderId);
                                if (owned == null)
                                    throw new MinisoWriteOffException(OWNERSHIP_CONFLICT);
                                if (!evidence(owned.receipt())) return false;
                                if (owned.order().orderStatus() == PENDING
                                        && "CANCEL_REQUESTED".equals(events.dispatchStatus(orderId))
                                        && owned.receipt().couponStatus() != VOID) {
                                    events.restoreSuccess(orderId, now());
                                    events.restoreDispatch(orderId, now());
                                }
                                events.stopInvalidation(eventId, now());
                                return true;
                            }));
        } catch (MinisoWriteOffException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw MinisoWriteOffFailures.storage(ex);
        }
    }

    @Override
    public void assertVoidAllowed(String orderId) {
        if (!TransactionSynchronizationManager.isActualTransactionActive())
            throw new IllegalStateException("MINISO void check requires order transaction");
        var local = receipts.lockReceipt(orderId);
        if (local != null && evidence(local))
            throw new MinisoWriteOffException(REDEEM_VOID_CONFLICT);
    }

    private boolean evidence(MinisoFulfillmentReceipt receipt) {
        // 即使密钥暂不可用、事实尚未推进，持久接收的核销证据也阻止作废/退款。
        var event = events.lockByCoupon(receipt.couponCode());
        return receipt.couponStatus() == USED
                || receipts.useTime(receipt.orderId()) != null
                || event != null;
    }
}
