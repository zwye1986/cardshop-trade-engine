package com.cardshop.trade.engine.infrastructure.persistence;

import static com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason.*;

import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;

import org.springframework.transaction.*;
import org.springframework.transaction.support.*;

import java.time.*;

/** 永久核销证据保护，不依赖处理开关或密钥；参见docs/交行直充业务系统详细设计.md「9.8」。
 * 独立前置检查不授予资金许可，最终作废复核必须参与调用方终态事务。
 */
public final class MybatisMinisoWriteOffProtection implements MinisoWriteOffProtection {
    private static final int USED = 1, VOID = 3;
    private final MinisoWriteOffEventMapper events;
    private final MinisoReceiptMapper receipts;
    private final MinisoCouponGuardMapper guards;
    private final MinisoSharedCallbackInboxMapper inbox;
    private final MinisoWriteOffOrderLocks orderLocks;
    private final TransactionTemplate tx;
    private final Clock clock;

    public MybatisMinisoWriteOffProtection(
            MinisoWriteOffEventMapper events,
            MinisoReceiptMapper receipts,
            MinisoCouponGuardMapper guards,
            MinisoSharedCallbackInboxMapper inbox,
            PlatformTransactionManager tm,
            Clock clock,
            int timeoutSeconds) {
        this.events = events;
        this.receipts = receipts;
        this.guards = guards;
        this.inbox = inbox;
        this.orderLocks = new MinisoWriteOffOrderLocks(events, receipts, guards);
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
                                var owned = orderLocks.lock(orderId, now());
                                if (owned == null)
                                    throw new MinisoWriteOffException(OWNERSHIP_CONFLICT);
                                if (!evidence(owned.receipt())) return false;
                                if ("CANCEL_REQUESTED".equals(events.dispatchStatus(orderId))
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
        // 调用方已锁主单/扩展，此处按凭据→guard→事件→inbox复核；不另开事务或吞读锁异常。
        var local = receipts.lockReceipt(orderId);
        if (local != null) {
            guards.lockInTransaction(local.couponCode(), now());
            if (evidence(local)) throw new MinisoWriteOffException(REDEEM_VOID_CONFLICT);
        }
    }

    private boolean evidence(MinisoFulfillmentReceipt receipt) {
        // 必须在guard后当前锁定读：不复用早先快照，不筛掉人工/已转投等已认证事实，见详细设计9.8。
        var event = events.lockByCoupon(receipt.couponCode());
        var shared = inbox.lockByCoupon(receipt.couponCode());
        return receipt.couponStatus() == USED
                || receipts.useTime(receipt.orderId()) != null
                || event != null
                || shared != null;
    }
}
