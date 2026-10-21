package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoCouponGuardMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoReceiptMapper;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 名创履约凭据保存，参与订单终态的同一事务，不提供独立库存能力。
 * 参见docs/交行直充业务系统详细设计.md「9.8」。异常直接抛回事务边界，不吞错。
 */
public final class MinisoReceiptStore {
    private static final int USED = 1, VOID = 3;
    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private final MinisoReceiptMapper mapper;
    private final MinisoCouponGuardMapper guards;
    private final Clock clock;
    private final java.util.function.Supplier<
                    com.cardshop.trade.engine.application.recharge.MinisoWriteOffProtection>
            protection;

    public MinisoReceiptStore(
            MinisoReceiptMapper mapper,
            MinisoCouponGuardMapper guards,
            Clock clock,
            java.util.function.Supplier<
                            com.cardshop.trade.engine.application.recharge.MinisoWriteOffProtection>
                    protection) {
        this.mapper = Objects.requireNonNull(mapper);
        this.guards = Objects.requireNonNull(guards);
        this.clock = Objects.requireNonNull(clock);
        this.protection = Objects.requireNonNull(protection);
    }

    private static boolean sameBatch(int batchId, String frozen) {
        // 名创batchId为Integer，冻结源为旧表字符串；前导零不改变批次身份，与Gateway数值转换一致。
        return frozen != null && frozen.length() <= 10 && frozen.matches("[0-9]+")
                && Long.parseLong(frozen) == batchId;
    }

    public void save(MinisoFulfillmentReceipt receipt) {
        try {
            saveWithinOrderTransaction(receipt);
        } catch (com.cardshop.trade.engine.application.recharge.MinisoWriteOffException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            // 数据库Duplicate entry消息含完整券码，禁止把原始cause交Worker日志/last_error；由上层采样安全栈。
            throw new IllegalStateException("MINISO receipt persistence failed: " + ex.getClass().getSimpleName());
        }
    }

    /**
     * 首次资金许可的凭据及核销门禁，必须参与主单/扩展已锁定的调用方事务。
     * 参见docs/交行直充业务系统详细设计.md「9.8」：缺凭据不能猜测未发券；当前仅可信作废凭据
     * 证明已发行券不再有效，未核验的供应商失败码不提供无券码放行例外。
     */
    public void assertTerminalReleaseAllowed(String orderId) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("MINISO release check requires order transaction");
        }
        var snapshot = mapper.lockSnapshot(orderId);
        if (snapshot == null) {
            throw new com.cardshop.trade.engine.application.recharge.MinisoWriteOffException(
                    com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason.OWNERSHIP_CONFLICT);
        }
        var local = mapper.lockReceipt(orderId);
        if (local == null) {
            throw new com.cardshop.trade.engine.application.recharge.MinisoWriteOffException(
                    com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason.RECEIPT_PENDING);
        }
        if (!snapshot.orgCode().equals(local.orgCode()) || !snapshot.account().equals(local.phone())
                || snapshot.num() != 1 || !sameBatch(local.batchId(), snapshot.batchId())) {
            throw new com.cardshop.trade.engine.application.recharge.MinisoWriteOffException(
                    com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason.OWNERSHIP_CONFLICT);
        }
        // 此保护内部按凭据→guard→新事件→inbox当前读，不另开REQUIRES_NEW事务。
        Objects.requireNonNull(protection.get(), "MINISO release protection required").assertVoidAllowed(orderId);
        if (local.couponStatus() != VOID) {
            throw new com.cardshop.trade.engine.application.recharge.MinisoWriteOffException(
                    com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason.RELEASE_EVIDENCE_MISSING);
        }
    }

    private void saveWithinOrderTransaction(MinisoFulfillmentReceipt receipt) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("MINISO receipt requires order transaction");
        }
        var snapshot = mapper.lockSnapshot(receipt.orderId());
        if (snapshot == null || !receipt.orgCode().equals(snapshot.orgCode())
                || !receipt.phone().equals(snapshot.account()) || snapshot.num() != 1
                || !sameBatch(receipt.batchId(), snapshot.batchId())) {
            throw new IllegalStateException("MINISO receipt ownership or frozen snapshot mismatch");
        }
        var existing = mapper.lockReceipt(receipt.orderId());
        if (existing != null && (!existing.orgCode().equals(receipt.orgCode())
                || existing.batchId() != receipt.batchId()
                || !existing.phone().equals(receipt.phone()) || !existing.couponCode().equals(receipt.couponCode())
                || !existing.beginTime().equals(receipt.beginTime()) || !existing.endTime().equals(receipt.endTime()))) {
            throw new IllegalStateException("MINISO immutable receipt conflict");
        }
        // 已有凭据先锁凭据，首次发布先锁同券guard再insert；锁与终态共用主事务，参见详细设计9.8。
        guards.lockInTransaction(receipt.couponCode(), LocalDateTime.ofInstant(clock.instant(), ZONE));
        if (existing != null && existing.couponStatus() == USED && receipt.couponStatus() == VOID) {
            throw new com.cardshop.trade.engine.application.recharge.MinisoWriteOffException(
                    com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason
                            .REDEEM_VOID_CONFLICT);
        }
        if (existing == null) {
            // 券码全局唯一约束阻止另一订单盗用券码；冲突使订单终态事务整体回滚。
            if (mapper.insert(receipt) != 1) throw new IllegalStateException("MINISO receipt insert failed");
        }
        if (receipt.couponStatus() == VOID) {
            // 首次作废凭据也先在本事务内发布，随后可按订单定位券码；证据冲突使insert一并回滚。
            // 保护缺失不能降级放行。独立blockInvalidation仅为前置提示，不授予资金许可；参见详细设计9.8。
            Objects.requireNonNull(protection.get(), "MINISO void protection required")
                    .assertVoidAllowed(receipt.orderId());
        }
        if (existing == null) return;
        // 状态3作废终态优先；已使用不能退回未使用/过期。迟到查询只补证据，不回退已知终态。
        int next = existing.couponStatus() == VOID || receipt.couponStatus() == VOID ? VOID
                : existing.couponStatus() == USED || receipt.couponStatus() == USED ? USED
                : existing.couponStatus() == 2 ? 2 : receipt.couponStatus();
        if (next != existing.couponStatus() && mapper.updateStatus(receipt.orderId(), next) != 1) {
            throw new IllegalStateException("MINISO receipt status update failed");
        }
    }
}
