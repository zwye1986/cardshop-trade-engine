package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoReceiptMapper;
import java.util.Objects;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 名创履约凭据保存，参与订单终态的同一事务，不提供独立库存能力。
 * 参见详细设计「名创 MINISO 专属协议实施约束（2026-10-17）」。异常直接抛回事务边界，不吞错。
 */
public final class MinisoReceiptStore {
    private final MinisoReceiptMapper mapper;
    private final java.util.function.Supplier<
                    com.cardshop.trade.engine.application.recharge.MinisoWriteOffProtection>
            protection;

    public MinisoReceiptStore(MinisoReceiptMapper mapper) {
        this(mapper, () -> null);
    }

    public MinisoReceiptStore(
            MinisoReceiptMapper mapper,
            java.util.function.Supplier<
                            com.cardshop.trade.engine.application.recharge.MinisoWriteOffProtection>
                    protection) { this.mapper = Objects.requireNonNull(mapper);
        this.protection = Objects.requireNonNull(protection); }

    private static boolean sameBatch(int batchId, String frozen) {
        // 名创batchId为Integer，冻结源为旧表字符串；前导零不改变批次身份，与Gateway数值转换一致。
        return frozen != null && frozen.length() <= 10 && frozen.matches("[0-9]+")
                && Long.parseLong(frozen) == batchId;
    }

    public void save(MinisoFulfillmentReceipt receipt) {
        try {
            saveWithinOrderTransaction(receipt);
            // 可信凭据和旧卡管导出投影必须一起提交；重放同样补齐缺失投影，不覆盖已有冲突数据。
            // 参见详细设计「名创卡密导出兼容修复（2026-10-21）」。主单归属锁仍由上一步持有。
            String existing = mapper.selectLegacyData(receipt.orderId(), receipt.orgCode());
            MinisoLegacyOrderData.requireCompatible(existing, receipt);
            if ((existing == null || existing.isBlank())
                    && mapper.fillLegacyData(receipt.orderId(), receipt.orgCode(),
                            MinisoLegacyOrderData.fromReceipt(receipt)) != 1) {
                throw new IllegalStateException("LEGACY_DATA_WRITE_CONFLICT");
            }
        } catch (com.cardshop.trade.engine.application.recharge.MinisoWriteOffException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            // 数据库Duplicate entry消息含完整券码，禁止把原始cause交Worker日志/last_error；由上层采样安全栈。
            throw new IllegalStateException("MINISO receipt persistence failed: " + ex.getClass().getSimpleName());
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
        var existing = mapper.selectReceipt(receipt.orderId());
        if (receipt.couponStatus() == 3 && protection.get() != null) {
            // 在同一主单事务内检查已接收事件，堵住作废HTTP返回到终态落库之间的竞态；参见详细设计9.5。
            protection.get().assertVoidAllowed(receipt.orderId());
        }
        if (existing != null && existing.couponStatus() == 1 && receipt.couponStatus() == 3) {
            throw new com.cardshop.trade.engine.application.recharge.MinisoWriteOffException(
                    com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason
                            .REDEEM_VOID_CONFLICT);
        }
        if (existing == null) {
            // 券码全局唯一约束阻止另一订单盗用券码；冲突使订单终态事务整体回滚。
            if (mapper.insert(receipt) != 1) throw new IllegalStateException("MINISO receipt insert failed");
            return;
        }
        if (!existing.orgCode().equals(receipt.orgCode()) || existing.batchId() != receipt.batchId()
                || !existing.phone().equals(receipt.phone()) || !existing.couponCode().equals(receipt.couponCode())
                || !existing.beginTime().equals(receipt.beginTime()) || !existing.endTime().equals(receipt.endTime())) {
            throw new IllegalStateException("MINISO immutable receipt conflict");
        }
        // 状态3作废终态优先；已使用不能退回未使用/过期。迟到查询只补证据，不回退已知终态。
        int next = existing.couponStatus() == 3 || receipt.couponStatus() == 3 ? 3
                : existing.couponStatus() == 1 || receipt.couponStatus() == 1 ? 1
                : existing.couponStatus() == 2 ? 2 : receipt.couponStatus();
        if (next != existing.couponStatus() && mapper.updateStatus(receipt.orderId(), next) != 1) {
            throw new IllegalStateException("MINISO receipt status update failed");
        }
    }
}
