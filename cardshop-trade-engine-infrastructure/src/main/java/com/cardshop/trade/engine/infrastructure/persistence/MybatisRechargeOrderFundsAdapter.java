package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
import com.cardshop.trade.engine.domain.order.PersistStatus;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeBalanceDeltaOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoReceiptMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeBalanceDeltaOutboxMapper;
import java.util.Objects;
import java.util.function.Supplier;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 以持久业务事实约束独立Redis资金操作，并原子完成MySQL退款交接。
 *
 * <p>参见 docs/交行直充业务系统详细设计.md「2026-10-21 订单快照失效后的独立资金协议」。
 * 复用主单→扩展→凭据→核销事件锁序，保护锁持续覆盖有界Redis调用；Redis成功后SQL回滚
 * 不会撤销Redis，只能由同订单资金凭证幂等恢复。这里不重建历史快照、消费事实或余额；
 * 历史无资金标记的作废退款由已核实的扣款及作废事实授权，见同文档「历史已作废订单退款最小兼容」。
 */
public final class MybatisRechargeOrderFundsAdapter implements RechargeOrderFundsPort {
    public static final int DEFAULT_TRANSACTION_TIMEOUT_SECONDS = 5;
    private static final String MINISO_CHANNEL = "MINISO";
    private static final String PRE_DEDUCT = "PRE_DEDUCT";
    private static final String RELEASE = "RELEASE";
    private static final String PRE_DEDUCT_EVENT_PREFIX = "balance-flush-";
    private final RechargeOrderFundsService service;
    private final RechargeFulfillmentOrderPort orders;
    private final RechargeBalanceDeltaOutboxMapper deltaMapper;
    private final MinisoReceiptMapper receipts;
    private final Supplier<MinisoWriteOffProtection> protection;
    private final TransactionTemplate transaction;

    public MybatisRechargeOrderFundsAdapter(BalanceReservationReleasePort redis, RechargeFulfillmentOrderPort orders,
            BalanceDeltaFlushOutboxPort deltas, RechargeBalanceDeltaOutboxMapper deltaMapper,
            MinisoReceiptMapper receipts, Supplier<MinisoWriteOffProtection> protection,
            PlatformTransactionManager transactionManager, int timeoutSeconds) {
        this.service = new RechargeOrderFundsService(redis, orders, deltas);
        this.orders = Objects.requireNonNull(orders);
        this.deltaMapper = Objects.requireNonNull(deltaMapper);
        this.receipts = Objects.requireNonNull(receipts);
        this.protection = Objects.requireNonNull(protection);
        this.transaction = new TransactionTemplate(Objects.requireNonNull(transactionManager));
        this.transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.transaction.setTimeout(timeoutSeconds);
    }

    /** 消费确认同样读取锁定的MySQL成功事实，不能用过期的Worker内存对象确认资金。 */
    @Override
    public boolean confirm(RechargeFulfillmentOrder candidate) {
        return Boolean.TRUE.equals(transaction.execute(status -> {
            var order = lockOwned(candidate);
            if (order.orderStatus() != LegacyOrderStatusCode.SUCCESS.code()
                    || order.cancelled() || !order.balanceReserved()) {
                throw new IllegalStateException("order is not eligible for funds confirmation");
            }
            verifyDelta(deltaMapper.selectFundsEventForUpdate(PRE_DEDUCT_EVENT_PREFIX + order.orderId()),
                    order, PRE_DEDUCT, -order.orderPrice());
            return service.confirm(order);
        }));
    }

    /**
     * 在同一短事务中校验资格、执行Redis、写唯一Delta及清除预扣标记。
     * 所有异常直接向Worker传播，由其采样记录和退避；本层不重复日志。
     */
    @Override
    public BalanceReservationReleaseResult release(RechargeFulfillmentOrder candidate, String reason) {
        return Objects.requireNonNull(transaction.execute(status -> {
            var order = lockOwned(candidate);
            boolean cancelled = order.cancelled()
                    && order.orderStatus() == LegacyOrderStatusCode.SUCCESS.code() && order.invalidTime() != null;
            boolean failed = !order.cancelled() && (order.orderStatus() == LegacyOrderStatusCode.FAILED.code()
                    || order.orderStatus() == LegacyOrderStatusCode.REFUNDED.code());
            if (!cancelled && !failed) throw new IllegalStateException("order is not eligible for funds release");
            if (MINISO_CHANNEL.equals(order.supplierChannel())) {
                var guard = protection.get();
                if (guard == null) throw new IllegalStateException("MINISO funds release protection unavailable");
                // assertVoidAllowed复用当前事务的凭据及事件锁，不释放保护后再调用Redis。
                guard.assertVoidAllowed(order.orderId());
            }
            var preDeduct = deltaMapper.selectFundsEventForUpdate(PRE_DEDUCT_EVENT_PREFIX + order.orderId());
            verifyDelta(preDeduct, order, PRE_DEDUCT, -order.orderPrice());
            var previous = deltaMapper.selectFundsEventForUpdate(RechargeOrderFundsService.RELEASE_EVENT_PREFIX + order.orderId());
            if (previous != null) verifyDelta(previous, order, RELEASE, order.orderPrice());
            if (!order.balanceReserved() && previous == null)
                throw new IllegalStateException("released order has no matching release delta");
            // 详细设计「历史已作废订单退款最小兼容」：资金约束只对已刷账扣款的作废单放宽。
            // 未成功的预扣仍走原严格协议；不能仅凭订单12或聚合frozen=0推断已扣款。
            boolean allowLegacyCancelledRefund = cancelled && TaskStatus.SUCCESS.name().equals(preDeduct.getStatus());
            var result = service.release(order, reason, previous != null, allowLegacyCancelledRefund);
            if (result.succeeded()) {
                verifyDelta(deltaMapper.selectFundsEventForUpdate(RechargeOrderFundsService.RELEASE_EVENT_PREFIX + order.orderId()),
                        order, RELEASE, order.orderPrice());
            }
            return result;
        }));
    }

    /** 复用既有单表锁查询；锁定后重读业务事实，并核对主单/扩展/调用方三方身份和金额。 */
    private RechargeFulfillmentOrder lockOwned(RechargeFulfillmentOrder candidate) {
        Objects.requireNonNull(candidate, "order must not be null");
        var main = receipts.lockOrder(candidate.orderId());
        var ext = receipts.lockFrozen(candidate.orderId());
        if (main == null || ext == null || !Objects.equals(main.orderId(), ext.orderId())
                || !Objects.equals(main.orgCode(), ext.orgCode()) || !Objects.equals(main.outOrderSn(), ext.outOrderSn())
                || !Objects.equals(main.orgCode(), candidate.orgCode())
                || !Objects.equals(main.outOrderSn(), candidate.outOrderSn())
                || !PersistStatus.DB_PERSISTED.name().equals(ext.persistStatus())) {
            throw new IllegalStateException("funds order identity or persistence mismatch");
        }
        var order = orders.findByOrderId(candidate.orderId())
                .orElseThrow(() -> new IllegalStateException("funds order identity missing"));
        if (!Objects.equals(order.orderId(), main.orderId()) || !Objects.equals(order.orgCode(), main.orgCode())
                || !Objects.equals(order.outOrderSn(), main.outOrderSn())
                || !Objects.equals(order.supplierChannel(), main.channel())
                || order.orderPrice() <= 0 || order.orderPrice() != candidate.orderPrice()) {
            throw new IllegalStateException("funds order identity or amount mismatch");
        }
        return order;
    }

    /** 唯一事件冲突仅在完整业务内容一致时幂等成功，拒绝只认event_id而吞掉金额或归属冲突。 */
    private static void verifyDelta(RechargeBalanceDeltaOutboxDO delta, RechargeFulfillmentOrder order,
            String type, long amount) {
        if (delta == null || !Objects.equals(delta.getOrgCode(), order.orgCode())
                || !Objects.equals(delta.getOrderId(), order.orderId())
                || !Objects.equals(delta.getOutOrderSn(), order.outOrderSn())
                || !Objects.equals(delta.getDeltaType(), type) || !Objects.equals(delta.getDeltaAmount(), amount)) {
            throw new IllegalStateException("funds delta identity, type or amount mismatch");
        }
    }
}
