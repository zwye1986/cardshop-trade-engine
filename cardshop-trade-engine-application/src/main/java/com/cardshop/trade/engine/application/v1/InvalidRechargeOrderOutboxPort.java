package com.cardshop.trade.engine.application.v1;

import com.cardshop.trade.engine.application.recharge.RechargeOrderInvalidationState;
import com.cardshop.trade.engine.application.recharge.InvalidationAttempt;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxEvent;
import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.application.recharge.SupplierFlowStartCommand;
import java.util.Optional;

/**
 * DB_PERSISTED 订单作废 outbox 端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 作废设计」「9.1 派单 Outbox」。
 */
public interface InvalidRechargeOrderOutboxPort {

    /** 短事务受理，原事件优先；NOT_ACCEPTED必须不创建首次事件。参见详细设计9.3.2。 */
    RechargeOrderInvalidationState prepareInvalidOrder(InvalidRechargeOrderOutboxCommand command);

    /** 主库一致事实读取，不筛due、不取写锁；存储故障不得伪装无订单。参见详细设计9.3.2。 */
    Optional<RechargeOrderInvalidationState> findInvalidOrder(String orgCode, String outOrderSn);

    /** 固定本HTTP已受理的轮次，禁止观察跳到后来新轮；参见详细设计9.3.7。 */
    default Optional<RechargeOrderInvalidationState> findInvalidOrder(String orgCode, String outOrderSn, long attemptNo) {
        return findInvalidOrder(orgCode, outOrderSn).filter(state -> state.attempt() == null
                || state.attempt().number() == attemptNo);
    }

    /** 锁内核对轮次、租约和截止后提交唯一调用尝试；empty表示禁止发送，参见详细设计9.3.7。 */
    default Optional<Long> startInvalidationCall(RechargeOutboxEvent event, SupplierFlowStartCommand flow) {
        throw new UnsupportedOperationException("persistent invalidation call registration required");
    }

    /** 只允许本轮未决→终态；旧version/轮次不得覆盖新轮，参见详细设计9.3.7。 */
    default boolean completeInvalidation(RechargeOutboxEvent event, InvalidationAttempt result) {
        throw new UnsupportedOperationException("persistent invalidation result required");
    }

    /** 已有可靠SUCCESS恢复本地收尾时只读取原本地凭据，绝不再请求供应商；参见详细设计9.3.5。 */
    default Optional<MinisoFulfillmentReceipt> findInvalidationReceipt(String orgCode, String orderId) {
        throw new UnsupportedOperationException("persistent invalidation receipt required");
    }

    /**
     * 首次事件写入失败且受理事务已确认回滚，参见详细设计9.3.2。
     * 只能在锁内证明无原事件后使用；事务回滚失败必须覆盖本异常，不能向调用方虚称未受理。
     */
    final class AdmissionRolledBackException extends RuntimeException {
        public AdmissionRolledBackException(RuntimeException cause) {
            super("first invalidation admission rolled back", cause);
        }
    }
}
