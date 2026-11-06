package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxCommand;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxPort;
import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 请求内推进原INVALID事件，按同轮供应商确认或持久事实交付，不等待资金收尾；参见详细设计9.3.3、9.3.5。 */
public final class RechargeOrderInvalidationService {
    private static final Logger log = LoggerFactory.getLogger(RechargeOrderInvalidationService.class);
    private final LogStormGuard errors = LogStormGuard.defaultGuard();
    private final InvalidRechargeOrderOutboxPort port;
    private final RechargeOutboxWorkerService worker;
    private final String workerId;

    public RechargeOrderInvalidationService(InvalidRechargeOrderOutboxPort port,
            RechargeOutboxWorkerService worker, String workerId) {
        this.port = port;
        this.worker = worker;
        this.workerId = workerId;
    }

    /** 受理提交后发布事实，允许HTTP在资金收尾期间独立观察，不以Worker Outcome判定券结果。 */
    public RechargeOrderInvalidationState invalidate(InvalidRechargeOrderOutboxCommand command,
            Consumer<RechargeOrderInvalidationState> onPrepared) {
        return invalidate(command, onPrepared, ignored -> true);
    }

    /** 已取得供应商终态优先交付；后续持久化异常不能覆盖本请求成功，参见详细设计9.3.3、9.3.5。 */
    public RechargeOrderInvalidationState invalidate(InvalidRechargeOrderOutboxCommand command,
            Consumer<RechargeOrderInvalidationState> onPrepared,
            java.util.function.Predicate<RechargeOrderInvalidationState> onResult) {
        var state = port.prepareInvalidOrder(command);
        onPrepared.accept(state);
        if (state.status() != RechargeOrderInvalidationState.Status.PENDING || state.eventId() == null) return state;
        var delivered = new java.util.concurrent.atomic.AtomicReference<RechargeOrderInvalidationState>();
        try {
            if (state.attempt() == null) {
                // 切换前无轮次事件不自动转换为新合同；仍按旧任务恢复，外部不再输出202。
                worker.processEvent(state.eventId(), OutboxEventType.INVALID_ORDER, workerId);
            } else {
                worker.processInvalidationEvent(state.eventId(), state.attempt().number(), workerId, attempt -> {
                    var result = state.withAttempt(attempt);
                    if (!onResult.test(result)) return false;
                    delivered.compareAndSet(null, result);
                    return true;
                });
            }
        } catch (RuntimeException ex) {
            // 事件已提交，高频执行故障采样保留栈；仍可通过主库判断已经提交的券/资金事实。
            if (errors.shouldLog("invalidation.execute").shouldLog()) {
                log.error("请求内作废推进异常 eventId={}, orderId={}", state.eventId(), command.orderId(), ex);
            }
        }
        if (delivered.get() != null) return delivered.get();
        return observe(command.orgCode(), command.outOrderSn(), state).orElse(state);
    }

    public Optional<RechargeOrderInvalidationState> observe(String orgCode, String outOrderSn) {
        return port.findInvalidOrder(orgCode, outOrderSn);
    }

    public Optional<RechargeOrderInvalidationState> observe(String orgCode, String outOrderSn,
            RechargeOrderInvalidationState bound) {
        return bound.attempt() == null ? port.findInvalidOrder(orgCode, outOrderSn)
                : port.findInvalidOrder(orgCode, outOrderSn, bound.attempt().number());
    }
}
