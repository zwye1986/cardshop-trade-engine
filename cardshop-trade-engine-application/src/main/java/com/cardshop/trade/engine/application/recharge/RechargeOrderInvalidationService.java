package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxCommand;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxPort;
import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 请求内推进原INVALID事件，业务完成依据主库事实；参见详细设计9.3.3、9.3.5。 */
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
        var state = port.prepareInvalidOrder(command);
        onPrepared.accept(state);
        if (state.status() != RechargeOrderInvalidationState.Status.PENDING || state.eventId() == null) return state;
        try {
            worker.processEvent(state.eventId(), OutboxEventType.INVALID_ORDER, workerId);
        } catch (RuntimeException ex) {
            // 事件已提交，高频执行故障采样保留栈；仍可通过主库判断已经提交的券/资金事实。
            if (errors.shouldLog("invalidation.execute").shouldLog()) {
                log.error("请求内作废推进异常 eventId={}, orderId={}", state.eventId(), command.orderId(), ex);
            }
        }
        return port.findInvalidOrder(command.orgCode(), command.outOrderSn()).orElse(state);
    }

    public Optional<RechargeOrderInvalidationState> observe(String orgCode, String outOrderSn) {
        return port.findInvalidOrder(orgCode, outOrderSn);
    }
}
