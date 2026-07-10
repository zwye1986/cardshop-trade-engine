package com.cardshop.trade.engine.application.recharge;

/**
 * 余额 delta 刷账 outbox 写入端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.2 作废设计」。履约 Worker 只表达“需要释放或退款
 * 预扣金额”的业务事实；具体 {@code recharge_outbox} 幂等事件 ID、payload 和重试字段由基础设施层统一生成。
 */
public interface BalanceDeltaFlushOutboxPort {

    void enqueueBalanceDeltaFlush(BalanceDeltaFlushOutboxCommand command);

    static BalanceDeltaFlushOutboxPort noop() {
        return command -> {
        };
    }
}
