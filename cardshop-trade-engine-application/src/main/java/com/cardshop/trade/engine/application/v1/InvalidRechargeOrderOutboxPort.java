package com.cardshop.trade.engine.application.v1;

/**
 * DB_PERSISTED 订单作废 outbox 端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.11 作废设计」。
 */
public interface InvalidRechargeOrderOutboxPort {

    void enqueueInvalidOrder(InvalidRechargeOrderOutboxCommand command);

    static InvalidRechargeOrderOutboxPort noop() {
        return command -> {
        };
    }
}
