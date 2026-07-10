package com.cardshop.trade.engine.application.recharge;

/**
 * MySQL 落库后的业务履约状态。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。这些值描述供应商
 * 派单、限频、作废和终态，不能写入 Redis/MQ 的 {@code persist_status}。
 */
public enum FulfillmentDispatchStatus {
    WAIT_DISPATCH,
    FREQUENCY_WAIT,
    DISPATCHING,
    SUPPLIER_ACCEPTED,
    SUCCESS,
    FAILED,
    CANCEL_REQUESTED,
    CANCELLED
}
