package com.cardshop.trade.engine.application.recharge;

/**
 * 供应商异步查单 outbox 写入端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。处理中响应不能在当前派单事件上
 * 无限重试，必须先落一条独立的 {@code SUPPLIER_QUERY} 可靠事件，再结束派单事件；实现应按 event_id
 * 幂等插入。
 */
public interface SupplierQueryOutboxPort {

    void enqueueSupplierQuery(SupplierQueryOutboxCommand command);
}
