package com.cardshop.trade.engine.application.recharge;

/**
 * 供应商 Gateway 端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和「9.2 作废设计」。
 * 适配器负责渠道协议、超时和私有响应解析；application 只消费归一化结果。
 */
public interface RechargeSupplierGatewayPort {

    SupplierGatewayResponse dispatch(SupplierDispatchGatewayRequest request);

    /**
     * 查询供应商已受理订单的最新状态。
     *
     * <p>查询仍由 Outbox Worker 异步调用，不能在商户接口线程同步修改订单状态。
     */
    SupplierGatewayResponse query(SupplierQueryGatewayRequest request);

    SupplierGatewayResponse invalid(SupplierInvalidGatewayRequest request);
}
