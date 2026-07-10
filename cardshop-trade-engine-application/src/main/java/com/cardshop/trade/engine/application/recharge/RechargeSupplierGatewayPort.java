package com.cardshop.trade.engine.application.recharge;

/**
 * 供应商 Gateway 端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和「9.2 作废设计」。
 * 适配器负责渠道协议、超时和私有响应解析；application 只消费归一化结果。
 */
public interface RechargeSupplierGatewayPort {

    SupplierGatewayResponse dispatch(SupplierDispatchGatewayRequest request);

    SupplierGatewayResponse invalid(SupplierInvalidGatewayRequest request);
}
