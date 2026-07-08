package com.cardshop.trade.engine.application.recharge;

/**
 * 供应商调用结果归一化状态。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」。Gateway 适配器必须把
 * 各渠道私有响应转换成成功、处理中或明确失败三类结果。
 */
public enum SupplierGatewayStatus {
    SUCCESS,
    PROCESSING,
    FAILED
}
