package com.cardshop.trade.engine.application.recharge;

/**
 * 供应商调用结果归一化状态。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。Gateway 适配器必须把
 * 各渠道私有响应转换成成功、处理中、明确失败或可重试失败四类结果。
 * {@link #FAILED} 表示供应商已经给出确定业务终态，例如明确拒绝充值；{@link #RETRYABLE_FAILED}
 * 表示 HTTP 5xx、超时、连接异常这类传输层或供应商临时不可用窗口，只能退回 outbox 等待重试，
 * 不能提前释放余额或创建商户失败回调。
 */
public enum SupplierGatewayStatus {
    SUCCESS,
    PROCESSING,
    FAILED,
    RETRYABLE_FAILED
}
