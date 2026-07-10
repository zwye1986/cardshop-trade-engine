package com.cardshop.trade.engine.application.recharge;

/**
 * 更新订单业务履约状态的命令。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。{@code orderStatus}
 * 是旧接口数字状态，{@code dispatchStatus} 是新扩展表中的业务履约状态，两者不能混用。
 *
 * @param orderId 平台订单号，对应旧表 {@code mc_order.order_Id}
 * @param dispatchStatus 业务履约状态，对应 {@code recharge_order_ext.dispatch_status}
 * @param orderStatus 旧接口数字订单状态，对应 {@code mc_order.order_status}
 * @param dataStatus 旧卡管数据处理状态，对应 {@code mc_order.data_status}
 * @param supplierFlowId 供应商流水主键，对应 {@code mc_order.flow_id} 的业务语义
 * @param failCode 失败码，对应 {@code mc_order.fail_code}
 * @param failReason 失败原因，对应 {@code mc_order.fail_reason}
 * @param remark 订单履约备注，对应 {@code mc_order.remark}
 */
public record OrderFulfillmentStatusUpdate(
        String orderId,
        FulfillmentDispatchStatus dispatchStatus,
        int orderStatus,
        String dataStatus,
        Long supplierFlowId,
        String failCode,
        String failReason,
        String remark) {
}
