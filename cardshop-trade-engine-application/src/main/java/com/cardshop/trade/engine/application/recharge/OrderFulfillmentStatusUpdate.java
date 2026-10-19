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
 * @param minisoReceipt 同事务保存的供应商凭据
 * @param invalidTime 作废完成时间，参见详细设计9.6
 */
public record OrderFulfillmentStatusUpdate(
        String orderId,
        FulfillmentDispatchStatus dispatchStatus,
        int orderStatus,
        String dataStatus,
        Long supplierFlowId,
        String failCode,
        String failReason,
        String remark,
        MinisoFulfillmentReceipt minisoReceipt,
        java.time.Instant invalidTime) {
    public OrderFulfillmentStatusUpdate(String orderId, FulfillmentDispatchStatus dispatchStatus, int orderStatus, String dataStatus, Long supplierFlowId, String failCode, String failReason, String remark) {
        this(orderId, dispatchStatus, orderStatus, dataStatus, supplierFlowId, failCode, failReason, remark, null, null);
    }

    public OrderFulfillmentStatusUpdate(String orderId, FulfillmentDispatchStatus dispatchStatus, int orderStatus,
            String dataStatus, Long supplierFlowId, String failCode, String failReason, String remark,
            MinisoFulfillmentReceipt minisoReceipt) {
        this(orderId, dispatchStatus, orderStatus, dataStatus, supplierFlowId, failCode, failReason, remark,
                minisoReceipt, null);
    }

    /** 携带凭据参与成功事务；参见详细设计名创实施约束。 */
    public OrderFulfillmentStatusUpdate withMinisoReceipt(MinisoFulfillmentReceipt receipt) {
        return new OrderFulfillmentStatusUpdate(orderId, dispatchStatus, orderStatus, dataStatus, supplierFlowId, failCode, failReason, remark, receipt, invalidTime);
    }

    /** 作废事实时间在终态与通知中复用，资金补偿不得重新取当前时间；参见详细设计9.6。 */
    public OrderFulfillmentStatusUpdate withInvalidTime(java.time.Instant time) {
        return new OrderFulfillmentStatusUpdate(orderId, dispatchStatus, orderStatus, dataStatus,
                supplierFlowId, failCode, failReason, remark, minisoReceipt, time);
    }

}
