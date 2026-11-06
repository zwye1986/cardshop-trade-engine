package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;

/** 请求内确认或主库读取的同轮作废事实，不携带券码或供应商原文；状态含义见详细设计9.3.2。 */
public record RechargeOrderInvalidationState(
        RechargeOrderSnapshot order, String eventId, Status status, String reason, InvalidationAttempt attempt) {
    public enum Status { COMPLETED, PENDING, NOT_ACCEPTED, MANUAL_REQUIRED, FAILED }

    /** 历史无轮次事实仅供切换盘点，不自动生成新轮；参见详细设计9.3.7。 */
    public RechargeOrderInvalidationState(RechargeOrderSnapshot order, String eventId, Status status, String reason) {
        this(order, eventId, status, reason, null);
    }

    public RechargeOrderInvalidationState withAttempt(InvalidationAttempt value) {
        var status = switch (value.result()) {
            case PENDING -> Status.PENDING;
            case SUCCESS -> Status.COMPLETED;
            case REJECTED, UNKNOWN -> Status.FAILED;
        };
        var snapshot = order;
        if (status == Status.COMPLETED) {
            String time = java.time.Instant.ofEpochMilli(value.confirmedEpochMs())
                    .atZone(java.time.ZoneId.of("Asia/Shanghai"))
                    .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            snapshot = new RechargeOrderSnapshot(order.orderId(), order.outOrderSn(), "SUCCESS",
                    order.persistStatus(), order.validationStatus(), "", "", order.dispatchAllowed(),
                    order.stateType(), "3", "", time);
        }
        return new RechargeOrderInvalidationState(snapshot, eventId, status, "", value);
    }
}
