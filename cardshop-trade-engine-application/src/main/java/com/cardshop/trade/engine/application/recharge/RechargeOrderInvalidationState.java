package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;

/** 主库作废事实，不携带券码或供应商原文；状态含义见详细设计9.3.2。 */
public record RechargeOrderInvalidationState(
        RechargeOrderSnapshot order, String eventId, Status status, String reason) {
    public enum Status { COMPLETED, PENDING, NOT_ACCEPTED, MANUAL_REQUIRED }
}
