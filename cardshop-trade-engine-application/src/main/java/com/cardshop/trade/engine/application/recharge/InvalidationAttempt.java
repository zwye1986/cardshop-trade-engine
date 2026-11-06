package com.cardshop.trade.engine.application.recharge;

import com.fasterxml.jackson.annotation.JsonProperty;

/** 同一原作废事件内的一轮裁决，不包含nonce、券码或供应商原文；参见详细设计9.3.7。 */
public record InvalidationAttempt(
        @JsonProperty("attempt_no") long number,
        @JsonProperty("supplier_request_id") String supplierRequestId,
        @JsonProperty("deadline_at") long deadlineEpochMs,
        @JsonProperty("result") Result result,
        @JsonProperty("confirmed_at") long confirmedEpochMs,
        @JsonProperty("flow_id") Long flowId) {
    public static final String PAYLOAD_KEY = "invalidation_attempts";
    public enum Result { PENDING, SUCCESS, REJECTED, UNKNOWN }

    public InvalidationAttempt finish(Result outcome, long confirmedEpochMs, Long flowId) {
        return new InvalidationAttempt(number, supplierRequestId, deadlineEpochMs, outcome, confirmedEpochMs, flowId);
    }
}
