package com.cardshop.trade.engine.application.recharge;

/** 安全分类异常，不携带原报文或SQL cause；参见docs/交行直充业务系统详细设计.md「9.5」。 */
public final class MinisoWriteOffException extends RuntimeException {
    public enum Reason {
        INVALID,
        SIGNATURE_FAILED,
        CONFLICT,
        DB_UNAVAILABLE,
        RECEIPT_PENDING,
        ORDER_PENDING,
        MERCHANT_KEY_UNAVAILABLE,
        OWNERSHIP_CONFLICT,
        USE_TIME_CONFLICT,
        USE_TIME_OUT_OF_RANGE,
        REDEEM_VOID_CONFLICT,
        RELEASE_AUTHORIZATION_INCONSISTENT,
        RELEASE_TERMINAL_CONFLICT,
        RELEASE_EVIDENCE_MISSING,
        NOTIFY_TASK_CONFLICT;

        public boolean retryable() {
            return this == DB_UNAVAILABLE
                    || this == RECEIPT_PENDING
                    || this == ORDER_PENDING
                    || this == MERCHANT_KEY_UNAVAILABLE;
        }
    }

    private final Reason reason;

    public MinisoWriteOffException(Reason reason) {
        super(reason.name());
        this.reason = reason;
    }

    /** 诊断只接受固定ASCII元信息，不允许正文/SQL原文或异常cause。 */
    public MinisoWriteOffException(Reason reason, String safeDiagnostic) {
        super(
                reason.name()
                        + ":"
                        + (safeDiagnostic.matches("[A-Za-z0-9_.;=-]{1,128}")
                                ? safeDiagnostic
                                : "unknown"));
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
