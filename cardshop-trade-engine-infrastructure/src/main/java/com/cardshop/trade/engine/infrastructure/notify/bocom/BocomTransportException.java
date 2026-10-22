package com.cardshop.trade.engine.infrastructure.notify.bocom;

import com.bocom.api.BocomApiException;

/** 仅携带已观测HTTP状态及失败阶段，禁止把报文/URL写入错误正文。参见详细设计2104实施细化。 */
public final class BocomTransportException extends BocomApiException {
    private final int httpStatus;
    private final boolean verificationFailure;
    public BocomTransportException(int httpStatus,boolean verificationFailure,Throwable cause) {
        super("Bocom HTTP response was not acknowledged",cause);
        this.httpStatus=httpStatus;this.verificationFailure=verificationFailure;
    }
    public int httpStatus() { return httpStatus; }
    public boolean verificationFailure() { return verificationFailure; }
}
