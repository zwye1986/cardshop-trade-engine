package com.cardshop.trade.engine.application.v1;

/**
 * 直充平台订单号生成端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「14.1 V1 DTO 字段」。
 * 平台订单号由服务端生成，不能由调用方传入。
 */
@FunctionalInterface
public interface RechargeOrderIdGenerator {

    /**
     * 生成平台订单号。
     */
    String nextOrderId();
}
