package com.cardshop.trade.engine.domain.order;

/**
 * 交行直充订单业务校验结果。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.13.4 下单业务校验矩阵」。
 * 身份、签名、JSON 解析失败的请求不会形成订单；只有能识别商户订单号的业务校验结果才进入该状态。
 */
public enum ValidationStatus {

    /**
     * 业务校验通过，可以进入余额预扣和供应商派单链路。
     */
    PASSED(true),

    /**
     * 业务校验失败，需要落 Redis/MySQL 留痕，但不能派单和扣款。
     */
    FAILED(false);

    private final boolean dispatchAllowed;

    ValidationStatus(boolean dispatchAllowed) {
        this.dispatchAllowed = dispatchAllowed;
    }

    /**
     * 判断该校验结果是否允许生成供应商派单 outbox。
     */
    public boolean allowsDispatch() {
        return dispatchAllowed;
    }
}
