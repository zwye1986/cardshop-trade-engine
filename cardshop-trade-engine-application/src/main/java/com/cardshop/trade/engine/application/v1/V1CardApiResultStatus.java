package com.cardshop.trade.engine.application.v1;

/**
 * `/v1/cardapi` 旧协议返回码模板。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.1 协议基线」。枚举只承载稳定的
 * code/default msg 映射；运行时动态文案和 data 仍由 {@link V1CardApiResult} 表达，避免把响应载荷固化到枚举实例。
 */
public enum V1CardApiResultStatus {

    SUCCESS("200", "操作成功"),
    AUTH_FAILED("401", "凭证错误"),
    SYSTEM_BUSY("500", "系统繁忙，请稍后重试"),
    SIGN_FAILED("501", "验签失败！"),
    SYSTEM_ERROR("502", "系统异常"),
    BUSINESS_FAILED("503", null),
    BALANCE_OR_MERCHANT_FAILED("504", null),
    NOT_FOUND_OR_DUPLICATE("505", "订单不存在"),
    PARAM_ERROR("506", "参数错误");

    private final String code;
    private final String defaultMsg;

    V1CardApiResultStatus(String code, String defaultMsg) {
        this.code = code;
        this.defaultMsg = defaultMsg;
    }

    public String code() {
        return code;
    }

    public V1CardApiResult result() {
        return result(requireDefaultMsg(), null);
    }

    public V1CardApiResult result(Object data) {
        return result(requireDefaultMsg(), data);
    }

    public V1CardApiResult result(String msg) {
        return result(msg, null);
    }

    public V1CardApiResult result(String msg, Object data) {
        if (isBlank(msg)) {
            throw new IllegalArgumentException(name() + " requires a non-blank msg");
        }
        return new V1CardApiResult(code, msg, data);
    }

    private String requireDefaultMsg() {
        if (isBlank(defaultMsg)) {
            throw new IllegalStateException(name() + " requires an explicit msg");
        }
        return defaultMsg;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
