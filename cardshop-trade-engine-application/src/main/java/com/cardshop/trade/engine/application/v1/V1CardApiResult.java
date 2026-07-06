package com.cardshop.trade.engine.application.v1;

/**
 * `/v1/cardapi` 应用用例返回值。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.1 协议基线」，
 * 应用层只表达旧接口的 code/msg/data 语义，不绑定 WebFlux 或 HTTP 类型。
 */
public record V1CardApiResult(String code, String msg, Object data) {

    public static V1CardApiResult unavailable(String msg) {
        return V1CardApiResultStatus.NOT_FOUND_OR_DUPLICATE.result(msg);
    }
}
