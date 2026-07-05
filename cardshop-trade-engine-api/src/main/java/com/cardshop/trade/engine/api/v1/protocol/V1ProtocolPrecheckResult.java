package com.cardshop.trade.engine.api.v1.protocol;

/**
 * V1 协议预校验结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.3 SM4 解密与 SM3 验签」。
 * payload 只在校验成功时返回，失败时保留 reason 供上层映射旧接口错误码。
 */
public record V1ProtocolPrecheckResult<T>(boolean valid, String reason, T payload) {

    public static <T> V1ProtocolPrecheckResult<T> ok(T payload) {
        return new V1ProtocolPrecheckResult<>(true, "OK", payload);
    }

    public static <T> V1ProtocolPrecheckResult<T> invalid(String reason) {
        return new V1ProtocolPrecheckResult<>(false, reason, null);
    }
}
