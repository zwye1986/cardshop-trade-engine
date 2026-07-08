package com.cardshop.trade.engine.api.v1.protocol;

/**
 * V1 协议预校验失败原因常量。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」。
 * 这些值只在新交易引擎内部流转，用于把解密、签名和 timestamp 校验失败映射成旧 V1 响应码；
 * 对外响应仍由各 use case 通过 {@code V1CardApiResultStatus} 生成。
 */
public final class V1ProtocolFailureReason {

    public static final String SIGN_FAILED = "SIGN_FAILED";
    public static final String DECRYPT_FAILED = "DECRYPT_FAILED";
    public static final String REQUEST_BODY_INVALID = "REQUEST_BODY_INVALID";
    public static final String TIMESTAMP_INVALID = "TIMESTAMP_INVALID";
    public static final String TIMESTAMP_OUT_OF_RANGE = "TIMESTAMP_OUT_OF_RANGE";

    private V1ProtocolFailureReason() {
    }
}
