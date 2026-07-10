package com.cardshop.trade.engine.api.v1.codec;

/**
 * `/v1/cardapi` 请求体解析异常。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * 异常消息只描述解析阶段，不包含原始请求体，避免日志泄露账号、签名或密文。
 */
public class V1RequestBodyDecodeException extends RuntimeException {

    public V1RequestBodyDecodeException(String message) {
        super(message);
    }

    public V1RequestBodyDecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
