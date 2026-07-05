package com.cardshop.trade.engine.api.v1.protocol;

/**
 * `/v1/cardapi` 协议预校验配置。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.3 SM4 解密与 SM3 验签」。
 */
public record V1ProtocolSettings(boolean requestSm4Enabled) {
}
