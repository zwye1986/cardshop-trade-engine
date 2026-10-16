package com.cardshop.trade.engine.api.v1.protocol;

/**
 * `/v1/cardapi` 协议预校验配置。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 *
 * @param requestSm4Enabled 是否启用请求 SM4 解密
 * @param loadTestToken 已废弃，V1.2忽略此配置，不能旁路验签
 */
public record V1ProtocolSettings(boolean requestSm4Enabled, String loadTestToken) {
    public V1ProtocolSettings {
        loadTestToken = loadTestToken == null ? "" : loadTestToken;
    }

    /** 单参构造；V1.2所有路径必须验签。 */
    public V1ProtocolSettings(boolean requestSm4Enabled) {
        this(requestSm4Enabled, "");
    }
}
