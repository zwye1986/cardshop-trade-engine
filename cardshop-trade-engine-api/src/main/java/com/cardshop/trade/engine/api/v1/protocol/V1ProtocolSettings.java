package com.cardshop.trade.engine.api.v1.protocol;

/**
 * `/v1/cardapi` 协议预校验配置。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 *
 * @param requestSm4Enabled 是否启用请求 SM4 解密
 * @param loadTestToken 压测签名结果旁路口令；空白表示旁路关闭
 */
public record V1ProtocolSettings(boolean requestSm4Enabled, String loadTestToken) {
    public V1ProtocolSettings {
        loadTestToken = loadTestToken == null ? "" : loadTestToken;
    }

    /** 兼容旧单参构造，旁路默认关闭。 */
    public V1ProtocolSettings(boolean requestSm4Enabled) {
        this(requestSm4Enabled, "");
    }
}
