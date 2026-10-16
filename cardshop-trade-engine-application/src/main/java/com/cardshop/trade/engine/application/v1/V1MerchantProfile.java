package com.cardshop.trade.engine.application.v1;

/**
 * 交行 V1 商户安全资料。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「14.1 V1 DTO 字段」和
 * 「3. API/V1 接入层」。token 只提供 appId 归属线索，接口层必须从服务端资料取得
 * orgCode、aesKey 和启停状态，不能信任请求体里的商户字段。
 *
 * @param appId 商户应用 ID
 * @param orgCode 商户机构编码
 * @param aesKey 兼容旧系统的 SM4加密密钥（V1.2不参与签名），日志禁止输出
 * @param enabled 商户是否启用
 * @param configVersion 商户配置版本，用于 Redis Lua 防止 TOCTOU
 */
public record V1MerchantProfile(
        String appId,
        String orgCode,
        String aesKey,
        boolean enabled,
        Long configVersion) {
    @Override public String toString() {
        return "V1MerchantProfile[appId=" + appId + ", orgCode=" + orgCode + ", aesKey=REDACTED]";
    }
}
