package com.cardshop.trade.engine.api.v1.protocol;

/**
 * V1 协议预校验所需的商户安全资料。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「14.1 V1 DTO 字段」
 * 和「3. API/V1 接入层」。该对象来自 token 解析后的商户资料查询结果，
 * 不允许由请求体传入。
 */
public record V1MerchantSecurityProfile(String appId, String orgCode, String aesKey,
        com.cardshop.trade.engine.application.v1.crypto.V12SigningCredential signingCredential) {
    public V1MerchantSecurityProfile(String appId,String orgCode,String aesKey) { this(appId,orgCode,aesKey,null); }
    @Override public String toString() { return "V1MerchantSecurityProfile[secrets=REDACTED]"; }
}
