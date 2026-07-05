package com.cardshop.trade.engine.api.v1.protocol;

/**
 * V1 协议预校验所需的商户安全资料。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.2 V1 请求 DTO 和字段来源」
 * 和「20.13.3 SM4 解密与 SM3 验签」。该对象来自 token 解析后的商户资料查询结果，
 * 不允许由请求体传入。
 */
public record V1MerchantSecurityProfile(String appId, String orgCode, String aesKey) {
}
