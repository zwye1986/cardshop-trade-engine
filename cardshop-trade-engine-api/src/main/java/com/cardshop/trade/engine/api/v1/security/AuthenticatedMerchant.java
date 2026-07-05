package com.cardshop.trade.engine.api.v1.security;

/**
 * `/v1/cardapi` Token 解析后的商户身份。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.4 Token 兼容校验」。
 */
public record AuthenticatedMerchant(String appId, String orgCode) {
}
