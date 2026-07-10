package com.cardshop.trade.engine.api.v1.security;

/**
 * `/v1/cardapi` Token 解析后的商户身份。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 */
public record AuthenticatedMerchant(String appId, String orgCode) {
}
