package com.cardshop.trade.engine.api.v1.security;

/**
 * V1.2 HMAC-SM3 签名校验结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * V1.2入口不返回source或expectedSign，避免在排障日志中暴露业务明文与认证码。
 */
public record V1SignatureVerifyResult(boolean valid, String source, String expectedSign) {
}
