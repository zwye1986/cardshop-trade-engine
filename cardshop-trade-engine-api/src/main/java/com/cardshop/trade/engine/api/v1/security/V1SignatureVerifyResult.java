package com.cardshop.trade.engine.api.v1.security;

/**
 * V1 SM3 签名校验结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * source 用于测试和脱敏排障，日志中禁止直接输出包含账号或 aesKey 的完整 source。
 */
public record V1SignatureVerifyResult(boolean valid, String source, String expectedSign) {
}
