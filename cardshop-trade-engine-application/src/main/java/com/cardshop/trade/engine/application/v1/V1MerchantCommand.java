package com.cardshop.trade.engine.application.v1;

/**
 * `/v1/cardapi` 新链路应用用例的最小命令对象。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 * 当前对象只携带已解析商户身份和原始请求体；正式应用服务会基于它查询商户安全资料、
 * 执行协议预校验并调用 Redis 接单端口。
 *
 * @param appId 从 token 或认证上下文解析出的商户 appId
 * @param orgCode 从 token 或认证上下文解析出的商户编码，不信任请求体自带值
 * @param rawBody 原始请求体，用于协议预校验、解密和验签
 */
public record V1MerchantCommand(String appId, String orgCode, String rawBody) {
}
