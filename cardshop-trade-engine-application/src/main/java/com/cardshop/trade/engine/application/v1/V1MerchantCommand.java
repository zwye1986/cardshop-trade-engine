package com.cardshop.trade.engine.application.v1;

/**
 * `/v1/cardapi` 新链路应用用例的最小命令对象。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.1.1 `/v1/cardapi` 路由兼容设计」。
 * 当前对象只携带已解析商户身份和原始请求体；正式应用服务会基于它查询商户安全资料、
 * 执行协议预校验并调用 Redis 接单端口。
 */
public record V1MerchantCommand(String appId, String orgCode, String rawBody) {
}
