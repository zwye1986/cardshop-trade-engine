package com.cardshop.trade.engine.application.recharge;

/**
 * 创建供应商异步查单 outbox 事件的命令。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」和「17.1 任务清单」。事件号按
 * 订单稳定生成并由基础设施层 {@code insertIfAbsent}，确保供应商返回处理中后只保留一个可重试的查单入口。
 *
 * @param eventId 查单事件唯一号，格式固定为 {@code supplier-query-{orderId}}
 * @param orgCode 商户编码
 * @param orderId 平台订单号
 * @param outOrderSn 商户外部订单号
 * @param account 直充账号
 * @param supplierChannel 供应商渠道编码
 * @param payload 原派单 outbox 载荷，供渠道适配器读取扩展参数
 */
public record SupplierQueryOutboxCommand(
        String eventId,
        String orgCode,
        String orderId,
        String outOrderSn,
        String account,
        String supplierChannel,
        String payload) {
}
