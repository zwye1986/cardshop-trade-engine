package com.cardshop.trade.engine.application.recharge;

/**
 * 切换供应商履约候选的写库命令。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。供应商业务失败后，
 * worker 会沿用同一订单金额和 outbox 事件，切换到旧卡管候选列表中的下一个渠道继续派单。
 *
 * @param orderId 平台订单号，对应旧表 {@code mc_order.order_Id}
 * @param selectedProductId 新的实际履约子商品编码
 * @param supplierChannel 新的供应商渠道编码
 * @param productName 新的商品名称快照
 * @param reason 切换原因，写入订单备注便于排障
 */
public record RechargeFulfillmentCandidateSwitchCommand(
        String orderId,
        String selectedProductId,
        String supplierChannel,
        String productName,
        String reason) {
}
