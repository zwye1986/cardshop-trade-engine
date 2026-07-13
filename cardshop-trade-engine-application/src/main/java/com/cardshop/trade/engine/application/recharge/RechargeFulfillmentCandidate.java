package com.cardshop.trade.engine.application.recharge;

/**
 * 供应商履约候选商品快照。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。该对象只表达 worker
 * 重新选择供应商所需的最小事实：旧表 `mc_order.produt_code`、`supplier_channel` 和
 * `product_name`，金额仍沿用下单时已经冻结的订单金额。
 *
 * @param selectedProductId 实际履约子商品编码，对应旧表 {@code mc_order.produt_code}
 * @param supplierChannel 供应商渠道编码，对应旧表 {@code mc_order.supplier_channel}
 * @param productName 商品名称快照，对应旧表 {@code mc_order.product_name}
 */
public record RechargeFulfillmentCandidate(
        String selectedProductId,
        String supplierChannel,
        String productName) {

    public static RechargeFulfillmentCandidate from(RechargeFulfillmentOrder order) {
        /*
         * 默认候选只用于非 MyBatis 测试替身或兼容路径。它没有重新读取旧商品表，因此不能补出可靠的商品名称；
         * 生产 MyBatis 适配器会覆盖 findDispatchCandidates 并返回带 productName 的健康候选。
         * 参见详细设计「9.1 派单 Outbox」。
         */
        return new RechargeFulfillmentCandidate(
                order.selectedProductId(),
                order.supplierChannel(),
                "");
    }
}
