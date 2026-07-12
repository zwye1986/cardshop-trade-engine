package com.cardshop.trade.engine.application.recharge;

import java.util.List;
import java.util.Optional;

/**
 * 订单履约写库端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。实现需在同一
 * MySQL 事务中更新 {@code mc_order}、{@code recharge_order_ext} 和供应商流水，确保状态可追溯。
 */
public interface RechargeFulfillmentOrderPort {

    Optional<RechargeFulfillmentOrder> findByOrderId(String orderId);

    /**
     * 按旧卡管 `listByProductIdDIY` 语义读取本次派单可尝试的商品/供应商候选。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。默认实现只返回订单当前
     * 已保存的候选，避免旧测试替身或非 MyBatis 适配器在升级期间失去编译兼容；生产 MyBatis 实现必须覆盖为
     * “原商品优先、stock 优先、sort 倒序，并排除已失败渠道”的旧系统行为。生产实现还必须和受理阶段使用
     * 同一套健康直充过滤，不能把非直充或低于底价的子商品作为 failover 目标。
     */
    default List<RechargeFulfillmentCandidate> findDispatchCandidates(RechargeFulfillmentOrder order) {
        return List.of(RechargeFulfillmentCandidate.from(order));
    }

    long insertSupplierFlowStart(SupplierFlowStartCommand command);

    void updateSupplierFlow(SupplierFlowUpdateCommand command);

    /**
     * 切换订单当前履约商品/供应商快照。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。旧系统在重试其它渠道时会以
     * 当前候选构造供应商请求；新系统需要同步更新旧表 `produt_code/supplier_channel/product_name`，
     * 让后续查单、回调和人工排障看到实际承接本次履约的渠道。
     */
    default void switchFulfillmentCandidate(RechargeFulfillmentCandidateSwitchCommand command) {
    }

    void updateFulfillmentStatus(OrderFulfillmentStatusUpdate update);

    /**
     * 标记 MySQL 扩展订单中的 Redis 预扣已经释放。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「14.6 订单最终失败/作废退款协议」。
     * 该动作必须在 Redis release 成功且反向 {@code BALANCE_DELTA_FLUSH} outbox 已写入后执行；
     * 若提前置位，后续 outbox 重试会失去继续补资金动作的判断依据。
     */
    void markBalanceReleased(String orderId);
}
