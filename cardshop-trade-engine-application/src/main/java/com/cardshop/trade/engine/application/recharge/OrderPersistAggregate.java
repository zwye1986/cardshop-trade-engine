package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.ValidationStatus;
import java.time.Instant;
import java.util.Objects;

/**
 * Consumer 准备写入 MySQL 的订单聚合。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
 * 该对象把 MQ 消息、Redis/MQ 快照来源和落库时间合并成 infrastructure 可直接映射到
 * {@code mc_order}、{@code recharge_order_ext}、{@code recharge_dispatch_outbox}
 * 与 {@code recharge_balance_delta_outbox} 的最小事实集合（详细设计「8.3」「9.1」「9.2」）。
 *
 * @param persistMessageId RabbitMQ 落库消息 ID
 * @param snapshot 实际用于 MySQL 落库的订单快照
 * @param persistSource 快照来源
 * @param persistedAt Consumer 进入 MySQL 事务前的时间
 */
public record OrderPersistAggregate(
        String persistMessageId,
        PersistOrderSnapshot snapshot,
        PersistSource persistSource,
        Instant persistedAt) {

    public OrderPersistAggregate {
        persistMessageId = requireText(persistMessageId, "persistMessageId");
        snapshot = Objects.requireNonNull(snapshot, "snapshot must not be null");
        persistSource = Objects.requireNonNull(persistSource, "persistSource must not be null");
        persistedAt = Objects.requireNonNull(persistedAt, "persistedAt must not be null");
    }

    /**
     * 只有业务校验通过且允许派单的订单才生成供应商派单 outbox。
     *
     * <p>业务失败订单必须落库留痕，但不能派单；参见详细设计「8.3 异步落库 Consumer」
     * 对失败订单不生成 `SUPPLIER_DISPATCH` outbox 的约束。
     */
    public boolean dispatchOutboxRequired() {
        return snapshot.dispatchAllowed() && ValidationStatus.PASSED.name().equals(snapshot.validationStatus());
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
