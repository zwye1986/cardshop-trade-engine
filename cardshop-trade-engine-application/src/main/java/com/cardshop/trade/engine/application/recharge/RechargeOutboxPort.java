package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * outbox 扫描、抢占和状态回写端口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」与
 * {@code docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md}。
 * 实现必须使用 {@code event_id + version} CAS 抢占，防止多节点重复履约。
 */
public interface RechargeOutboxPort {

    /**
     * 默认只扫描 {@link OutboxEventType#SUPPLIER_DISPATCH}，避免与 QUERY 混扫抢名额。
     */
    default List<RechargeOutboxEvent> selectDueForShard(int shardTotal, int shardIndex, int limit) {
        return selectDueForShard(shardTotal, shardIndex, limit, List.of(OutboxEventType.SUPPLIER_DISPATCH));
    }

    List<RechargeOutboxEvent> selectDueForShard(
            int shardTotal, int shardIndex, int limit, List<OutboxEventType> eventTypes);

    /**
     * 按事件号读取单个已到期或租约过期的履约事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。该入口供进程内 timer
     * 直接唤醒指定事件，必须与分片扫描使用相同的 due 条件和 {@code recharge_order_ext} 归属哨兵，
     * 只是省去范围扫描成本。
     */
    Optional<RechargeOutboxEvent> findDueByEventId(String eventId, OutboxEventType eventType);

    /**
     * 读取仍待执行事件距持久化到期时间的非负延迟，供提前唤醒后续排。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」timer 提前唤醒修订。
     * 仅返回满足新引擎归属哨兵、INIT/FAILED 且重试未耗尽的事件；已领取、终态或不存在返回空。
     * 实现以与 due 查询相同的时钟和数据库时区计算；两次读取之间已到期时返回零，不能据此跳过 CAS。
     */
    Optional<Duration> findPendingRetryDelay(String eventId, OutboxEventType eventType);

    Optional<RechargeOutboxEvent> claim(RechargeOutboxEvent event, String workerId, Duration lease);

    void markSuccess(RechargeOutboxEvent event);

    void markFailed(RechargeOutboxEvent event, String errorMessage, Duration nextRetryDelay);

    /**
     * 将达到最大重试次数的事件转入人工处理。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」。该状态用于终止永久失败事件的
     * 自动重试，同时保留人工排查入口。
     */
    void markManualRequired(RechargeOutboxEvent event, String errorMessage);
}
