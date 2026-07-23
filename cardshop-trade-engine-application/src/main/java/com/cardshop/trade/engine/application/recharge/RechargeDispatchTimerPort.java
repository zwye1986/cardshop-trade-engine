package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.time.Duration;

/**
 * 供应商履约 outbox 的进程内 timer 端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。timer 只负责低延迟唤醒指定
 * {@code event_id}，不可替代 {@code recharge_dispatch_outbox} 的持久化任务事实；重启恢复、多节点互斥和最终
 * 状态仍由 outbox due 条件与 CAS 抢占保证。
 */
public interface RechargeDispatchTimerPort {

    /**
     * 在指定延迟后唤醒一个 outbox 事件。
     *
     * @param eventId outbox 事件号，例如 {@code dispatch-{orderId}} 或 {@code supplier-query-{orderId}}
     * @param eventType outbox 事件类型
     * @param delay 唤醒延迟
     */
    void schedule(String eventId, OutboxEventType eventType, Duration delay);

    /**
     * 未启用 timer 时的空实现，供单元测试和降级配置保持构造兼容。
     */
    static RechargeDispatchTimerPort noop() {
        return (eventId, eventType, delay) -> {
        };
    }
}
