package com.cardshop.trade.engine.application.recharge;

import java.util.List;

/**
 * 异步落库批路径的 outbox 异步写入端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer · Outbox 异步落库模式」
 * 与 {@code docs/superpowers/specs/2026-07-24-persist-outbox-async-design.md}。
 * 事实事务（{@code mc_order} / {@code recharge_order_ext} / {@code mc_depart_record}）提交后，
 * Consumer 在同事务线程内同步调用本端口，把 {@code recharge_dispatch_outbox} 与
 * {@code recharge_balance_delta_outbox} 的写入移到独立连接池的短事务中，避免 timer 热路径
 * claim 与批事务内 outbox INSERT 的锁争用回压到订单事实事务。
 *
 * <p>一致性不变量：本端口必须对入参全部聚合执行无条件幂等写入（只按
 * {@link OrderPersistAggregate#dispatchOutboxRequired()} 与快照 {@code balanceReserved()} 过滤），
 * 不随 {@code recharge_order_ext} 二次确认跳过收缩；重复触发靠 {@code event_id} 唯一键 ODKU 去重。
 * 写入失败必须抛异常，由 Consumer 走整批不 ack + {@code markPersistFailed} 的现有重试路径，
 * 保证「ack 必在 outbox 写成功之后」。
 */
public interface RechargePersistOutboxAsyncWriter {

    /**
     * 在独立事务内批量幂等写入派单与余额刷账 outbox。
     *
     * @param aggregates 本批全部 selected 订单聚合（含二次确认已落库的消息）
     * @throws RuntimeException 写入失败时抛出，调用方据此整批重试
     */
    void writeBatch(List<OrderPersistAggregate> aggregates);
}
