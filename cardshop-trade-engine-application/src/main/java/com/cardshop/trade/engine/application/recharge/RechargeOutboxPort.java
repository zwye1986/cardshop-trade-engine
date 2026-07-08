package com.cardshop.trade.engine.application.recharge;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * outbox 扫描、抢占和状态回写端口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」。实现必须使用
 * {@code event_id + version} CAS 抢占，防止多节点重复履约。
 */
public interface RechargeOutboxPort {

    List<RechargeOutboxEvent> selectDueForShard(int shardTotal, int shardIndex, int limit);

    Optional<RechargeOutboxEvent> claim(RechargeOutboxEvent event, String workerId, Duration lease);

    void markSuccess(RechargeOutboxEvent event);

    void markFailed(RechargeOutboxEvent event, String errorMessage, Duration nextRetryDelay);
}
