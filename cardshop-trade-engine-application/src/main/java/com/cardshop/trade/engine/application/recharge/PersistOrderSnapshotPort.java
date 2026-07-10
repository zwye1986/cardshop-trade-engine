package com.cardshop.trade.engine.application.recharge;

import java.util.Optional;

/**
 * Persist Consumer 读取 Redis 最新订单快照的端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」第 2-5 步。
 * application 层只关心是否能拿到完整落库快照；Redis key、HGETALL、超时降级等细节留给 infrastructure。
 */
public interface PersistOrderSnapshotPort {

    /**
     * 按商户订单号和平台订单号读取 Redis 最新订单快照。
     */
    Optional<PersistOrderSnapshot> findLatestSnapshot(String orgCode, String outOrderSn, String orderId);

    /**
     * Redis 查询能力未装配时的空实现。Consumer 会自动退回 MQ initial snapshot 兜底落库。
     */
    static PersistOrderSnapshotPort noop() {
        return (orgCode, outOrderSn, orderId) -> Optional.empty();
    }
}
