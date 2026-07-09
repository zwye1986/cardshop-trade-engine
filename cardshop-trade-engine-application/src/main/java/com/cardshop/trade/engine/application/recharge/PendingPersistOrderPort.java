package com.cardshop.trade.engine.application.recharge;

import java.util.List;

/**
 * Redis pending 订单补投端口。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.3 MQ 补投任务」。真实实现需要按
 * org_index bucket、订单级 claim 锁和二次确认扫描 Redis；application 层只编排“选出快照 -> 发布 MQ ->
 * 根据 confirm 结果刷新 pending 状态”，避免补偿任务直接依赖 Redis 命令细节。
 */
public interface PendingPersistOrderPort {

    /**
     * 扫描当前分片到期的 pending 订单快照。
     *
     * <p>{@code workerId} 用于基础设施层写入 Redis 订单级 claim 锁。参见
     * {@code docs/交行直充业务系统详细设计.md}「20.11 XXL-Job Handler 契约」第 5-6 步。
     */
    List<PendingPersistOrderSnapshot> selectDueForShard(int shardTotal, int shardIndex, int limit, String workerId);

    /**
     * MQ 补投 confirm 成功后刷新 Redis pending 状态。
     */
    void markRepublished(PendingPersistOrderSnapshot snapshot, PersistMessagePublishResult result, String workerId);

    /**
     * MQ 补投失败后刷新失败原因和下次重试入口。
     */
    void markRepublishFailed(PendingPersistOrderSnapshot snapshot, String reason, String workerId);

    /**
     * 未接入 Redis pending 扫描时的空实现，用于 local profile 或局部测试。
     */
    static PendingPersistOrderPort noop() {
        return new PendingPersistOrderPort() {
            @Override
            public List<PendingPersistOrderSnapshot> selectDueForShard(
                    int shardTotal,
                    int shardIndex,
                    int limit,
                    String workerId) {
                return List.of();
            }

            @Override
            public void markRepublished(
                    PendingPersistOrderSnapshot snapshot,
                    PersistMessagePublishResult result,
                    String workerId) {
            }

            @Override
            public void markRepublishFailed(PendingPersistOrderSnapshot snapshot, String reason, String workerId) {
            }
        };
    }
}
