package com.cardshop.trade.engine.application.recharge;

/**
 * MQ publisher confirm 成功后 Redis 持久化状态推进结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.5 下单服务实现细节」第 13 步。
 * 该结果只表示 Redis 订单快照是否成功从 {@code PENDING_MQ} 推进到 {@code PERSIST_QUEUED}，
 * 不代表 MySQL 已落库，pending 索引仍需保留给后续 Consumer/补偿任务清理。
 *
 * @param marked 是否标记成功
 * @param failCode 标记失败码
 * @param failReason 标记失败原因
 */
public record MarkPersistQueuedResult(boolean marked, String failCode, String failReason) {

    public MarkPersistQueuedResult {
        failCode = failCode == null ? "" : failCode;
        failReason = failReason == null ? "" : failReason;
    }

    public static MarkPersistQueuedResult success() {
        return new MarkPersistQueuedResult(true, "", "");
    }

    public static MarkPersistQueuedResult failed(String failCode, String failReason) {
        return new MarkPersistQueuedResult(false, failCode, failReason);
    }
}
