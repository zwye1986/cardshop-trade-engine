package com.cardshop.trade.engine.application.recharge;

/**
 * Persist Consumer 批处理结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。Rabbit 批 listener
 * 只依据 {@link #shouldAck()} 决定整批 ack 或进入 retry/DLQ；MySQL 事务、Redis mark 和幂等细节都留在 application 层。
 *
 * @param batchSize 本次 Rabbit 批次消息数
 * @param persistedCount 已成功提交 MySQL 的订单数
 * @param redisMarkFailedCount MySQL 已提交后 Redis mark 失败的订单数
 * @param shouldAck 是否允许 Rabbit adapter ack 原批次
 * @param failCode 失败码，成功时为空
 * @param failReason 失败原因摘要，成功时为空
 */
public record PersistRechargeOrderBatchResult(
        int batchSize,
        int persistedCount,
        int redisMarkFailedCount,
        boolean shouldAck,
        String failCode,
        String failReason) {

    public PersistRechargeOrderBatchResult {
        if (batchSize < 0) {
            throw new IllegalArgumentException("batchSize must be >= 0");
        }
        if (persistedCount < 0) {
            throw new IllegalArgumentException("persistedCount must be >= 0");
        }
        if (redisMarkFailedCount < 0) {
            throw new IllegalArgumentException("redisMarkFailedCount must be >= 0");
        }
        failCode = failCode == null ? "" : failCode;
        failReason = failReason == null ? "" : failReason;
    }

    public static PersistRechargeOrderBatchResult persisted(
            int batchSize,
            int persistedCount,
            int redisMarkFailedCount) {
        String failCode = redisMarkFailedCount > 0
                ? PersistRechargeOrderResult.FAIL_CODE_REDIS_MARK_DB_PERSISTED_FAILED
                : "";
        String failReason = redisMarkFailedCount > 0
                ? "Redis markDbPersisted failed for %d orders".formatted(redisMarkFailedCount)
                : "";
        return new PersistRechargeOrderBatchResult(
                batchSize,
                persistedCount,
                redisMarkFailedCount,
                true,
                failCode,
                failReason);
    }

    public static PersistRechargeOrderBatchResult persistFailed(
            int batchSize,
            String failCode,
            String failReason) {
        return new PersistRechargeOrderBatchResult(
                batchSize,
                0,
                0,
                false,
                failCode,
                failReason);
    }
}
