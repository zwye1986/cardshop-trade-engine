package com.cardshop.trade.engine.application.recharge;

/**
 * Persist Consumer 核心服务处理结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。
 * 当前 D7A 还不直接绑定 Rabbit listener，因此先用该结果明确后续 adapter 的 ack/nack 决策。
 *
 * @param status 单条消息处理状态
 * @param persistSource 实际使用的快照来源
 * @param shouldAck 是否允许 Rabbit adapter ack 当前消息
 * @param failCode 失败码，成功时为空
 * @param failReason 失败原因，成功时为空
 */
public record PersistRechargeOrderResult(
        PersistRechargeOrderStatus status,
        PersistSource persistSource,
        boolean shouldAck,
        String failCode,
        String failReason) {

    public PersistRechargeOrderResult {
        failCode = failCode == null ? "" : failCode;
        failReason = failReason == null ? "" : failReason;
    }

    public static PersistRechargeOrderResult persisted(PersistSource persistSource) {
        return new PersistRechargeOrderResult(
                PersistRechargeOrderStatus.PERSISTED,
                persistSource,
                true,
                "",
                "");
    }

    public static PersistRechargeOrderResult persistedRedisMarkFailed(PersistSource persistSource, String failReason) {
        return new PersistRechargeOrderResult(
                PersistRechargeOrderStatus.PERSISTED_REDIS_MARK_FAILED,
                persistSource,
                true,
                "REDIS_MARK_DB_PERSISTED_FAILED",
                failReason);
    }

    public static PersistRechargeOrderResult persistFailed(
            PersistSource persistSource,
            String failCode,
            String failReason) {
        return new PersistRechargeOrderResult(
                PersistRechargeOrderStatus.PERSIST_FAILED,
                persistSource,
                false,
                failCode,
                failReason);
    }
}
