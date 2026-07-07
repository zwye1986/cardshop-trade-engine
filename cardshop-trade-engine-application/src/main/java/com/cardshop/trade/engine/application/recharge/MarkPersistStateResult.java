package com.cardshop.trade.engine.application.recharge;

/**
 * Redis 持久化状态推进结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。
 * Consumer 或补偿任务在 MySQL 事务提交/失败后，会通过 `markDbPersisted.lua` 或
 * `markPersistFailed.lua` 推进 Redis 订单快照；该结果只描述 Redis 状态推进是否成功，不代表 MySQL 事务本身。
 *
 * @param marked 是否标记成功
 * @param failCode 标记失败码
 * @param failReason 标记失败原因
 */
public record MarkPersistStateResult(boolean marked, String failCode, String failReason) {

    public static final String FAIL_CODE_REDIS_SESSION_UNAVAILABLE = "REDIS_SESSION_UNAVAILABLE";
    public static final String FAIL_CODE_MARK_DB_PERSISTED_LUA_ERROR = "MARK_DB_PERSISTED_LUA_ERROR";
    public static final String FAIL_CODE_MARK_PERSIST_FAILED_LUA_ERROR = "MARK_PERSIST_FAILED_LUA_ERROR";

    public MarkPersistStateResult {
        failCode = failCode == null ? "" : failCode;
        failReason = failReason == null ? "" : failReason;
    }

    public static MarkPersistStateResult success() {
        return new MarkPersistStateResult(true, "", "");
    }

    public static MarkPersistStateResult failed(String failCode, String failReason) {
        return new MarkPersistStateResult(false, failCode, failReason);
    }
}
