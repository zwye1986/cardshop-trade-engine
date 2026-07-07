package com.cardshop.trade.engine.application.recharge;

import java.util.Objects;

/**
 * Redis-first 下单受理结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
 * 该对象只表达 Redis 接单阶段的结果，不混入 WAIT、MQ confirm、MySQL 落库或供应商派单状态。
 *
 * @param status Lua 受理结果码
 * @param orderId 平台订单号；重复订单可能返回已存在订单号
 * @param validationStatus 业务校验结果，通常为 PASSED 或 FAILED
 * @param failCode 失败码
 * @param failReason 失败原因
 * @param dispatchAllowed 是否允许后续派单
 * @param balanceReserved 是否已经完成 Redis 余额预扣
 * @param orderVersion Redis 订单版本，初始受理为 1
 */
public record AcceptRechargeOrderResult(
        AcceptRechargeOrderStatus status,
        String orderId,
        String validationStatus,
        String failCode,
        String failReason,
        boolean dispatchAllowed,
        boolean balanceReserved,
        long orderVersion) {

    public static final String FAIL_CODE_REDIS_WRITE_DISABLED = AcceptRechargeOrderStatus.REDIS_WRITE_DISABLED.name();
    public static final String FAIL_CODE_REDIS_UNAVAILABLE = AcceptRechargeOrderStatus.REDIS_UNAVAILABLE.name();
    public static final String FAIL_CODE_UNKNOWN = AcceptRechargeOrderStatus.UNKNOWN.name();

    public AcceptRechargeOrderResult {
        status = Objects.requireNonNull(status, "status");
        orderId = orderId == null ? "" : orderId;
        validationStatus = validationStatus == null ? "" : validationStatus;
        failCode = failCode == null ? "" : failCode;
        failReason = failReason == null ? "" : failReason;
    }

    /**
     * Redis 写入开关或 HA Guard 已关闭时的统一失败结果。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。该失败发生在 Lua 写入前，
     * 因此 `orderWritten=false`，调用方不能执行 WAIT、MQ confirm 或作废回滚。
     */
    public static AcceptRechargeOrderResult redisWriteDisabled(String orderId) {
        return new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.REDIS_WRITE_DISABLED,
                orderId,
                "",
                FAIL_CODE_REDIS_WRITE_DISABLED,
                "Redis 写入开关关闭",
                false,
                false,
                0);
    }

    /**
     * Redis 客户端、session 或 Lua 调用不可用时的统一失败结果。
     *
     * <p>该失败码用于 fail-closed 路径，避免不同适配器散落硬编码字符串导致接口语义漂移。
     */
    public static AcceptRechargeOrderResult redisUnavailable(String orderId, String failReason) {
        return new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.REDIS_UNAVAILABLE,
                orderId,
                "",
                FAIL_CODE_REDIS_UNAVAILABLE,
                failReason,
                false,
                false,
                0);
    }

    /**
     * 返回 Redis 是否已写订单快照，供后续 D5/D6 失败处理判断能否重试或代理。
     */
    public boolean orderWritten() {
        return status.isOrderWritten();
    }
}
