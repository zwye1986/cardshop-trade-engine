package com.cardshop.trade.engine.application.recharge;

/**
 * Redis WAIT 副本确认结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「4. Application 用例编排层」
 * 和 {@code docs/交行直充业务系统高可用方案设计.md}「14.2 Redis WAIT 协议」。
 * WAIT 只表达 Redis 写入被多少副本确认，不等同于 AOF fsync；上层必须基于该结果决定是否继续 MQ 投递。
 *
 * @param confirmed 是否达到配置要求的副本确认数
 * @param replicaCount 实际确认副本数
 * @param reason 未达标或异常原因
 */
public record ReplicationWaitResult(boolean confirmed, long replicaCount, String reason) {

    public ReplicationWaitResult {
        reason = reason == null ? "" : reason;
    }

    public static ReplicationWaitResult confirmed(long replicaCount) {
        return new ReplicationWaitResult(true, replicaCount, "");
    }

    public static ReplicationWaitResult missed(long replicaCount, String reason) {
        return new ReplicationWaitResult(false, replicaCount, reason);
    }
}
