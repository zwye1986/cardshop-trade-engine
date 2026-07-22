package com.cardshop.trade.engine.infrastructure.observability;

import java.util.Map;

/**
 * 交易引擎业务监控快照。
 *
 * <p>参见 {@code docs/开发进度.md}「5.1 监控告警最小闭环执行边界」与
 * {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」「9.2 余额 Delta 刷账 Outbox」。
 * 该对象只保存可安全进入 Prometheus 的聚合值：不包含订单号、商户订单号、商户号、手机号、账号或密钥。
 * Redis/MySQL 指标由后台低频采样生成快照，Micrometer scrape 只读取本对象，避免每次 scrape 都扫描共享旧库。
 *
 * <p>拆表后：{@code outbox} 仅承载派单表按 {@code event_type+status} 聚合；
 * {@code balanceDelta} 仅按 {@code status} 聚合余额刷账积压，禁止把 {@code org_code} 做成标签。
 */
public record TradeEngineObservabilitySnapshot(
        RedisPending redisPending,
        Map<OutboxKey, CountAge> outbox,
        Map<String, CountAge> balanceDelta,
        Map<AcceptAnomalyKey, Long> acceptAnomalies,
        long balanceReconcileDiffOrgs,
        boolean mqPublishBreakerOpen) {

    public TradeEngineObservabilitySnapshot {
        redisPending = redisPending == null ? RedisPending.empty() : redisPending;
        outbox = outbox == null ? Map.of() : Map.copyOf(outbox);
        balanceDelta = balanceDelta == null ? Map.of() : Map.copyOf(balanceDelta);
        acceptAnomalies = acceptAnomalies == null ? Map.of() : Map.copyOf(acceptAnomalies);
        balanceReconcileDiffOrgs = Math.max(0, balanceReconcileDiffOrgs);
    }

    public static TradeEngineObservabilitySnapshot empty() {
        return new TradeEngineObservabilitySnapshot(
                RedisPending.empty(), Map.of(), Map.of(), Map.of(), 0, false);
    }

    public double outboxCount(String eventType, String status) {
        return outbox.getOrDefault(new OutboxKey(eventType, status), CountAge.empty()).count();
    }

    public double outboxMaxAgeSeconds(String eventType, String status) {
        return outbox.getOrDefault(new OutboxKey(eventType, status), CountAge.empty()).maxAgeSeconds();
    }

    public double outboxMaxAgeSinceDueSeconds(String eventType, String status) {
        return outbox.getOrDefault(new OutboxKey(eventType, status), CountAge.empty()).maxAgeSinceDueSeconds();
    }

    public double balanceDeltaCount(String status) {
        return balanceDelta.getOrDefault(status, CountAge.empty()).count();
    }

    public double balanceDeltaMaxAgeSeconds(String status) {
        return balanceDelta.getOrDefault(status, CountAge.empty()).maxAgeSeconds();
    }

    public double acceptAnomalyCount(String anomalyType, String status) {
        return acceptAnomalies.getOrDefault(new AcceptAnomalyKey(anomalyType, status), 0L);
    }

    /**
     * Redis pending 聚合值。
     *
     * <p>参见高可用设计「17.3 MQ 补投任务」。超过保护窗口的 pending 代表 Redis 已接单但 MySQL
     * 最终事实尚未确认，是监控告警最小闭环的核心入口。
     */
    public record RedisPending(long total, long due, long maxAgeSeconds) {

        public RedisPending {
            total = Math.max(0, total);
            due = Math.max(0, due);
            maxAgeSeconds = Math.max(0, maxAgeSeconds);
        }

        public static RedisPending empty() {
            return new RedisPending(0, 0, 0);
        }
    }

    /**
     * 带最大滞留时间的计数。
     *
     * @param maxAgeSeconds 自创建以来的最大滞留秒数（兼容旧指标）
     * @param maxAgeSinceDueSeconds 仅 due 行的最大到期年龄秒数
     */
    public record CountAge(long count, long maxAgeSeconds, long maxAgeSinceDueSeconds) {

        public CountAge {
            count = Math.max(0, count);
            maxAgeSeconds = Math.max(0, maxAgeSeconds);
            maxAgeSinceDueSeconds = Math.max(0, maxAgeSinceDueSeconds);
        }

        public static CountAge empty() {
            return new CountAge(0, 0, 0);
        }
    }

    /**
     * 派单 outbox 聚合键，只允许事件类型和状态这类低基数字段。
     */
    public record OutboxKey(String eventType, String status) {
    }

    /**
     * 接单异常聚合键，只允许异常类型和状态这类低基数字段。
     */
    public record AcceptAnomalyKey(String anomalyType, String status) {
    }
}
