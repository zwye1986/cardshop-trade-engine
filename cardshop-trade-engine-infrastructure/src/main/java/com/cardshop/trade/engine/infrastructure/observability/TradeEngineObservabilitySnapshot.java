package com.cardshop.trade.engine.infrastructure.observability;

import java.util.Map;

/**
 * 交易引擎业务监控快照。
 *
 * <p>参见 {@code docs/开发进度.md}「5.1 监控告警最小闭环执行边界」。
 * 该对象只保存可安全进入 Prometheus 的聚合值：不包含订单号、商户订单号、商户号、手机号、账号或密钥。
 * Redis/MySQL 指标由后台低频采样生成快照，Micrometer scrape 只读取本对象，避免每次 scrape 都扫描共享旧库。
 */
public record TradeEngineObservabilitySnapshot(
        RedisPending redisPending,
        Map<OutboxKey, CountAge> outbox,
        Map<AcceptAnomalyKey, Long> acceptAnomalies,
        long balanceReconcileDiffOrgs,
        boolean mqPublishBreakerOpen) {

    public TradeEngineObservabilitySnapshot {
        redisPending = redisPending == null ? RedisPending.empty() : redisPending;
        outbox = outbox == null ? Map.of() : Map.copyOf(outbox);
        acceptAnomalies = acceptAnomalies == null ? Map.of() : Map.copyOf(acceptAnomalies);
        balanceReconcileDiffOrgs = Math.max(0, balanceReconcileDiffOrgs);
    }

    public static TradeEngineObservabilitySnapshot empty() {
        return new TradeEngineObservabilitySnapshot(RedisPending.empty(), Map.of(), Map.of(), 0, false);
    }

    public double outboxCount(String eventType, String status) {
        return outbox.getOrDefault(new OutboxKey(eventType, status), CountAge.empty()).count();
    }

    public double outboxMaxAgeSeconds(String eventType, String status) {
        return outbox.getOrDefault(new OutboxKey(eventType, status), CountAge.empty()).maxAgeSeconds();
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
     */
    public record CountAge(long count, long maxAgeSeconds) {

        public CountAge {
            count = Math.max(0, count);
            maxAgeSeconds = Math.max(0, maxAgeSeconds);
        }

        public static CountAge empty() {
            return new CountAge(0, 0);
        }
    }

    /**
     * outbox 聚合键，只允许事件类型和状态这类低基数字段。
     */
    public record OutboxKey(String eventType, String status) {
    }

    /**
     * 接单异常聚合键，只允许异常类型和状态这类低基数字段。
     */
    public record AcceptAnomalyKey(String anomalyType, String status) {
    }
}
