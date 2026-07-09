package com.cardshop.trade.engine.job.recharge;

/**
 * 补偿任务分片参数归一化工具。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.2 分片规则」。XXL-Job 或人工触发
 * 可能传入异常分片/批量参数；job facade 在进入 application service 前统一截断，防止后台任务全量扫描。
 */
final class RechargeJobParameters {

    static final int DEFAULT_LIMIT = 500;
    private static final int MAX_LIMIT = 2_000;

    private RechargeJobParameters() {
    }

    static int shardTotal(int shardTotal) {
        return Math.max(1, shardTotal);
    }

    static int shardIndex(int shardTotal, int shardIndex) {
        int normalizedTotal = shardTotal(shardTotal);
        if (shardIndex < 0) {
            return 0;
        }
        return Math.min(shardIndex, normalizedTotal - 1);
    }

    static int limit(int limit) {
        if (limit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }

    static String workerId(String workerId) {
        return workerId == null || workerId.isBlank() ? "unknown-worker" : workerId;
    }
}
