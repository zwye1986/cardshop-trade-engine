package com.cardshop.trade.engine.job.recharge;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyAutoCheckService;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckBatchResult;
import java.util.Objects;

/**
 * 接单异常自动核查任务入口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.3 Redis HA Guard」、
 * 「17. 分片补偿与对账任务」和「20.13.6 Redis Lua 下单结果」。该类刻意不直接依赖 XXL-Job：
 * D5B 先沉淀稳定的任务调用边界，后续接入调度框架时只需要把分片参数和批量大小解析后委托到这里，
 * 避免调度框架 API 泄漏到 application 层。
 */
public class AcceptAnomalyAutoCheckJob {

    public static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 500;

    private final AcceptAnomalyAutoCheckService autoCheckService;

    public AcceptAnomalyAutoCheckJob(AcceptAnomalyAutoCheckService autoCheckService) {
        this.autoCheckService = Objects.requireNonNull(autoCheckService, "autoCheckService must not be null");
    }

    /**
     * 执行一批接单异常自动核查。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「17. 分片补偿与对账任务」。调度侧可能传入空参数、
     * 0 或异常大的批量大小；这里统一归一化，既保证任务默认能推进，也避免单次扫描过大拖慢交易库。
     *
     * @param limit 单批核查上限，非正数使用 {@link #DEFAULT_LIMIT}，超过内部上限时截断到 500
     * @param workerId 调度节点或分片标识，用于写入 resolved_by 和排查任务来源
     */
    public AcceptAnomalyCheckBatchResult runOnce(int limit, String workerId) {
        return autoCheckService.checkBatch(normalizeLimit(limit), workerId);
    }

    private static int normalizeLimit(int limit) {
        if (limit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }
}
