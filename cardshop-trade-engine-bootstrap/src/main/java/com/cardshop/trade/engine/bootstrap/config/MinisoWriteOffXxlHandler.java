package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.job.recharge.MinisoWriteOffProcessJob;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;

import org.slf4j.*;

/** 核销恢复真实XXL入口，受控参数只接受limit/workerId；参见docs/交行直充业务系统详细设计.md「9.5」、10.1。 */
public final class MinisoWriteOffXxlHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MinisoWriteOffXxlHandler.class);
    private final MinisoWriteOffProcessJob job;
    private final int batchSize;
    private final LogStormGuard logs = LogStormGuard.defaultGuard();

    public MinisoWriteOffXxlHandler(MinisoWriteOffProcessJob job, int batchSize) {
        this.job = job;
        this.batchSize = batchSize;
    }

    @XxlJob("minisoWriteOffProcessJob")
    public void execute() {
        try {
            var result =
                    runOnce(
                            Math.max(1, XxlJobHelper.getShardTotal()),
                            Math.max(0, XxlJobHelper.getShardIndex()),
                            XxlJobHelper.getJobParam());
            XxlJobHelper.log("名创核销处理 {}", result);
            if (result.failed() > 0 || result.manual() > 0)
                XxlJobHelper.handleFail("MINISO events require attention");
            else XxlJobHelper.handleSuccess();
        } catch (RuntimeException ex) {
            // 调度轮询可高频触发；只记录安全分类，防止SQL/参数原文泄密及日志风暴。
            if (logs.shouldLog("miniso.job.failed").shouldLog())
                LOG.error(
                        "名创核销恢复任务失败",
                        new MinisoWriteOffException(MinisoWriteOffException.Reason.DB_UNAVAILABLE));
            XxlJobHelper.handleFail("MINISO processing failed");
        }
    }

    public MinisoWriteOffBatchResult runOnce(int shards, int shard, String raw) {
        int limit = batchSize;
        String worker = "xxl-miniso";
        if (raw != null && !raw.isBlank())
            for (String part : raw.split(",")) {
                var pair = part.trim().split("=", 2);
                if (pair.length != 2)
                    throw new IllegalArgumentException("Invalid MINISO job parameters");
                switch (pair[0]) {
                    case "limit" -> limit = Integer.parseInt(pair[1]);
                    case "workerId" -> worker = pair[1];
                    default -> throw new IllegalArgumentException("Invalid MINISO job parameters");
                }
            }
        return job.runOnce(shards, shard, limit, worker);
    }
}
