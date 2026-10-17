package com.cardshop.trade.engine.job.recharge;

import com.cardshop.trade.engine.application.recharge.*;

import java.util.Objects;

/** 核销事件恢复门面，分片参数不静默截断；参见docs/交行直充业务系统详细设计.md「9.5」。 */
public final class MinisoWriteOffProcessJob {
    private final MinisoWriteOffStore store;

    public MinisoWriteOffProcessJob(MinisoWriteOffStore store) {
        this.store = Objects.requireNonNull(store);
    }

    public MinisoWriteOffBatchResult runOnce(
            int shardTotal, int shardIndex, int limit, String workerId) {
        if (shardTotal < 1
                || shardTotal > 1024
                || shardIndex < 0
                || shardIndex >= shardTotal
                || limit < 1
                || limit > 1000
                || workerId == null
                || !workerId.matches("[A-Za-z0-9._:-]{1,80}"))
            throw new IllegalArgumentException("Invalid MINISO job parameters");
        return store.processBatch(shardTotal, shardIndex, limit, workerId);
    }
}
