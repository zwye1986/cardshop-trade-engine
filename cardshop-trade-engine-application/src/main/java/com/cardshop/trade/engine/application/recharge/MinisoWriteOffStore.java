package com.cardshop.trade.engine.application.recharge;

/** 持久接收与恢复端口；参见docs/交行直充业务系统详细设计.md「9.5」，accept正常返回时接收事务已提交。 */
public interface MinisoWriteOffStore {
    void accept(MinisoWriteOffNotification notice);

    MinisoWriteOffBatchResult processBatch(
            int shardTotal, int shardIndex, int limit, String workerId);
}
