package com.cardshop.trade.engine.application.recharge;

/** 核销恢复扫描结果，保留失败与延期计数；参见docs/交行直充业务系统详细设计.md「9.5」。 */
public record MinisoWriteOffBatchResult(
        int scanned, int processed, int retried, int manual, int deferred, int failed) {}
