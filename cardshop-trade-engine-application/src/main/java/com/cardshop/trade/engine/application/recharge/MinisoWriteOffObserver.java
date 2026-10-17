package com.cardshop.trade.engine.application.recharge;

/** 核销低基数可观测端口，框架指标留在基础设施；参见docs/交行直充业务系统详细设计.md「9.5」。 */
public interface MinisoWriteOffObserver {
    MinisoWriteOffObserver NOOP = new MinisoWriteOffObserver() {};

    default void started() {}

    default void event(String category) {}

    default void batch(MinisoWriteOffBatchResult result) {}
}
