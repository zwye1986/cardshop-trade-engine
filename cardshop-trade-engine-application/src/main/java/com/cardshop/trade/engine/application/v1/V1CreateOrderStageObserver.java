package com.cardshop.trade.engine.application.v1;

import java.time.Duration;

/**
 * V1 下单同步路径子阶段耗时观测端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「12. 可观测性」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「3.7 线程池与隔离」。
 * stage 仅允许低基数枚举，禁止订单号、商户号进入标签。
 */
public interface V1CreateOrderStageObserver {

    String STAGE_PREPARE = "prepare";
    String STAGE_ACCEPT = "accept";
    String STAGE_WAIT = "wait";
    String STAGE_MQ_CONFIRM = "mq_confirm";
    String STAGE_MARK_PERSIST_QUEUED = "mark_persist_queued";
    String STAGE_REDIS_SESSION_CONNECT = "redis_session_connect";
    String STAGE_SYNC_TOTAL = "sync_total";

    void recordStage(String stage, Duration duration);

    static V1CreateOrderStageObserver noop() {
        return (stage, duration) -> {
        };
    }
}
