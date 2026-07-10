package com.cardshop.trade.engine.application.recharge;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis pending 到 RabbitMQ 的 MQ 补投服务。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「17.3 MQ 补投任务」。本服务只做补投编排：
 * pending 扫描、claim 锁、MySQL 二次确认和 Redis 状态推进由 {@link PendingPersistOrderPort} 实现；
 * RabbitMQ confirm、mandatory return 和重试细节由 {@link PersistRechargeOrderMessagePublisher} 实现。
 */
public class RechargePersistMqRepublishService {

    private static final Logger log = LoggerFactory.getLogger(RechargePersistMqRepublishService.class);
    private static final int DEFAULT_MAX_IN_FLIGHT_PUBLISHES = 32;

    private final PendingPersistOrderPort pendingPort;
    private final PersistRechargeOrderMessagePublisher publisher;
    private final int maxInFlightPublishes;

    public RechargePersistMqRepublishService(
            PendingPersistOrderPort pendingPort,
            PersistRechargeOrderMessagePublisher publisher) {
        this(pendingPort, publisher, DEFAULT_MAX_IN_FLIGHT_PUBLISHES);
    }

    public RechargePersistMqRepublishService(
            PendingPersistOrderPort pendingPort,
            PersistRechargeOrderMessagePublisher publisher,
            int maxInFlightPublishes) {
        this.pendingPort = Objects.requireNonNull(pendingPort, "pendingPort must not be null");
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        if (maxInFlightPublishes <= 0) {
            throw new IllegalArgumentException("maxInFlightPublishes must be positive");
        }
        this.maxInFlightPublishes = maxInFlightPublishes;
    }

    /**
     * 补投当前分片到期 pending 订单。
     *
     * <p>补偿任务属于后台高频路径，单个订单发布失败按 warn 记录并留给下一轮重试，避免 broker 短时抖动时形成
     * error 日志风暴；若发布调用本身抛异常，会写 error 并保留订单在 pending 中。
     */
    public RechargeCompensationBatchResult republishDue(int shardTotal, int shardIndex, int limit, String workerId) {
        var snapshots = pendingPort.selectDueForShard(shardTotal, shardIndex, limit, workerId);
        RechargeCompensationBatchResult result = RechargeCompensationBatchResult.empty().scanned(snapshots.size());
        for (int start = 0; start < snapshots.size(); start += maxInFlightPublishes) {
            int end = Math.min(start + maxInFlightPublishes, snapshots.size());
            result = publishWindow(snapshots.subList(start, end), workerId, result);
        }
        return result;
    }

    private RechargeCompensationBatchResult publishWindow(
            List<PendingPersistOrderSnapshot> snapshots,
            String workerId,
            RechargeCompensationBatchResult result) {
        /*
         * docs/交行直充业务系统高可用方案设计.md「17.3 MQ 补投任务」要求补投任务可消化积压。
         * publisher 内部会等待 confirm，若逐条 join，单批耗时会退化成 limit * confirmTimeout。
         * 这里按固定窗口启动多个发布请求，再按快照顺序收敛结果，既降低 XXL-Job 阻塞时间，也限制
         * RabbitMQ in-flight 压力。
         */
        List<PublishAttempt> attempts = new ArrayList<>(snapshots.size());
        for (PendingPersistOrderSnapshot snapshot : snapshots) {
            try {
                CompletableFuture<PersistMessagePublishResult> future =
                        publisher.publish(snapshot.toPersistMessage()).toCompletableFuture();
                attempts.add(new PublishAttempt(snapshot, future));
            } catch (RuntimeException ex) {
                result = markPublishException(snapshot, workerId, ex, result);
            }
        }
        for (PublishAttempt attempt : attempts) {
            result = settlePublishAttempt(attempt, workerId, result);
        }
        return result;
    }

    private RechargeCompensationBatchResult settlePublishAttempt(
            PublishAttempt attempt,
            String workerId,
            RechargeCompensationBatchResult result) {
        PendingPersistOrderSnapshot snapshot = attempt.snapshot();
        try {
            PersistMessagePublishResult publishResult = attempt.future().join();
            if (publishResult.confirmed()) {
                pendingPort.markRepublished(snapshot, publishResult, workerId);
                return result.succeedOne();
            }
            log.warn("Redis pending MQ补投未被broker确认 orgCode={}, orderId={}, messageId={}, failCode={}, failReason={}",
                    snapshot.orgCode(),
                    snapshot.orderId(),
                    snapshot.messageId(),
                    publishResult.failCode(),
                    publishResult.failReason());
            pendingPort.markRepublishFailed(snapshot, publishResult.failReason(), workerId);
            return result.failOne();
        } catch (RuntimeException ex) {
            return markPublishException(snapshot, workerId, ex, result);
        }
    }

    private RechargeCompensationBatchResult markPublishException(
            PendingPersistOrderSnapshot snapshot,
            String workerId,
            RuntimeException ex,
            RechargeCompensationBatchResult result) {
        log.error("Redis pending MQ补投异常 orgCode={}, orderId={}, messageId={}, workerId={}",
                snapshot.orgCode(), snapshot.orderId(), snapshot.messageId(), workerId, ex);
        pendingPort.markRepublishFailed(snapshot, ex.getMessage(), workerId);
        return result.failOne();
    }

    private record PublishAttempt(
            PendingPersistOrderSnapshot snapshot,
            CompletableFuture<PersistMessagePublishResult> future) {
    }
}
