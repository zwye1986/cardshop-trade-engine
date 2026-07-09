package com.cardshop.trade.engine.application.recharge;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis pending 到 RabbitMQ 的 MQ 补投服务。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.11 XXL-Job Handler 契约」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「17.3 MQ 补投任务」。本服务只做补投编排：
 * pending 扫描、claim 锁、MySQL 二次确认和 Redis 状态推进由 {@link PendingPersistOrderPort} 实现；
 * RabbitMQ confirm、mandatory return 和重试细节由 {@link PersistRechargeOrderMessagePublisher} 实现。
 */
public class RechargePersistMqRepublishService {

    private static final Logger log = LoggerFactory.getLogger(RechargePersistMqRepublishService.class);

    private final PendingPersistOrderPort pendingPort;
    private final PersistRechargeOrderMessagePublisher publisher;

    public RechargePersistMqRepublishService(
            PendingPersistOrderPort pendingPort,
            PersistRechargeOrderMessagePublisher publisher) {
        this.pendingPort = Objects.requireNonNull(pendingPort, "pendingPort must not be null");
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
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
        for (PendingPersistOrderSnapshot snapshot : snapshots) {
            try {
                PersistMessagePublishResult publishResult = publisher.publish(snapshot.toPersistMessage())
                        .toCompletableFuture()
                        .join();
                if (publishResult.confirmed()) {
                    pendingPort.markRepublished(snapshot, publishResult, workerId);
                    result = result.succeedOne();
                } else {
                    log.warn("Redis pending MQ补投未被broker确认 orgCode={}, orderId={}, messageId={}, failCode={}, failReason={}",
                            snapshot.orgCode(),
                            snapshot.orderId(),
                            snapshot.messageId(),
                            publishResult.failCode(),
                            publishResult.failReason());
                    pendingPort.markRepublishFailed(snapshot, publishResult.failReason(), workerId);
                    result = result.failOne();
                }
            } catch (RuntimeException ex) {
                log.error("Redis pending MQ补投异常 orgCode={}, orderId={}, messageId={}, workerId={}",
                        snapshot.orgCode(), snapshot.orderId(), snapshot.messageId(), workerId, ex);
                pendingPort.markRepublishFailed(snapshot, ex.getMessage(), workerId);
                result = result.failOne();
            }
        }
        return result;
    }
}
