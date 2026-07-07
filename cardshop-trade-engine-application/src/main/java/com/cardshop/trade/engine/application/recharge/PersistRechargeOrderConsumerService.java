package com.cardshop.trade.engine.application.recharge;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis/MQ 到 MySQL 的异步落库 Consumer 核心服务。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。
 * 本类不直接绑定 RabbitMQ listener，也不直接依赖 MyBatis 事务；它只负责选择快照、调用事务写库端口、
 * 并在事务提交后推进 Redis `DB_PERSISTED` 标记。后续 Rabbit adapter 可以根据返回结果决定 ack/nack。
 */
public class PersistRechargeOrderConsumerService {

    private static final Logger log = LoggerFactory.getLogger(PersistRechargeOrderConsumerService.class);
    private static final String MYSQL_PERSIST_FAILED = "MYSQL_PERSIST_FAILED";
    private static final long RETRY_DELAY_MILLIS = 60_000L;

    private final PersistOrderSnapshotPort snapshotPort;
    private final RechargeOrderPersistencePort persistencePort;
    private final RechargeAcceptReliabilityPort reliabilityPort;
    private final Clock clock;

    public PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            Clock clock) {
        this.snapshotPort = Objects.requireNonNull(snapshotPort, "snapshotPort must not be null");
        this.persistencePort = Objects.requireNonNull(persistencePort, "persistencePort must not be null");
        this.reliabilityPort = Objects.requireNonNull(reliabilityPort, "reliabilityPort must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * 处理一条持久化 MQ 消息。
     *
     * <p>正常路径严格按「读取快照 -> MySQL 事务提交 -> Redis markDbPersisted」推进。
     * MySQL 写库失败时必须记录 error 日志，并通过 `markPersistFailed` 保留 Redis pending 补偿入口。
     */
    public PersistRechargeOrderResult persist(PersistTradeOrderMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        SelectedSnapshot selected = selectSnapshot(message);
        OrderPersistAggregate aggregate = new OrderPersistAggregate(
                message.messageId(),
                selected.snapshot(),
                selected.persistSource(),
                clock.instant());
        try {
            persistencePort.persist(aggregate);
        } catch (RuntimeException ex) {
            log.error("Redis订单异步落MySQL失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}, persistSource={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    selected.persistSource(),
                    ex);
            markPersistFailed(message, selected.snapshot(), ex);
            return PersistRechargeOrderResult.persistFailed(
                    selected.persistSource(),
                    MYSQL_PERSIST_FAILED,
                    ex.getMessage());
        }

        MarkPersistStateResult markResult = markDbPersisted(message, selected.snapshot());
        if (!markResult.marked()) {
            log.error("Redis订单异步落MySQL成功但markDbPersisted失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}, failCode={}, failReason={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    markResult.failCode(),
                    markResult.failReason());
            return PersistRechargeOrderResult.persistedRedisMarkFailed(
                    selected.persistSource(),
                    markResult.failReason());
        }
        return PersistRechargeOrderResult.persisted(selected.persistSource());
    }

    private SelectedSnapshot selectSnapshot(PersistTradeOrderMessage message) {
        Optional<PersistOrderSnapshot> redisSnapshot;
        try {
            redisSnapshot = snapshotPort.findLatestSnapshot(
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId());
        } catch (RuntimeException ex) {
            log.error("Redis订单完整快照读取异常，退回MQ initialOrderSnapshot落库 orgCode={}, outOrderSn={}, orderId={}, messageId={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    ex);
            redisSnapshot = Optional.empty();
        }
        if (redisSnapshot.isPresent() && redisSnapshot.get().orderVersion() >= message.orderVersion()) {
            return new SelectedSnapshot(redisSnapshot.get(), PersistSource.REDIS_SNAPSHOT);
        }
        return new SelectedSnapshot(
                PersistOrderSnapshot.fromInitialSnapshot(message),
                PersistSource.MQ_SNAPSHOT_FALLBACK);
    }

    private MarkPersistStateResult markDbPersisted(
            PersistTradeOrderMessage message,
            PersistOrderSnapshot snapshot) {
        long now = clock.millis();
        try {
            return reliabilityPort.markDbPersisted(
                            snapshot.toAcceptCommand(now, now + RETRY_DELAY_MILLIS),
                            snapshot.toAcceptResult(),
                            now)
                    .toCompletableFuture()
                    .join();
        } catch (RuntimeException ex) {
            log.error("Redis订单markDbPersisted调用异常 orgCode={}, outOrderSn={}, orderId={}, messageId={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    ex);
            return MarkPersistStateResult.failed("REDIS_MARK_EXCEPTION", ex.getMessage());
        }
    }

    private void markPersistFailed(
            PersistTradeOrderMessage message,
            PersistOrderSnapshot snapshot,
            RuntimeException cause) {
        long now = clock.millis();
        try {
            reliabilityPort.markPersistFailed(
                            snapshot.toAcceptCommand(now, now + RETRY_DELAY_MILLIS),
                            snapshot.toAcceptResult(),
                            MYSQL_PERSIST_FAILED,
                            cause.getMessage(),
                            1,
                            now + RETRY_DELAY_MILLIS,
                            now)
                    .toCompletableFuture()
                    .join();
        } catch (RuntimeException markEx) {
            log.error("Redis订单异步落库失败后markPersistFailed也失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    markEx);
        }
    }

    private record SelectedSnapshot(PersistOrderSnapshot snapshot, PersistSource persistSource) {
    }
}
