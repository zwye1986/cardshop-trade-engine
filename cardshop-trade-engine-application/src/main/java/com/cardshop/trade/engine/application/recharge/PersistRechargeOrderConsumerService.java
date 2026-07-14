package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.logging.LogStormGuard.LogDecision;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis/MQ 到 MySQL 的异步落库 Consumer 核心服务。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
 * 本类不直接绑定 RabbitMQ listener，也不直接依赖 MyBatis 事务；它只负责选择快照、调用事务写库端口、
 * 并在事务提交后推进 Redis `DB_PERSISTED` 标记。后续 Rabbit adapter 可以根据返回结果决定 ack/nack。
 */
public class PersistRechargeOrderConsumerService {

    private static final Logger log = LoggerFactory.getLogger(PersistRechargeOrderConsumerService.class);
    private static final String MYSQL_PERSIST_FAILED = "MYSQL_PERSIST_FAILED";
    private static final String REDIS_MARK_TIMEOUT = "REDIS_MARK_TIMEOUT";
    private static final String REDIS_MARK_INTERRUPTED = "REDIS_MARK_INTERRUPTED";
    private static final String REDIS_MARK_EXCEPTION = "REDIS_MARK_EXCEPTION";
    private static final String LOG_EVENT_MYSQL_PERSIST_FAILED = "persist-consumer.mysql-persist.failed";
    private static final String LOG_EVENT_REDIS_MARK_AFTER_MYSQL_FAILED = "persist-consumer.redis-mark.after-mysql.failed";
    private static final String LOG_EVENT_REDIS_SNAPSHOT_READ_FAILED = "persist-consumer.redis-snapshot.read.failed";
    private static final String LOG_EVENT_REDIS_MARK_DB_TIMEOUT = "persist-consumer.redis-mark-db.timeout";
    private static final String LOG_EVENT_REDIS_MARK_DB_INTERRUPTED = "persist-consumer.redis-mark-db.interrupted";
    private static final String LOG_EVENT_REDIS_MARK_DB_EXCEPTION = "persist-consumer.redis-mark-db.exception";
    private static final String LOG_EVENT_REDIS_MARK_FAILED_TIMEOUT = "persist-consumer.redis-mark-failed.timeout";
    private static final String LOG_EVENT_REDIS_MARK_FAILED_INTERRUPTED = "persist-consumer.redis-mark-failed.interrupted";
    private static final String LOG_EVENT_REDIS_MARK_FAILED_EXCEPTION = "persist-consumer.redis-mark-failed.exception";
    private static final long RETRY_DELAY_MILLIS = 60_000L;
    private static final Duration DEFAULT_REDIS_MARK_TIMEOUT = Duration.ofSeconds(3);
    private static final int MAX_FAIL_REASON_LENGTH = 500;
    private static final String FIELD_REDIS_ORDER_WRITTEN = "redis_order_written";
    private static final String REDIS_FALSE = "0";

    private final PersistOrderSnapshotPort snapshotPort;
    private final RechargeOrderPersistencePort persistencePort;
    private final RechargeAcceptReliabilityPort reliabilityPort;
    private final Clock clock;
    private final Duration redisMarkTimeout;
    private final LogStormGuard logStormGuard = LogStormGuard.defaultGuard();

    public PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            Clock clock) {
        this(snapshotPort, persistencePort, reliabilityPort, clock, DEFAULT_REDIS_MARK_TIMEOUT);
    }

    /**
     * 创建异步落库 Consumer 核心服务。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
     * `redisMarkTimeout` 只约束 MySQL 事务之后的 Redis 状态推进调用，避免 RabbitMQ consumer 线程因
     * Redis 客户端 hang 住而无限阻塞；MySQL 已提交后的 `markDbPersisted` 超时会 ack 当前消息并交给补偿修复，
     * MySQL 失败后的 `markPersistFailed` 超时则保持当前消息可重试。
     */
    PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            Clock clock,
            Duration redisMarkTimeout) {
        this.snapshotPort = Objects.requireNonNull(snapshotPort, "snapshotPort must not be null");
        this.persistencePort = Objects.requireNonNull(persistencePort, "persistencePort must not be null");
        this.reliabilityPort = Objects.requireNonNull(reliabilityPort, "reliabilityPort must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.redisMarkTimeout = positiveTimeout(redisMarkTimeout);
    }

    /**
     * 处理一条持久化 MQ 消息。
     *
     * <p>正常路径严格按「读取快照 -> MySQL 事务提交 -> Redis markDbPersisted」推进。
     * MySQL 写库失败时必须记录 error 日志，并通过 `markPersistFailed` 保留 Redis pending 补偿入口。
     */
    public PersistRechargeOrderResult persist(PersistTradeOrderMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        boolean redisOrderWritten = redisOrderWritten(message);
        SelectedSnapshot selected = selectSnapshot(message, redisOrderWritten);
        OrderPersistAggregate aggregate = new OrderPersistAggregate(
                message.messageId(),
                selected.snapshot(),
                selected.persistSource(),
                clock.instant());
        try {
            persistencePort.persist(aggregate);
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_MYSQL_PERSIST_FAILED, decision -> log.error(
                    "Redis订单异步落MySQL失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}, persistSource={}, occurrence={}, sampled={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    selected.persistSource(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            String failReason = truncateFailReason(ex.getMessage());
            if (redisOrderWritten) {
                markPersistFailed(message, selected.snapshot(), failReason);
            }
            return PersistRechargeOrderResult.persistFailed(
                    selected.persistSource(),
                    MYSQL_PERSIST_FAILED,
                    failReason);
        }

        if (!redisOrderWritten) {
            return PersistRechargeOrderResult.persisted(selected.persistSource());
        }
        MarkPersistStateResult markResult = markDbPersisted(message, selected.snapshot());
        if (!markResult.marked()) {
            logSampledError(LOG_EVENT_REDIS_MARK_AFTER_MYSQL_FAILED, decision -> log.error(
                    "Redis订单异步落MySQL成功但markDbPersisted失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}, failCode={}, failReason={}, occurrence={}, sampled={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    markResult.failCode(),
                    markResult.failReason(),
                    decision.occurrence(),
                    decision.sampledOccurrence()));
            return PersistRechargeOrderResult.persistedRedisMarkFailed(
                    selected.persistSource(),
                    markResult.failReason());
        }
        return PersistRechargeOrderResult.persisted(selected.persistSource());
    }

    private SelectedSnapshot selectSnapshot(PersistTradeOrderMessage message, boolean redisOrderWritten) {
        /*
         * CONFIG_CHANGED 这类失败发生在 Lua 写 Redis 订单前，Producer 会用 redis_order_written=false 标记
         * “只需要 MySQL 失败事实”。此时 Redis 没有 order hash/pending/幂等快照，不能读取 Redis 快照，也不能在
         * MySQL 提交后执行 markDbPersisted。参见 docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」。
         */
        if (!redisOrderWritten) {
            return new SelectedSnapshot(
                    PersistOrderSnapshot.fromInitialSnapshot(message),
                    PersistSource.MQ_SNAPSHOT_FALLBACK);
        }
        Optional<PersistOrderSnapshot> redisSnapshot;
        try {
            redisSnapshot = snapshotPort.findLatestSnapshot(
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId());
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_REDIS_SNAPSHOT_READ_FAILED, decision -> log.error(
                    "Redis订单完整快照读取异常，退回MQ initialOrderSnapshot落库 orgCode={}, outOrderSn={}, orderId={}, messageId={}, occurrence={}, sampled={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            redisSnapshot = Optional.empty();
        }
        if (redisSnapshot.isPresent() && redisSnapshot.get().orderVersion() >= message.orderVersion()) {
            return new SelectedSnapshot(redisSnapshot.get(), PersistSource.REDIS_SNAPSHOT);
        }
        return new SelectedSnapshot(
                PersistOrderSnapshot.fromInitialSnapshot(message),
                PersistSource.MQ_SNAPSHOT_FALLBACK);
    }

    private static boolean redisOrderWritten(PersistTradeOrderMessage message) {
        Object value = message.initialOrderSnapshot().get(FIELD_REDIS_ORDER_WRITTEN);
        if (value == null) {
            return true;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        String text = String.valueOf(value);
        return !REDIS_FALSE.equals(text) && !"false".equalsIgnoreCase(text);
    }

    private MarkPersistStateResult markDbPersisted(
            PersistTradeOrderMessage message,
            PersistOrderSnapshot snapshot) {
        long now = clock.millis();
        CompletableFuture<MarkPersistStateResult> markFuture = null;
        try {
            /*
             * 设计要求 persisted_version 表示“已成功写入 MySQL 的 Redis 订单版本”。
             * 因此这里传本次落库快照的 orderVersion，而不是 MQ 重试次数；Lua 会在 Redis 当前版本更新时
             * 只推进 persisted_version 并保留 pending，等待补刷最新版本。
             */
            long persistedVersion = snapshot.orderVersion();
            markFuture = reliabilityPort.markDbPersisted(
                            snapshot.toAcceptCommand(now, now + RETRY_DELAY_MILLIS),
                            snapshot.toAcceptResult(),
                            persistedVersion,
                            now)
                    .toCompletableFuture();
            return markFuture.get(redisMarkTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            cancelRedisMarkFuture(markFuture);
            logSampledError(LOG_EVENT_REDIS_MARK_DB_TIMEOUT, decision -> log.error(
                    "Redis订单markDbPersisted调用超时 orgCode={}, outOrderSn={}, orderId={}, messageId={}, timeoutMs={}, occurrence={}, sampled={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    redisMarkTimeout.toMillis(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            return MarkPersistStateResult.failed(
                    REDIS_MARK_TIMEOUT,
                    "markDbPersisted timeout after %dms".formatted(redisMarkTimeout.toMillis()));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            cancelRedisMarkFuture(markFuture);
            logSampledError(LOG_EVENT_REDIS_MARK_DB_INTERRUPTED, decision -> log.error(
                    "Redis订单markDbPersisted等待被中断 orgCode={}, outOrderSn={}, orderId={}, messageId={}, occurrence={}, sampled={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            return MarkPersistStateResult.failed(REDIS_MARK_INTERRUPTED, "markDbPersisted interrupted");
        } catch (ExecutionException ex) {
            Throwable cause = rootCause(ex);
            logSampledError(LOG_EVENT_REDIS_MARK_DB_EXCEPTION, decision -> log.error(
                    "Redis订单markDbPersisted调用异常 orgCode={}, outOrderSn={}, orderId={}, messageId={}, occurrence={}, sampled={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    cause));
            return MarkPersistStateResult.failed(REDIS_MARK_EXCEPTION, truncateFailReason(cause.getMessage()));
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_REDIS_MARK_DB_EXCEPTION, decision -> log.error(
                    "Redis订单markDbPersisted调用异常 orgCode={}, outOrderSn={}, orderId={}, messageId={}, occurrence={}, sampled={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            return MarkPersistStateResult.failed(REDIS_MARK_EXCEPTION, truncateFailReason(ex.getMessage()));
        }
    }

    private void markPersistFailed(
            PersistTradeOrderMessage message,
            PersistOrderSnapshot snapshot,
            String failReason) {
        long now = clock.millis();
        CompletableFuture<MarkPersistStateResult> markFuture = null;
        try {
            markFuture = reliabilityPort.markPersistFailed(
                            snapshot.toAcceptCommand(now, now + RETRY_DELAY_MILLIS),
                            snapshot.toAcceptResult(),
                            MYSQL_PERSIST_FAILED,
                            failReason,
                            1,
                            now + RETRY_DELAY_MILLIS,
                            now)
                    .toCompletableFuture();
            markFuture.get(redisMarkTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            cancelRedisMarkFuture(markFuture);
            logSampledError(LOG_EVENT_REDIS_MARK_FAILED_TIMEOUT, decision -> log.error(
                    "Redis订单异步落库失败后markPersistFailed超时 orgCode={}, outOrderSn={}, orderId={}, messageId={}, timeoutMs={}, occurrence={}, sampled={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    redisMarkTimeout.toMillis(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            cancelRedisMarkFuture(markFuture);
            logSampledError(LOG_EVENT_REDIS_MARK_FAILED_INTERRUPTED, decision -> log.error(
                    "Redis订单异步落库失败后markPersistFailed等待被中断 orgCode={}, outOrderSn={}, orderId={}, messageId={}, occurrence={}, sampled={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
        } catch (ExecutionException ex) {
            Throwable cause = rootCause(ex);
            logSampledError(LOG_EVENT_REDIS_MARK_FAILED_EXCEPTION, decision -> log.error(
                    "Redis订单异步落库失败后markPersistFailed也失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}, occurrence={}, sampled={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    cause));
        } catch (RuntimeException markEx) {
            logSampledError(LOG_EVENT_REDIS_MARK_FAILED_EXCEPTION, decision -> log.error(
                    "Redis订单异步落库失败后markPersistFailed也失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}, occurrence={}, sampled={}",
                    message.orgCode(),
                    message.outOrderSn(),
                    message.orderId(),
                    message.messageId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    markEx));
        }
    }

    private void logSampledError(String eventKey, Consumer<LogDecision> logger) {
        LogDecision decision = logStormGuard.shouldLog(eventKey);
        if (decision.shouldLog()) {
            logger.accept(decision);
        }
    }

    private static void cancelRedisMarkFuture(CompletableFuture<MarkPersistStateResult> markFuture) {
        if (markFuture == null || markFuture.isDone()) {
            return;
        }
        /*
         * 参见 docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」。
         * consumer 线程超时返回后，底层 Redis 调用可能仍挂起；显式取消用于触发适配器 Reactor 链路的
         * doFinally，释放同 session Redis 连接，避免高频超时继续耗尽连接池。
         */
        markFuture.cancel(true);
    }

    private static Duration positiveTimeout(Duration timeout) {
        Objects.requireNonNull(timeout, "redisMarkTimeout must not be null");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("redisMarkTimeout must be positive");
        }
        return timeout;
    }

    private static Throwable rootCause(ExecutionException exception) {
        return exception.getCause() == null ? exception : exception.getCause();
    }

    private static String truncateFailReason(String reason) {
        if (reason == null || reason.isBlank()) {
            return "";
        }
        if (reason.length() <= MAX_FAIL_REASON_LENGTH) {
            return reason;
        }
        return reason.substring(0, MAX_FAIL_REASON_LENGTH) + "...[truncated]";
    }

    private record SelectedSnapshot(PersistOrderSnapshot snapshot, PersistSource persistSource) {
    }
}
