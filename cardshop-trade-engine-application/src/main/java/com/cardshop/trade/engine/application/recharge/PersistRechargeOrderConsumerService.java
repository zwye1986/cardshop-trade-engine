package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.logging.LogStormGuard.LogDecision;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
    private static final String LOG_EVENT_PERSISTED_STATE_LOOKUP_FAILED = "persist-consumer.persisted-state.lookup.failed";
    private static final String LOG_EVENT_DISPATCH_TIMER_SCHEDULE_FAILED = "persist-consumer.dispatch-timer.schedule.failed";
    private static final long RETRY_DELAY_MILLIS = 60_000L;
    private static final Duration DEFAULT_REDIS_MARK_TIMEOUT = Duration.ofSeconds(3);
    /**
     * 测试/轻量构造默认首次派单延迟；生产路径必须注入
     * {@code trade.engine.fulfillment.dispatch-initial-delay-seconds}，与落库
     * {@code next_retry_time} 保持一致。参见详细设计「9.1 派单 Outbox」。
     */
    private static final Duration DEFAULT_DISPATCH_INITIAL_DELAY = Duration.ofSeconds(1);
    private static final int MAX_FAIL_REASON_LENGTH = 500;
    private static final String FIELD_REDIS_ORDER_WRITTEN = "redis_order_written";
    private static final String REDIS_FALSE = "0";

    private final PersistOrderSnapshotPort snapshotPort;
    private final RechargeOrderPersistencePort persistencePort;
    private final RechargeOrderBatchPersistencePort batchPersistencePort;
    private final PersistedOrderStatePort persistedOrderStatePort;
    private final RechargeAcceptReliabilityPort reliabilityPort;
    private final RechargeDispatchTimerPort dispatchTimerPort;
    private final Clock clock;
    private final Duration redisMarkTimeout;
    private final Duration dispatchInitialDelay;
    /**
     * outbox 异步写入端口；仅在 {@code outboxAsyncEnabled=true} 时必填，
     * 同步落库模式（默认）为 {@code null}。参见详细设计「8.3 异步落库 Consumer · Outbox 异步落库模式」。
     */
    private final RechargePersistOutboxAsyncWriter outboxAsyncWriter;
    private final boolean outboxAsyncEnabled;
    private final LogStormGuard logStormGuard = LogStormGuard.defaultGuard();

    public PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            Clock clock) {
        this(
                snapshotPort,
                persistencePort,
                null,
                PersistedOrderStatePort.noop(),
                reliabilityPort,
                clock,
                DEFAULT_REDIS_MARK_TIMEOUT);
    }

    public PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeOrderBatchPersistencePort batchPersistencePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            Clock clock) {
        this(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                PersistedOrderStatePort.noop(),
                reliabilityPort,
                RechargeDispatchTimerPort.noop(),
                clock,
                DEFAULT_REDIS_MARK_TIMEOUT,
                DEFAULT_DISPATCH_INITIAL_DELAY);
    }

    public PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeOrderBatchPersistencePort batchPersistencePort,
            PersistedOrderStatePort persistedOrderStatePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            Clock clock) {
        this(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                persistedOrderStatePort,
                reliabilityPort,
                RechargeDispatchTimerPort.noop(),
                clock,
                DEFAULT_REDIS_MARK_TIMEOUT,
                DEFAULT_DISPATCH_INITIAL_DELAY);
    }

    public PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeOrderBatchPersistencePort batchPersistencePort,
            PersistedOrderStatePort persistedOrderStatePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            RechargeDispatchTimerPort dispatchTimerPort,
            Clock clock) {
        this(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                persistedOrderStatePort,
                reliabilityPort,
                dispatchTimerPort,
                clock,
                DEFAULT_REDIS_MARK_TIMEOUT,
                DEFAULT_DISPATCH_INITIAL_DELAY);
    }

    /**
     * 创建异步落库 Consumer 核心服务。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
     * `redisMarkTimeout` 只约束 MySQL 事务之后的 Redis 状态推进调用，避免 RabbitMQ consumer 线程因
     * Redis 客户端 hang 住而无限阻塞；MySQL 已提交后的 `markDbPersisted` 超时会 ack 当前消息并交给补偿修复，
     * MySQL 失败后的 `markPersistFailed` 超时则保持当前消息可重试。
     *
     * <p>`dispatchInitialDelay` 对应 {@code trade.engine.fulfillment.dispatch-initial-delay-seconds}，
     * 必须与落库写入的 {@code recharge_dispatch_outbox.next_retry_time} 使用同一配置值。
     */
    public PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeOrderBatchPersistencePort batchPersistencePort,
            PersistedOrderStatePort persistedOrderStatePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            RechargeDispatchTimerPort dispatchTimerPort,
            Clock clock,
            Duration redisMarkTimeout,
            Duration dispatchInitialDelay) {
        this(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                persistedOrderStatePort,
                reliabilityPort,
                dispatchTimerPort,
                clock,
                redisMarkTimeout,
                dispatchInitialDelay,
                null,
                false);
    }

    /**
     * 创建支持 outbox 异步落库模式的异步落库 Consumer 核心服务。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer · Outbox 异步落库模式」。
     * {@code outboxAsyncEnabled=true} 时批路径改为两段事务：先调
     * {@link RechargeOrderBatchPersistencePort#persistBatchFactsOnly(List)} 提交三张事实表，
     * 再调 {@code outboxAsyncWriter.writeBatch} 对本批全部 selected 聚合（含二次确认跳过的消息）
     * 无条件幂等补写两张 outbox，outbox 写成功后才允许 timer 调度与 MQ ack。
     * {@code outboxAsyncEnabled=false} 时 {@code outboxAsyncWriter} 传 {@code null}，行为与原五表单事务完全一致。
     */
    public PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeOrderBatchPersistencePort batchPersistencePort,
            PersistedOrderStatePort persistedOrderStatePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            RechargeDispatchTimerPort dispatchTimerPort,
            Clock clock,
            Duration redisMarkTimeout,
            Duration dispatchInitialDelay,
            RechargePersistOutboxAsyncWriter outboxAsyncWriter,
            boolean outboxAsyncEnabled) {
        this.snapshotPort = Objects.requireNonNull(snapshotPort, "snapshotPort must not be null");
        this.persistencePort = Objects.requireNonNull(persistencePort, "persistencePort must not be null");
        this.batchPersistencePort = batchPersistencePort;
        this.persistedOrderStatePort = Objects.requireNonNull(
                persistedOrderStatePort,
                "persistedOrderStatePort must not be null");
        this.reliabilityPort = Objects.requireNonNull(reliabilityPort, "reliabilityPort must not be null");
        this.dispatchTimerPort = Objects.requireNonNull(dispatchTimerPort, "dispatchTimerPort must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.redisMarkTimeout = positiveTimeout(redisMarkTimeout);
        this.dispatchInitialDelay = positiveTimeout(dispatchInitialDelay);
        if (outboxAsyncEnabled) {
            this.outboxAsyncWriter = Objects.requireNonNull(
                    outboxAsyncWriter,
                    "outboxAsyncWriter must not be null when outboxAsyncEnabled=true");
        } else {
            this.outboxAsyncWriter = outboxAsyncWriter;
        }
        this.outboxAsyncEnabled = outboxAsyncEnabled;
    }

    PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            Clock clock,
            Duration redisMarkTimeout) {
        this(
                snapshotPort,
                persistencePort,
                null,
                PersistedOrderStatePort.noop(),
                reliabilityPort,
                RechargeDispatchTimerPort.noop(),
                clock,
                redisMarkTimeout,
                DEFAULT_DISPATCH_INITIAL_DELAY);
    }

    PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeOrderBatchPersistencePort batchPersistencePort,
            PersistedOrderStatePort persistedOrderStatePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            Clock clock,
            Duration redisMarkTimeout) {
        this(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                persistedOrderStatePort,
                reliabilityPort,
                RechargeDispatchTimerPort.noop(),
                clock,
                redisMarkTimeout,
                DEFAULT_DISPATCH_INITIAL_DELAY);
    }

    PersistRechargeOrderConsumerService(
            PersistOrderSnapshotPort snapshotPort,
            RechargeOrderPersistencePort persistencePort,
            RechargeOrderBatchPersistencePort batchPersistencePort,
            PersistedOrderStatePort persistedOrderStatePort,
            RechargeAcceptReliabilityPort reliabilityPort,
            RechargeDispatchTimerPort dispatchTimerPort,
            Clock clock,
            Duration redisMarkTimeout) {
        this(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                persistedOrderStatePort,
                reliabilityPort,
                dispatchTimerPort,
                clock,
                redisMarkTimeout,
                DEFAULT_DISPATCH_INITIAL_DELAY);
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

        scheduleDispatchAfterMysqlCommit(aggregate);
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

    /**
     * 处理一批持久化 MQ 消息。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。批模式只合并
     * MySQL commit 成本，不改变单笔订单的快照选择、幂等和 Redis mark 语义。MySQL 批事务失败时整批可重试；
     * MySQL 已提交后的 Redis mark 失败不回滚数据库，仍允许 RabbitMQ ack，交由 pending 二次确认修复。
     */
    public PersistRechargeOrderBatchResult persistBatch(List<PersistTradeOrderMessage> messages) {
        Objects.requireNonNull(messages, "messages must not be null");
        if (messages.isEmpty()) {
            return PersistRechargeOrderBatchResult.persisted(0, 0, 0);
        }
        if (batchPersistencePort == null) {
            throw new IllegalStateException("RechargeOrderBatchPersistencePort is not configured");
        }

        List<SelectedBatchMessage> selectedMessages = new ArrayList<>(messages.size());
        for (PersistTradeOrderMessage message : messages) {
            Objects.requireNonNull(message, "messages must not contain null");
            boolean redisOrderWritten = redisOrderWritten(message);
            SelectedSnapshot selected = selectSnapshot(message, redisOrderWritten);
            OrderPersistAggregate aggregate = new OrderPersistAggregate(
                    message.messageId(),
                    selected.snapshot(),
                    selected.persistSource(),
                    clock.instant());
            selectedMessages.add(new SelectedBatchMessage(
                    message,
                    redisOrderWritten,
                    selected,
                    aggregate,
                    new PersistedOrderProbe(
                            selected.snapshot().orgCode(),
                            selected.snapshot().outOrderSn(),
                            selected.snapshot().orderId(),
                            selected.snapshot().orderVersion())));
        }
        Set<PersistedOrderProbe> alreadyPersistedProbes = alreadyPersistedProbes(selectedMessages);
        List<SelectedBatchMessage> persistedCandidates = selectedMessages.stream()
                .filter(selectedMessage -> !alreadyPersistedProbes.contains(selectedMessage.persistedProbe()))
                .toList();
        int skippedAlreadyPersistedCount = selectedMessages.size() - persistedCandidates.size();
        List<OrderPersistAggregate> aggregates = persistedCandidates.stream()
                .map(SelectedBatchMessage::aggregate)
                .toList();
        /*
         * outbox 异步落库模式的一致性不变量（参见详细设计「8.3 异步落库 Consumer · Outbox 异步落库模式」）：
         * writer 入参必须是本批全部 selected 聚合，不能随二次确认跳过收缩。
         * 崩溃窗口「事实已提交、outbox 未写」的订单重投后会被 alreadyPersistedProbes 跳过事实事务，
         * 若 writer 也跳过它们，outbox 行将永久缺失、派单永远不会发生；这里靠 event_id 唯一键 ODKU 去重。
         */
        List<OrderPersistAggregate> allSelectedAggregates = selectedMessages.stream()
                .map(SelectedBatchMessage::aggregate)
                .toList();

        List<SelectedBatchMessage> failedMessagesToMark = persistedCandidates;
        try {
            if (!aggregates.isEmpty()) {
                if (outboxAsyncEnabled) {
                    batchPersistencePort.persistBatchFactsOnly(aggregates);
                } else {
                    batchPersistencePort.persistBatchWithOutbox(aggregates);
                }
            }
            if (outboxAsyncEnabled) {
                // ack 必在 outbox 写成功之后：写失败抛异常即整批不 ack，走下方统一失败处理
                failedMessagesToMark = selectedMessages;
                outboxAsyncWriter.writeBatch(allSelectedAggregates);
            }
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_MYSQL_PERSIST_FAILED, decision -> log.error(
                    "Redis订单批量异步落MySQL失败 batchSize={}, firstMessageId={}, lastMessageId={}, outboxAsyncEnabled={}, occurrence={}, sampled={}",
                    messages.size(),
                    messages.getFirst().messageId(),
                    messages.getLast().messageId(),
                    outboxAsyncEnabled,
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            String failReason = truncateFailReason(ex.getMessage());
            for (SelectedBatchMessage selectedMessage : failedMessagesToMark) {
                if (selectedMessage.redisOrderWritten()) {
                    markPersistFailed(
                            selectedMessage.message(),
                            selectedMessage.selected().snapshot(),
                            failReason);
                }
            }
            return PersistRechargeOrderBatchResult.persistFailed(
                    messages.size(),
                    MYSQL_PERSIST_FAILED,
                    failReason);
        }

        if (outboxAsyncEnabled) {
            /*
             * 异步模式下 timer 调度同样覆盖全部 selected 聚合：崩溃窗口重投的订单首次调度可能从未发生，
             * 且此时 outbox 行已可见，调度语义安全；重复调度由 timer 端口幂等兜底。
             */
            selectedMessages.forEach(selectedMessage -> scheduleDispatchAfterMysqlCommit(selectedMessage.aggregate()));
        } else {
            persistedCandidates.forEach(selectedMessage -> scheduleDispatchAfterMysqlCommit(selectedMessage.aggregate()));
        }
        int redisMarkFailedCount = 0;
        for (SelectedBatchMessage selectedMessage : selectedMessages) {
            if (!selectedMessage.redisOrderWritten()) {
                continue;
            }
            MarkPersistStateResult markResult = markDbPersisted(
                    selectedMessage.message(),
                    selectedMessage.selected().snapshot());
            if (!markResult.marked()) {
                redisMarkFailedCount++;
                logSampledError(LOG_EVENT_REDIS_MARK_AFTER_MYSQL_FAILED, decision -> log.error(
                        "Redis订单批量异步落MySQL成功但markDbPersisted失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}, failCode={}, failReason={}, occurrence={}, sampled={}",
                        selectedMessage.message().orgCode(),
                        selectedMessage.message().outOrderSn(),
                        selectedMessage.message().orderId(),
                        selectedMessage.message().messageId(),
                        markResult.failCode(),
                        markResult.failReason(),
                        decision.occurrence(),
                        decision.sampledOccurrence()));
            }
        }
        return PersistRechargeOrderBatchResult.persisted(
                messages.size(),
                aggregates.size(),
                skippedAlreadyPersistedCount,
                redisMarkFailedCount);
    }

    private Set<PersistedOrderProbe> alreadyPersistedProbes(List<SelectedBatchMessage> selectedMessages) {
        List<PersistedOrderProbe> probes = selectedMessages.stream()
                .map(SelectedBatchMessage::persistedProbe)
                .toList();
        try {
            return persistedOrderStatePort.findPersistedAtLeast(probes);
        } catch (RuntimeException ex) {
            /*
             * 参见 docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」。
             * 二次确认只是用于降低 Rabbit 重复副本的 MySQL 写入成本；查询失败时不能把订单当作已落库，
             * 否则会误跳过真正未提交的订单。这里采样记录 error 后退回原批写幂等路径。
             */
            logSampledError(LOG_EVENT_PERSISTED_STATE_LOOKUP_FAILED, decision -> log.error(
                    "批量二次确认MySQL已落库状态失败 batchSize={}, firstMessageId={}, lastMessageId={}, occurrence={}, sampled={}",
                    selectedMessages.size(),
                    selectedMessages.getFirst().message().messageId(),
                    selectedMessages.getLast().message().messageId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            return Set.of();
        }
    }

    private void scheduleDispatchAfterMysqlCommit(OrderPersistAggregate aggregate) {
        /*
         * 参见 docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」。
         * timer 只是热路径唤醒，真正任务事实已随 MySQL 事务写入 recharge_dispatch_outbox。调度失败不能影响
         * 已提交订单的 MQ ack 语义，低频 XXL-Job 扫描会按 next_retry_time 恢复。
         */
        if (!aggregate.dispatchOutboxRequired() || !aggregate.snapshot().balanceReserved()) {
            return;
        }
        String eventId = RechargeDispatchEventIds.dispatchEventId(aggregate.snapshot().orderId());
        try {
            dispatchTimerPort.schedule(eventId, OutboxEventType.SUPPLIER_DISPATCH, dispatchInitialDelay);
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_DISPATCH_TIMER_SCHEDULE_FAILED, decision -> log.error(
                    "订单落库后调度供应商派单timer失败 orgCode={}, outOrderSn={}, orderId={}, eventId={}, occurrence={}, sampled={}",
                    aggregate.snapshot().orgCode(),
                    aggregate.snapshot().outOrderSn(),
                    aggregate.snapshot().orderId(),
                    eventId,
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
        }
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

    private record SelectedBatchMessage(
            PersistTradeOrderMessage message,
            boolean redisOrderWritten,
            SelectedSnapshot selected,
            OrderPersistAggregate aggregate,
            PersistedOrderProbe persistedProbe) {
    }
}
