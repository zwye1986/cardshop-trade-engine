package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.logging.LogStormGuard.LogDecision;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecord;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecordPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.CancelUnconfirmedAcceptResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistQueuedResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistStateResult;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.ReplicationWaitResult;
import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import java.time.Duration;
import java.util.Locale;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

/**
 * 同 Redis session 的接单与可靠性适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.5 下单服务实现细节」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「14.2 Redis WAIT 协议」。Redis WAIT 必须确认
 * 同一客户端连接上 accept Lua 产生的写入，因此该适配器同时实现 {@link AcceptRechargeOrderPort}
 * 与 {@link RechargeAcceptReliabilityPort}，并在 `orderId` 维度短暂保存接单 session。
 */
public class RedisSessionAcceptReliabilityAdapter implements AcceptRechargeOrderPort, RechargeAcceptReliabilityPort {

    private static final Logger log = LoggerFactory.getLogger(RedisSessionAcceptReliabilityAdapter.class);
    private static final int MAX_ACCEPT_ANOMALY_ID_LENGTH = 100;
    private static final int MAX_ERROR_MESSAGE_LENGTH = 500;
    private static final String LOG_EVENT_ACCEPT_OPEN_SESSION_FAILED = "redis-session.accept.open-session.failed";
    private static final String LOG_EVENT_ACCEPT_LUA_FAILED = "redis-session.accept.lua.failed";
    private static final String LOG_EVENT_ACCEPT_UNKNOWN_CLOSE_WRITE_FAILED =
            "redis-session.accept.unknown.close-write.failed";
    private static final String LOG_EVENT_ACCEPT_UNKNOWN_ANOMALY_FAILED =
            "redis-session.accept.unknown.anomaly.failed";
    private static final String LOG_EVENT_ACCEPT_UNKNOWN_ANOMALY_SCHEDULE_FAILED =
            "redis-session.accept.unknown.anomaly.schedule.failed";
    private static final String LOG_EVENT_WAIT_SESSION_MISSING = "redis-session.wait.session-missing";
    private static final String LOG_EVENT_WAIT_FAILED = "redis-session.wait.failed";
    private static final String LOG_EVENT_CANCEL_OPEN_SESSION_FAILED = "redis-session.cancel.open-session.failed";
    private static final String LOG_EVENT_CANCEL_LUA_FAILED = "redis-session.cancel.lua.failed";
    private static final String LOG_EVENT_MARK_QUEUED_OPEN_SESSION_FAILED =
            "redis-session.mark-queued.open-session.failed";
    private static final String LOG_EVENT_MARK_QUEUED_LUA_FAILED = "redis-session.mark-queued.lua.failed";
    private static final String LOG_EVENT_MARK_DB_OPEN_SESSION_FAILED = "redis-session.mark-db.open-session.failed";
    private static final String LOG_EVENT_MARK_DB_LUA_FAILED = "redis-session.mark-db.lua.failed";
    private static final String LOG_EVENT_MARK_FAILED_OPEN_SESSION_FAILED =
            "redis-session.mark-failed.open-session.failed";
    private static final String LOG_EVENT_MARK_FAILED_LUA_FAILED = "redis-session.mark-failed.lua.failed";
    private static final String LOG_EVENT_CLOSE_SESSION_FAILED = "redis-session.close.failed";

    private final RechargeRedisSessionFactory sessionFactory;
    private final RechargeRedisKeyBuilder keyBuilder;
    private final RechargeRedisScriptLoader scriptLoader;
    private final TradeEngineProperties properties;
    private final RedisHaGuardPort redisHaGuardPort;
    private final AcceptAnomalyRecordPort acceptAnomalyRecordPort;
    private final Executor acceptAnomalyRecordExecutor;
    private final AcceptRechargeOrderLuaResultMapper acceptResultMapper = new AcceptRechargeOrderLuaResultMapper();
    private final Map<String, RechargeRedisSession> pendingSessions = new ConcurrentHashMap<>();
    private final LogStormGuard logStormGuard = LogStormGuard.defaultGuard();

    public RedisSessionAcceptReliabilityAdapter(
            RechargeRedisSessionFactory sessionFactory,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            TradeEngineProperties properties) {
        this(sessionFactory, keyBuilder, scriptLoader, properties, RedisHaGuardPort.noop());
    }

    public RedisSessionAcceptReliabilityAdapter(
            RechargeRedisSessionFactory sessionFactory,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            TradeEngineProperties properties,
            RedisHaGuardPort redisHaGuardPort) {
        this(sessionFactory, keyBuilder, scriptLoader, properties, redisHaGuardPort, AcceptAnomalyRecordPort.noop(), Runnable::run);
    }

    public RedisSessionAcceptReliabilityAdapter(
            RechargeRedisSessionFactory sessionFactory,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            TradeEngineProperties properties,
            RedisHaGuardPort redisHaGuardPort,
            AcceptAnomalyRecordPort acceptAnomalyRecordPort,
            Executor acceptAnomalyRecordExecutor) {
        this.sessionFactory = sessionFactory;
        this.keyBuilder = keyBuilder;
        this.scriptLoader = scriptLoader;
        this.properties = properties;
        this.redisHaGuardPort = redisHaGuardPort == null ? RedisHaGuardPort.noop() : redisHaGuardPort;
        this.acceptAnomalyRecordPort = acceptAnomalyRecordPort == null
                ? AcceptAnomalyRecordPort.noop()
                : acceptAnomalyRecordPort;
        this.acceptAnomalyRecordExecutor = acceptAnomalyRecordExecutor == null ? Runnable::run : acceptAnomalyRecordExecutor;
    }

    @Override
    public CompletionStage<AcceptRechargeOrderResult> accept(AcceptRechargeOrderCommand command) {
        if (!properties.getRedis().isWriteEnabled() || !redisHaGuardPort.isWriteAllowed()) {
            return CompletableFuture.completedFuture(AcceptRechargeOrderResult.redisWriteDisabled(command.orderId()));
        }

        /*
         * 参见 docs/交行直充业务系统详细设计.md「20.13.6 Redis Lua 下单结果」。
         * Lua 参数和脚本对象都不依赖 Redis 连接，必须在 openSession 之前完成；否则参数构造或脚本加载异常时，
         * 已打开的同 session WAIT 连接没有机会进入 Reactor 链路的关闭分支。
         */
        var luaArguments = AcceptRechargeOrderLuaArguments.from(
                command,
                keyBuilder,
                Duration.ofDays(properties.getOrder().getPendingTtlDays()),
                Duration.ofSeconds(properties.getV1().getNonceReplayTtlSeconds()));
        RedisScript<?> acceptScript = scriptLoader.acceptRechargeOrderScript();

        RechargeRedisSession session;
        try {
            session = sessionFactory.openSession();
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_ACCEPT_OPEN_SESSION_FAILED, decision -> log.error(
                    "打开Redis接单会话失败 orgCode={}, outOrderSn={}, orderId={}, occurrence={}, sampled={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    command.orderId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            return CompletableFuture.completedFuture(
                    AcceptRechargeOrderResult.redisUnavailable(command.orderId(), "Redis 会话不可用"));
        }

        return session
                .execute(acceptScript, luaArguments.keys(), luaArguments.args())
                .switchIfEmpty(Mono.just(AcceptRechargeOrderResult.redisUnavailable(
                        command.orderId(), "Lua 未返回结果")))
                .map(result -> result instanceof AcceptRechargeOrderResult acceptResult
                        ? acceptResult
                        : acceptResultMapper.map(result))
                .doOnNext(result -> retainSessionOnlyForWrittenOrder(command, session, result))
                .onErrorResume(ex -> {
                    closeWithErrorLog(session);
                    AcceptAnomalyType anomalyType = classifyAcceptAnomalyType(ex);
                    closeRedisWriteForUnknownAccept(command, anomalyType, ex);
                    recordAcceptStateUnknownAnomalyAsync(command, anomalyType, ex);
                    logSampledError(LOG_EVENT_ACCEPT_LUA_FAILED, decision -> log.error(
                            "Redis session接单Lua状态未知 orgCode={}, outOrderSn={}, orderId={}, requestHash={}, anomalyType={}, occurrence={}, sampled={}",
                            command.orgCode(),
                            command.outOrderSn(),
                            command.orderId(),
                            command.requestHash(),
                            anomalyType,
                            decision.occurrence(),
                            decision.sampledOccurrence(),
                            ex));
                    return Mono.just(AcceptRechargeOrderResult.redisUnavailable(
                            command.orderId(), acceptUnknownFailReason(anomalyType, ex)));
                })
                .toFuture();
    }

    @Override
    public CompletionStage<ReplicationWaitResult> waitForReplication(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult) {
        int replicas = properties.getRedis().getWaitReplicas();
        if (replicas <= 0) {
            releaseSession(acceptResult.orderId());
            return CompletableFuture.completedFuture(ReplicationWaitResult.confirmed(0));
        }

        RechargeRedisSession session = pendingSessions.get(acceptResult.orderId());
        if (session == null) {
            logSampledError(LOG_EVENT_WAIT_SESSION_MISSING, decision -> log.error(
                    "Redis WAIT缺少接单session orgCode={}, outOrderSn={}, orderId={}, occurrence={}, sampled={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    acceptResult.orderId(),
                    decision.occurrence(),
                    decision.sampledOccurrence()));
            return CompletableFuture.completedFuture(
                    ReplicationWaitResult.missed(0, "REDIS_SESSION_NOT_FOUND"));
        }

        Duration timeout = Duration.ofMillis(Math.max(1, properties.getRedis().getWaitTimeoutMs()));
        return session.waitForReplication(replicas, timeout)
                .map(actual -> {
                    if (actual >= replicas) {
                        releaseSession(acceptResult.orderId());
                        return ReplicationWaitResult.confirmed(actual);
                    }
                    return ReplicationWaitResult.missed(
                            actual,
                            "WAIT replicas not reached: expected=%d, actual=%d".formatted(replicas, actual));
                })
                .onErrorResume(ex -> {
                    releaseSession(acceptResult.orderId());
                    logSampledError(LOG_EVENT_WAIT_FAILED, decision -> log.error(
                            "Redis WAIT执行失败 orgCode={}, outOrderSn={}, orderId={}, replicas={}, timeoutMs={}, occurrence={}, sampled={}",
                            command.orgCode(),
                            command.outOrderSn(),
                            acceptResult.orderId(),
                            replicas,
                            timeout.toMillis(),
                            decision.occurrence(),
                            decision.sampledOccurrence(),
                            ex));
                    return Mono.just(ReplicationWaitResult.missed(0, "WAIT_ERROR"));
                })
                .toFuture();
    }

    @Override
    public CompletionStage<CancelUnconfirmedAcceptResult> cancelUnconfirmedAccept(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            String reason) {
        RechargeRedisSession session = pendingSessions.remove(acceptResult.orderId());
        if (session == null) {
            try {
                session = sessionFactory.openSession();
            } catch (RuntimeException ex) {
                logSampledError(LOG_EVENT_CANCEL_OPEN_SESSION_FAILED, decision -> log.error(
                        "打开Redis取消会话失败 orgCode={}, outOrderSn={}, orderId={}, occurrence={}, sampled={}",
                        command.orgCode(),
                        command.outOrderSn(),
                        acceptResult.orderId(),
                        decision.occurrence(),
                        decision.sampledOccurrence(),
                        ex));
                return CompletableFuture.completedFuture(
                        CancelUnconfirmedAcceptResult.failed(
                                CancelUnconfirmedAcceptResult.FAIL_CODE_REDIS_SESSION_UNAVAILABLE,
                                "Redis 会话不可用"));
            }
        }
        RechargeRedisSession sessionToClose = session;
        List<String> args = List.of(
                command.orgCode(),
                acceptResult.orderId(),
                command.outOrderSn(),
                Long.toString(command.orderAmountCent()),
                Long.toString(command.nowEpochMillis()),
                reason == null ? "" : reason);
        return session
                .execute(
                        scriptLoader.cancelUnconfirmedAcceptScript(),
                        keyBuilder.cancelUnconfirmedAcceptKeys(command),
                        args)
                .map(RedisSessionAcceptReliabilityAdapter::toCancelResult)
                .doFinally(signal -> closeWithErrorLog(sessionToClose))
                .onErrorResume(ex -> {
                    logSampledError(LOG_EVENT_CANCEL_LUA_FAILED, decision -> log.error(
                            "Redis取消未确认接单失败 orgCode={}, outOrderSn={}, orderId={}, occurrence={}, sampled={}",
                            command.orgCode(),
                            command.outOrderSn(),
                            acceptResult.orderId(),
                            decision.occurrence(),
                            decision.sampledOccurrence(),
                            ex));
                    return Mono.just(CancelUnconfirmedAcceptResult.failed(
                            CancelUnconfirmedAcceptResult.FAIL_CODE_CANCEL_LUA_ERROR,
                            "Redis 取消脚本执行失败"));
                })
                .toFuture();
    }

    @Override
    public CompletionStage<MarkPersistQueuedResult> markPersistQueued(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            String persistMessageId,
            int publishCount) {
        RechargeRedisSession session;
        try {
            session = sessionFactory.openSession();
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_MARK_QUEUED_OPEN_SESSION_FAILED, decision -> log.error(
                    "打开Redis持久化状态推进会话失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}, occurrence={}, sampled={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    acceptResult.orderId(),
                    persistMessageId,
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            return CompletableFuture.completedFuture(
                    MarkPersistQueuedResult.failed(
                            MarkPersistQueuedResult.FAIL_CODE_REDIS_SESSION_UNAVAILABLE,
                            "Redis 会话不可用"));
        }
        RechargeRedisSession sessionToClose = session;
        List<String> args = List.of(
                acceptResult.orderId(),
                persistMessageId == null ? "" : persistMessageId,
                Integer.toString(Math.max(1, publishCount)),
                Long.toString(command.nowEpochMillis()));
        return session
                .execute(scriptLoader.markPersistQueuedScript(), keyBuilder.markPersistQueuedKeys(command), args)
                .map(RedisSessionAcceptReliabilityAdapter::toMarkResult)
                .doFinally(signal -> closeWithErrorLog(sessionToClose))
                .onErrorResume(ex -> {
                    logSampledError(LOG_EVENT_MARK_QUEUED_LUA_FAILED, decision -> log.error(
                            "Redis持久化状态推进失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}, occurrence={}, sampled={}",
                            command.orgCode(),
                            command.outOrderSn(),
                            acceptResult.orderId(),
                            persistMessageId,
                            decision.occurrence(),
                            decision.sampledOccurrence(),
                            ex));
                    return Mono.just(MarkPersistQueuedResult.failed(
                            MarkPersistQueuedResult.FAIL_CODE_MARK_LUA_ERROR,
                            "Redis 状态推进脚本执行失败"));
                })
                .toFuture();
    }

    @Override
    public CompletionStage<MarkPersistStateResult> markDbPersisted(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            long persistedVersion,
            long nowEpochMillis) {
        RechargeRedisSession session;
        try {
            session = sessionFactory.openSession();
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_MARK_DB_OPEN_SESSION_FAILED, decision -> log.error(
                    "打开Redis落库完成标记会话失败 orgCode={}, outOrderSn={}, orderId={}, occurrence={}, sampled={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    acceptResult.orderId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            return CompletableFuture.completedFuture(
                    MarkPersistStateResult.failed(
                            MarkPersistStateResult.FAIL_CODE_REDIS_SESSION_UNAVAILABLE,
                            "Redis 会话不可用"));
        }
        RechargeRedisSession sessionToClose = session;
        long terminalTtlSeconds = Duration.ofHours(Math.max(1, properties.getOrder().getTerminalTtlHours())).toSeconds();
        /*
         * persistedVersion 对应本次已落 MySQL 的 Redis order_version。Lua 会比较 Redis 当前版本：
         * 版本未变化时清 pending；Redis 已有更新版本时只推进 persisted_version，继续保留 pending 等待补刷。
         */
        List<String> args = List.of(
                command.orgCode(),
                acceptResult.orderId(),
                Long.toString(Math.max(0, persistedVersion)),
                Long.toString(nowEpochMillis),
                Long.toString(terminalTtlSeconds));
        return session
                .execute(scriptLoader.markDbPersistedScript(), keyBuilder.markDbPersistedKeys(command), args)
                .map(RedisSessionAcceptReliabilityAdapter::toPersistStateMarkResult)
                .doFinally(signal -> closeWithErrorLog(sessionToClose))
                .onErrorResume(ex -> {
                    logSampledError(LOG_EVENT_MARK_DB_LUA_FAILED, decision -> log.error(
                            "Redis落库完成状态推进失败 orgCode={}, outOrderSn={}, orderId={}, occurrence={}, sampled={}",
                            command.orgCode(),
                            command.outOrderSn(),
                            acceptResult.orderId(),
                            decision.occurrence(),
                            decision.sampledOccurrence(),
                            ex));
                    return Mono.just(MarkPersistStateResult.failed(
                            MarkPersistStateResult.FAIL_CODE_MARK_DB_PERSISTED_LUA_ERROR,
                            "Redis 落库完成标记失败"));
                })
                .toFuture();
    }

    @Override
    public CompletionStage<MarkPersistStateResult> markPersistFailed(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            String failCode,
            String failReason,
            int retryCount,
            long nextRetryEpochMillis,
            long nowEpochMillis) {
        RechargeRedisSession session;
        try {
            session = sessionFactory.openSession();
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_MARK_FAILED_OPEN_SESSION_FAILED, decision -> log.error(
                    "打开Redis落库失败标记会话失败 orgCode={}, outOrderSn={}, orderId={}, occurrence={}, sampled={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    acceptResult.orderId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            return CompletableFuture.completedFuture(
                    MarkPersistStateResult.failed(
                            MarkPersistStateResult.FAIL_CODE_REDIS_SESSION_UNAVAILABLE,
                            "Redis 会话不可用"));
        }
        RechargeRedisSession sessionToClose = session;
        /*
         * 参数顺序必须与 mark_persist_failed.lua 保持一致。该脚本保留 pending，并刷新 next_retry_time，
         * 让补偿任务继续扫描，而不是在 MySQL 短暂故障时丢失恢复入口。
         */
        List<String> args = List.of(
                command.orgCode(),
                acceptResult.orderId(),
                Long.toString(nextRetryEpochMillis),
                Long.toString(nowEpochMillis),
                failCode == null ? "" : failCode,
                failReason == null ? "" : failReason,
                Integer.toString(Math.max(0, retryCount)));
        return session
                .execute(scriptLoader.markPersistFailedScript(), keyBuilder.markPersistFailedKeys(command), args)
                .map(RedisSessionAcceptReliabilityAdapter::toPersistStateMarkResult)
                .doFinally(signal -> closeWithErrorLog(sessionToClose))
                .onErrorResume(ex -> {
                    logSampledError(LOG_EVENT_MARK_FAILED_LUA_FAILED, decision -> log.error(
                            "Redis落库失败状态推进失败 orgCode={}, outOrderSn={}, orderId={}, failCode={}, occurrence={}, sampled={}",
                            command.orgCode(),
                            command.outOrderSn(),
                            acceptResult.orderId(),
                            failCode,
                            decision.occurrence(),
                            decision.sampledOccurrence(),
                            ex));
                    return Mono.just(MarkPersistStateResult.failed(
                            MarkPersistStateResult.FAIL_CODE_MARK_PERSIST_FAILED_LUA_ERROR,
                            "Redis 落库失败标记失败"));
                })
                .toFuture();
    }

    private void retainSessionOnlyForWrittenOrder(
            AcceptRechargeOrderCommand command,
            RechargeRedisSession session,
            AcceptRechargeOrderResult result) {
        if (!result.orderWritten()) {
            closeWithErrorLog(session);
            return;
        }
        RechargeRedisSession previous = pendingSessions.put(result.orderId(), session);
        if (previous != null && previous != session) {
            closeWithErrorLog(previous);
            log.warn("Redis接单session被新会话覆盖 orgCode={}, outOrderSn={}, orderId={}",
                    command.orgCode(), command.outOrderSn(), result.orderId());
        }
    }

    /**
     * 将 accept Lua 异常映射成可核查的异常类型。
     *
     * <p>Redis 脚本 timeout、BUSY 和客户端连接中断都可能出现在“Redis 已经收到脚本但 Java 未拿到返回值”
     * 的窗口。这里不能简单当作写入前失败，也不能立即重试同一请求；因此统一进入状态未知分支，只在异常类型上
     * 做细分，方便后续自动核查或人工处理按根因聚合。
     */
    private static AcceptAnomalyType classifyAcceptAnomalyType(Throwable ex) {
        for (Throwable current = ex; current != null; current = current.getCause()) {
            String text = ((current.getClass().getName()) + " " + current.getMessage()).toLowerCase(Locale.ROOT);
            if (text.contains("busy")) {
                return AcceptAnomalyType.REDIS_BUSY;
            }
            if (text.contains("timeout") || text.contains("timed out")) {
                return AcceptAnomalyType.LUA_TIMEOUT;
            }
        }
        return AcceptAnomalyType.ACCEPT_STATE_UNKNOWN;
    }

    /**
     * accept Lua 状态未知时关闭 Redis 写入。
     *
     * <p>参见 docs/交行直充业务系统详细设计.md「20.13.6 Redis Lua 下单结果」。
     * 这类异常无法证明订单、余额预扣和 pending 是否已经写入；继续放量只会扩大核查范围，所以先通过
     * HA Guard fail closed，再由 anomaly 自动核查或人工确认恢复。
     */
    private void closeRedisWriteForUnknownAccept(
            AcceptRechargeOrderCommand command,
            AcceptAnomalyType anomalyType,
            Throwable ex) {
        Map<String, Object> context = unknownAcceptContext(command, anomalyType, ex);
        try {
            /*
             * RedisHaGuardPort.closeWrite 约定必须是本机内存或等价快速操作。accept Lua 的 onErrorResume 可能在
             * Lettuce/Reactor 回调线程上执行，不能在这里同步做远程配置发布；需要持久化关写时应在端口内部异步化。
             */
            redisHaGuardPort.closeWrite(anomalyType.name(), context);
        } catch (RuntimeException closeEx) {
            logSampledError(LOG_EVENT_ACCEPT_UNKNOWN_CLOSE_WRITE_FAILED, decision -> log.error(
                    "Redis接单状态未知后关闭HA Guard失败 orgCode={}, outOrderSn={}, orderId={}, anomalyType={}, occurrence={}, sampled={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    command.orderId(),
                    anomalyType,
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    closeEx));
        }
    }

    /**
     * 异步登记 accept Lua 状态未知 anomaly。
     *
     * <p>异常登记最终可能落 MySQL，必须切到 DB executor 执行，避免 Redis 回调线程被 MyBatis 阻塞。
     * 登记失败只影响后续核查入口的完整性，不能改变当前请求的 fail-closed 响应；因此只记录日志并返回。
     */
    private void recordAcceptStateUnknownAnomalyAsync(
            AcceptRechargeOrderCommand command,
            AcceptAnomalyType anomalyType,
            Throwable ex) {
        try {
            CompletableFuture
                    .runAsync(
                            () -> recordAcceptStateUnknownAnomaly(command, anomalyType, ex),
                            acceptAnomalyRecordExecutor)
                    .exceptionally(recordEx -> {
                        logSampledError(LOG_EVENT_ACCEPT_UNKNOWN_ANOMALY_FAILED, decision -> log.error(
                                "Redis接单状态未知异常登记失败 orgCode={}, outOrderSn={}, orderId={}, anomalyType={}, occurrence={}, sampled={}",
                                command.orgCode(),
                                command.outOrderSn(),
                                command.orderId(),
                                anomalyType,
                                decision.occurrence(),
                                decision.sampledOccurrence(),
                                recordEx));
                        return null;
                    });
        } catch (RuntimeException scheduleEx) {
            logSampledError(LOG_EVENT_ACCEPT_UNKNOWN_ANOMALY_SCHEDULE_FAILED, decision -> log.error(
                    "Redis接单状态未知异常登记调度失败 orgCode={}, outOrderSn={}, orderId={}, anomalyType={}, occurrence={}, sampled={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    command.orderId(),
                    anomalyType,
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    scheduleEx));
        }
    }

    private void recordAcceptStateUnknownAnomaly(
            AcceptRechargeOrderCommand command,
            AcceptAnomalyType anomalyType,
            Throwable ex) {
        AcceptAnomalyRecord record = new AcceptAnomalyRecord(
                acceptAnomalyId(anomalyType, command),
                anomalyType,
                command.orgCode(),
                command.outOrderSn(),
                command.orderId(),
                command.requestHash(),
                "",
                unknownAcceptContext(command, anomalyType, ex),
                acceptUnknownFailReason(anomalyType, ex));
        acceptAnomalyRecordPort.record(record);
    }

    private static String acceptAnomalyId(AcceptAnomalyType type, AcceptRechargeOrderCommand command) {
        String prefix = "accept-" + type.name() + "-";
        if (hasText(command.orderId())) {
            return prefix + abbreviate(command.orderId(), MAX_ACCEPT_ANOMALY_ID_LENGTH - prefix.length());
        }
        /*
         * 理论上 V1 下单在进入 Redis 前已生成 orderId；这里仍保留 request_hash 兜底，防止后续复用适配器的
         * 入口在状态未知时拿不到平台订单号，多个异常退化成同一个 `unknown`。
         */
        return prefix + abbreviate("request-" + command.requestHash(), MAX_ACCEPT_ANOMALY_ID_LENGTH - prefix.length());
    }

    private static Map<String, Object> unknownAcceptContext(
            AcceptRechargeOrderCommand command,
            AcceptAnomalyType anomalyType,
            Throwable ex) {
        Throwable root = rootCause(ex);
        Map<String, Object> context = new LinkedHashMap<>();
        // 只放核查定位字段；账号、原始请求体、token、密钥等敏感信息不能进入 anomaly raw_context。
        context.put("org_code", command.orgCode());
        context.put("out_order_sn", command.outOrderSn());
        context.put("order_id", command.orderId());
        context.put("request_hash", command.requestHash());
        context.put("anomaly_type", anomalyType.name());
        context.put("failure_phase", "ACCEPT_LUA");
        context.put("accept_status", "UNKNOWN");
        context.put("order_written", null);
        context.put("redis_write_closed", true);
        context.put("exception_class", root.getClass().getName());
        context.put("exception_message", truncate(root.getMessage(), MAX_ERROR_MESSAGE_LENGTH));
        context.put("now_epoch_millis", command.nowEpochMillis());
        return context;
    }

    private static String acceptUnknownFailReason(AcceptAnomalyType type, Throwable ex) {
        String message = truncate(rootCause(ex).getMessage(), MAX_ERROR_MESSAGE_LENGTH);
        String prefix = switch (type) {
            case LUA_TIMEOUT -> "Redis Lua 执行超时，接单状态未知";
            case REDIS_BUSY -> "Redis BUSY，接单状态未知";
            case ACCEPT_STATE_UNKNOWN -> "Redis Lua 执行状态未知";
            case WAIT_CANCEL_FAILED -> "Redis WAIT/cancel 状态未知";
        };
        return hasText(message) ? prefix + ": " + message : prefix;
    }

    private static Throwable rootCause(Throwable ex) {
        Throwable current = ex;
        while (current != null && current.getCause() != null) {
            current = current.getCause();
        }
        return current == null ? new IllegalStateException("unknown redis accept error") : current;
    }

    private static String abbreviate(String value, int maxLength) {
        if (maxLength <= 0) {
            return "";
        }
        if (value == null || value.length() <= maxLength) {
            return value == null ? "" : value;
        }
        return value.substring(0, maxLength);
    }

    private static String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value == null ? "" : value;
        }
        return value.substring(0, maxLength);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private void releaseSession(String orderId) {
        RechargeRedisSession session = pendingSessions.remove(orderId);
        if (session != null) {
            closeWithErrorLog(session);
        }
    }

    private void logSampledError(String eventKey, Consumer<LogDecision> logger) {
        LogDecision decision = logStormGuard.shouldLog(eventKey);
        if (decision.shouldLog()) {
            logger.accept(decision);
        }
    }

    private static CancelUnconfirmedAcceptResult toCancelResult(Object rawResult) {
        Map<String, String> fields = flatResult(rawResult);
        String code = fields.getOrDefault("code", "CANCEL_FAILED");
        if ("CANCELLED".equals(code)) {
            return CancelUnconfirmedAcceptResult.success();
        }
        return CancelUnconfirmedAcceptResult.failed(code, fields.getOrDefault("fail_reason", code));
    }

    private static MarkPersistQueuedResult toMarkResult(Object rawResult) {
        Map<String, String> fields = flatResult(rawResult);
        String code = fields.getOrDefault("code", "MARK_FAILED");
        if ("MARKED".equals(code)) {
            return MarkPersistQueuedResult.success();
        }
        return MarkPersistQueuedResult.failed(code, fields.getOrDefault("fail_reason", code));
    }

    private static MarkPersistStateResult toPersistStateMarkResult(Object rawResult) {
        Map<String, String> fields = flatResult(rawResult);
        String code = fields.getOrDefault("code", "MARK_FAILED");
        if ("MARKED".equals(code)) {
            return MarkPersistStateResult.success();
        }
        return MarkPersistStateResult.failed(code, fields.getOrDefault("fail_reason", code));
    }

    private static Map<String, String> flatResult(Object rawResult) {
        if (!(rawResult instanceof List<?> values)) {
            return Map.of("code", String.valueOf(rawResult));
        }
        Map<String, String> fields = new LinkedHashMap<>();
        for (int index = 0; index + 1 < values.size(); index += 2) {
            fields.put(String.valueOf(values.get(index)), String.valueOf(values.get(index + 1)));
        }
        return fields;
    }

    private void closeWithErrorLog(RechargeRedisSession session) {
        try {
            session.close();
        } catch (RuntimeException ex) {
            /*
             * 关闭 Redis 会话失败不改变业务结果，但可能指向连接池泄漏或客户端异常。
             * 该路径在 Redis 故障时可能按请求量触发，按 AGENTS.md 日志风暴规则只记录首条和采样条。
             */
            logSampledError(LOG_EVENT_CLOSE_SESSION_FAILED, decision -> log.error(
                    "关闭Redis接单会话失败 occurrence={}, sampled={}",
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
        }
    }
}
