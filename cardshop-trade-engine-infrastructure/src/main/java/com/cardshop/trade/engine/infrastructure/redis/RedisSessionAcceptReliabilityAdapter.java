package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import com.cardshop.trade.engine.application.recharge.CancelUnconfirmedAcceptResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistQueuedResult;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.ReplicationWaitResult;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private final RechargeRedisSessionFactory sessionFactory;
    private final RechargeRedisKeyBuilder keyBuilder;
    private final RechargeRedisScriptLoader scriptLoader;
    private final TradeEngineProperties properties;
    private final AcceptRechargeOrderLuaResultMapper acceptResultMapper = new AcceptRechargeOrderLuaResultMapper();
    private final Map<String, RechargeRedisSession> pendingSessions = new ConcurrentHashMap<>();

    public RedisSessionAcceptReliabilityAdapter(
            RechargeRedisSessionFactory sessionFactory,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            TradeEngineProperties properties) {
        this.sessionFactory = sessionFactory;
        this.keyBuilder = keyBuilder;
        this.scriptLoader = scriptLoader;
        this.properties = properties;
    }

    @Override
    public CompletionStage<AcceptRechargeOrderResult> accept(AcceptRechargeOrderCommand command) {
        if (!properties.getRedis().isWriteEnabled()) {
            return CompletableFuture.completedFuture(new AcceptRechargeOrderResult(
                    AcceptRechargeOrderStatus.REDIS_WRITE_DISABLED,
                    command.orderId(),
                    "",
                    "REDIS_WRITE_DISABLED",
                    "Redis 写入开关关闭",
                    false,
                    false,
                    0));
        }

        RechargeRedisSession session;
        try {
            session = sessionFactory.openSession();
        } catch (RuntimeException ex) {
            log.warn("打开Redis接单会话失败 orgCode={}, outOrderSn={}, orderId={}",
                    command.orgCode(), command.outOrderSn(), command.orderId(), ex);
            return CompletableFuture.completedFuture(redisUnavailable(command, "Redis 会话不可用"));
        }

        var luaArguments = AcceptRechargeOrderLuaArguments.from(
                command,
                keyBuilder,
                Duration.ofDays(properties.getOrder().getPendingTtlDays()),
                Duration.ofSeconds(properties.getV1().getNonceReplayTtlSeconds()));

        return session
                .execute(scriptLoader.acceptRechargeOrderScript(), luaArguments.keys(), luaArguments.args())
                .switchIfEmpty(Mono.just(redisUnavailable(command, "Lua 未返回结果")))
                .map(result -> result instanceof AcceptRechargeOrderResult acceptResult
                        ? acceptResult
                        : acceptResultMapper.map(result))
                .doOnNext(result -> retainSessionOnlyForWrittenOrder(command, session, result))
                .onErrorResume(ex -> {
                    closeQuietly(session);
                    log.warn(
                            "Redis session接单Lua失败 orgCode={}, outOrderSn={}, orderId={}, requestHash={}",
                            command.orgCode(),
                            command.outOrderSn(),
                            command.orderId(),
                            command.requestHash(),
                            ex);
                    // TODO(D5): Lua 超时、Redis BUSY 或状态未知时写 recharge_accept_anomaly 并触发核查；参见 docs/交行直充业务系统详细设计.md「20.13.6 Redis Lua 下单结果」。
                    return Mono.just(redisUnavailable(command, "Redis Lua 执行失败"));
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
            log.warn("Redis WAIT缺少接单session orgCode={}, outOrderSn={}, orderId={}",
                    command.orgCode(), command.outOrderSn(), acceptResult.orderId());
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
                    log.warn("Redis WAIT执行失败 orgCode={}, outOrderSn={}, orderId={}, replicas={}, timeoutMs={}",
                            command.orgCode(),
                            command.outOrderSn(),
                            acceptResult.orderId(),
                            replicas,
                            timeout.toMillis(),
                            ex);
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
                log.warn("打开Redis取消会话失败 orgCode={}, outOrderSn={}, orderId={}",
                        command.orgCode(), command.outOrderSn(), acceptResult.orderId(), ex);
                return CompletableFuture.completedFuture(
                        CancelUnconfirmedAcceptResult.failed("REDIS_SESSION_UNAVAILABLE", "Redis 会话不可用"));
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
                .doFinally(signal -> closeQuietly(sessionToClose))
                .onErrorResume(ex -> {
                    log.warn("Redis取消未确认接单失败 orgCode={}, outOrderSn={}, orderId={}",
                            command.orgCode(), command.outOrderSn(), acceptResult.orderId(), ex);
                    return Mono.just(CancelUnconfirmedAcceptResult.failed("CANCEL_LUA_ERROR", "Redis 取消脚本执行失败"));
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
            log.warn("打开Redis持久化状态推进会话失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}",
                    command.orgCode(), command.outOrderSn(), acceptResult.orderId(), persistMessageId, ex);
            return CompletableFuture.completedFuture(
                    MarkPersistQueuedResult.failed("REDIS_SESSION_UNAVAILABLE", "Redis 会话不可用"));
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
                .doFinally(signal -> closeQuietly(sessionToClose))
                .onErrorResume(ex -> {
                    log.warn("Redis持久化状态推进失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}",
                            command.orgCode(), command.outOrderSn(), acceptResult.orderId(), persistMessageId, ex);
                    return Mono.just(MarkPersistQueuedResult.failed("MARK_LUA_ERROR", "Redis 状态推进脚本执行失败"));
                })
                .toFuture();
    }

    private void retainSessionOnlyForWrittenOrder(
            AcceptRechargeOrderCommand command,
            RechargeRedisSession session,
            AcceptRechargeOrderResult result) {
        if (!result.orderWritten()) {
            closeQuietly(session);
            return;
        }
        RechargeRedisSession previous = pendingSessions.put(result.orderId(), session);
        if (previous != null && previous != session) {
            closeQuietly(previous);
            log.warn("Redis接单session被新会话覆盖 orgCode={}, outOrderSn={}, orderId={}",
                    command.orgCode(), command.outOrderSn(), result.orderId());
        }
    }

    private void releaseSession(String orderId) {
        RechargeRedisSession session = pendingSessions.remove(orderId);
        if (session != null) {
            closeQuietly(session);
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

    private static AcceptRechargeOrderResult redisUnavailable(
            AcceptRechargeOrderCommand command,
            String failReason) {
        return new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.REDIS_UNAVAILABLE,
                command.orderId(),
                "",
                "REDIS_UNAVAILABLE",
                failReason,
                false,
                false,
                0);
    }

    private static void closeQuietly(RechargeRedisSession session) {
        try {
            session.close();
        } catch (RuntimeException ignored) {
            // 关闭 Redis 会话失败不改变业务结果，连接池/客户端会在后续健康检查暴露真实故障。
        }
    }
}
