package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * 基于 Redis Lua 的直充下单受理适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
 * 该类只负责调用 `accept_recharge_order.lua`，用于没有装配同 session WAIT 能力时的兼容受理端口。
 * 生产主链路优先使用 {@link RedisSessionAcceptReliabilityAdapter}，由 Java 编排层继续执行 WAIT、cancel、
 * RabbitMQ confirm 和 `recharge_accept_anomaly` 登记。
 */
public class RedisAcceptRechargeOrderAdapter implements AcceptRechargeOrderPort {

    private static final Logger log = LoggerFactory.getLogger(RedisAcceptRechargeOrderAdapter.class);

    private final RechargeRedisScriptExecutor scriptExecutor;
    private final RechargeRedisKeyBuilder keyBuilder;
    private final RechargeRedisScriptLoader scriptLoader;
    private final AcceptRechargeOrderLuaResultMapper resultMapper = new AcceptRechargeOrderLuaResultMapper();
    private final TradeEngineProperties properties;
    private final RedisHaGuardPort redisHaGuardPort;

    public RedisAcceptRechargeOrderAdapter(
            RechargeRedisScriptExecutor scriptExecutor,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            TradeEngineProperties properties) {
        this(scriptExecutor, keyBuilder, scriptLoader, properties, RedisHaGuardPort.noop());
    }

    public RedisAcceptRechargeOrderAdapter(
            RechargeRedisScriptExecutor scriptExecutor,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            TradeEngineProperties properties,
            RedisHaGuardPort redisHaGuardPort) {
        this.scriptExecutor = scriptExecutor;
        this.keyBuilder = keyBuilder;
        this.scriptLoader = scriptLoader;
        this.properties = properties;
        this.redisHaGuardPort = redisHaGuardPort == null ? RedisHaGuardPort.noop() : redisHaGuardPort;
    }

    @Override
    public CompletionStage<AcceptRechargeOrderResult> accept(AcceptRechargeOrderCommand command) {
        if (!properties.getRedis().isWriteEnabled() || !redisHaGuardPort.isWriteAllowed()) {
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

        var luaArguments = AcceptRechargeOrderLuaArguments.from(
                command,
                keyBuilder,
                Duration.ofDays(properties.getOrder().getPendingTtlDays()),
                Duration.ofSeconds(properties.getV1().getNonceReplayTtlSeconds()));

        return scriptExecutor
                .execute(scriptLoader.acceptRechargeOrderScript(), luaArguments.keys(), luaArguments.args())
                .switchIfEmpty(Mono.just(redisUnavailable(command, "Lua 未返回结果")))
                .map(result -> result instanceof AcceptRechargeOrderResult acceptResult
                        ? acceptResult
                        : resultMapper.map(result))
                .onErrorResume(ex -> {
                    log.warn(
                            "accept recharge order by redis lua failed, orgCode={}, outOrderSn={}, orderId={}, requestHash={}",
                            command.orgCode(),
                            command.outOrderSn(),
                            command.orderId(),
                            command.requestHash(),
                            ex);
                    // TODO(D5B): Lua 超时、Redis BUSY 或状态未知时写 recharge_accept_anomaly 并触发核查；参见 docs/交行直充业务系统详细设计.md「20.13.6 Redis Lua 下单结果」。
                    return Mono.just(redisUnavailable(command, "Redis Lua 执行失败"));
                })
                .toFuture();
    }

    private static AcceptRechargeOrderResult redisUnavailable(
            AcceptRechargeOrderCommand command, String failReason) {
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
}
