package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.logging.LogStormGuard.LogDecision;
import com.cardshop.trade.engine.application.recharge.BalanceReservationReleaseCommand;
import com.cardshop.trade.engine.application.recharge.BalanceReservationReleasePort;
import com.cardshop.trade.engine.application.recharge.BalanceReservationReleaseResult;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis Lua 余额 reservation 释放适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「14.6 订单最终失败/作废退款协议」
 * 和「16.6 release.lua」。供应商最终失败或作废成功后，本适配器通过同商户 hash tag 下的 Lua
 * 原子回补 {@code available/frozen}，并以 {@code releaseEventId} 防止 outbox 重试重复释放。
 */
public class RedisBalanceReservationReleaseAdapter implements BalanceReservationReleasePort {

    private static final Logger log = LoggerFactory.getLogger(RedisBalanceReservationReleaseAdapter.class);
    private static final String LOG_EVENT_OPEN_SESSION_FAILED = "redis-release-balance.open-session.failed";
    private static final String LOG_EVENT_LUA_FAILED = "redis-release-balance.lua.failed";
    private static final String LOG_EVENT_CLOSE_SESSION_FAILED = "redis-release-balance.close-session.failed";
    private static final String STATUS_RELEASED = "RELEASED";
    private static final String STATUS_ALREADY_RELEASED = "ALREADY_RELEASED";
    private static final String DEFAULT_FAIL_CODE = "RELEASE_FAILED";

    private final RechargeRedisSessionFactory sessionFactory;
    private final RechargeRedisKeyBuilder keyBuilder;
    private final RechargeRedisScriptLoader scriptLoader;
    private final Clock clock;
    private final LogStormGuard logStormGuard = LogStormGuard.defaultGuard();

    public RedisBalanceReservationReleaseAdapter(
            RechargeRedisSessionFactory sessionFactory,
            RechargeRedisKeyBuilder keyBuilder,
            RechargeRedisScriptLoader scriptLoader,
            Clock clock) {
        this.sessionFactory = Objects.requireNonNull(sessionFactory, "sessionFactory must not be null");
        this.keyBuilder = Objects.requireNonNull(keyBuilder, "keyBuilder must not be null");
        this.scriptLoader = Objects.requireNonNull(scriptLoader, "scriptLoader must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public BalanceReservationReleaseResult release(BalanceReservationReleaseCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        RechargeRedisSession session;
        try {
            session = sessionFactory.openSession();
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_OPEN_SESSION_FAILED, decision -> log.error(
                    "打开Redis余额释放会话失败 orgCode={}, outOrderSn={}, orderId={}, releaseEventId={}, occurrence={}, sampled={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    command.orderId(),
                    command.releaseEventId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            return BalanceReservationReleaseResult.failed("REDIS_SESSION_UNAVAILABLE", "Redis 会话不可用");
        }
        try {
            Object rawResult = session
                    .execute(
                            scriptLoader.releaseBalanceReservationScript(),
                            keyBuilder.releaseBalanceReservationKeys(command),
                            args(command))
                    .block();
            return toResult(rawResult);
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_LUA_FAILED, decision -> log.error(
                    "Redis余额预扣释放Lua失败 orgCode={}, outOrderSn={}, orderId={}, releaseEventId={}, occurrence={}, sampled={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    command.orderId(),
                    command.releaseEventId(),
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
            return BalanceReservationReleaseResult.failed("RELEASE_LUA_ERROR", "Redis 余额释放脚本执行失败");
        } finally {
            closeWithErrorLog(session);
        }
    }

    private List<String> args(BalanceReservationReleaseCommand command) {
        return List.of(
                command.orgCode(),
                command.orderId(),
                command.outOrderSn(),
                Long.toString(Math.max(0, command.amount())),
                normalize(command.releaseType(), "RELEASE"),
                command.releaseEventId() == null ? "" : command.releaseEventId(),
                command.reason() == null ? "" : command.reason(),
                Long.toString(clock.instant().toEpochMilli()));
    }

    private static BalanceReservationReleaseResult toResult(Object rawResult) {
        Map<String, String> fields = flatResult(rawResult);
        String code = fields.getOrDefault("code", DEFAULT_FAIL_CODE);
        if (STATUS_RELEASED.equals(code)) {
            return BalanceReservationReleaseResult.releasedSuccess();
        }
        if (STATUS_ALREADY_RELEASED.equals(code)) {
            return BalanceReservationReleaseResult.alreadyReleasedSuccess();
        }
        return BalanceReservationReleaseResult.failed(
                fields.getOrDefault("fail_code", code),
                fields.getOrDefault("fail_reason", code));
    }

    private static Map<String, String> flatResult(Object rawResult) {
        if (!(rawResult instanceof List<?> values)) {
            return Map.of("code", String.valueOf(rawResult));
        }
        values = flattenSingleNestedList(values);
        Map<String, String> fields = new LinkedHashMap<>();
        for (int index = 0; index + 1 < values.size(); index += 2) {
            fields.put(toRedisScalarString(values.get(index)), toRedisScalarString(values.get(index + 1)));
        }
        return fields;
    }

    private static List<?> flattenSingleNestedList(List<?> values) {
        if (values.size() == 1 && values.getFirst() instanceof List<?> nested) {
            return nested;
        }
        return values;
    }

    private static String toRedisScalarString(Object rawValue) {
        if (rawValue == null) {
            return "";
        }
        if (rawValue instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(rawValue);
    }

    private static String normalize(String value, String defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.strip().toUpperCase(java.util.Locale.ROOT);
    }

    private void closeWithErrorLog(RechargeRedisSession session) {
        try {
            session.close();
        } catch (RuntimeException ex) {
            logSampledError(LOG_EVENT_CLOSE_SESSION_FAILED, decision -> log.error(
                    "关闭Redis余额释放会话失败 occurrence={}, sampled={}",
                    decision.occurrence(),
                    decision.sampledOccurrence(),
                    ex));
        }
    }

    private void logSampledError(String eventKey, Consumer<LogDecision> logger) {
        LogDecision decision = logStormGuard.shouldLog(eventKey);
        if (decision.shouldLog()) {
            logger.accept(decision);
        }
    }
}
