package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于本机内存状态的 Redis HA 写入门禁。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「15. Redis 高可用设计」。D5B 先实现节点内 fail closed：
 * Nacos/配置写入开关关闭，或运行时因为 WAIT/cancel 未知态主动关闭写入，都会让下单入口在 `accept.lua` 前拒绝。
 * Sentinel 事件监听、跨节点状态同步和自动恢复条件后续继续接入该类或其替代实现。
 *
 * <p>TODO(D5C/D6B): 接入 Sentinel switch-master、Lettuce topology/连接重建事件、异常核查收敛结果和
 * Nacos/跨节点写入开关，完成“修复任务确认余额、pending、订单快照和异常订单后再恢复写入”的自动恢复闭环；
 * 参见 docs/交行直充业务系统详细设计.md「6.3 Redis HA Guard 与状态未知窗口」和
 * docs/交行直充业务系统高可用方案设计.md「13. Redis 主从切换与恢复时序图」。
 */
public class InMemoryRedisHaGuard implements RedisHaGuardPort {

    private static final Logger log = LoggerFactory.getLogger(InMemoryRedisHaGuard.class);

    private final TradeEngineProperties properties;
    private final Clock clock;
    private final AtomicBoolean runtimeClosed = new AtomicBoolean();
    private final AtomicReference<String> lastCloseReason = new AtomicReference<>("");
    private final AtomicReference<Instant> lastCloseAt = new AtomicReference<>();
    private final AtomicReference<Map<String, Object>> lastCloseContext = new AtomicReference<>(Map.of());

    public InMemoryRedisHaGuard(TradeEngineProperties properties, Clock clock) {
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * 只有配置开关和运行时 HA Guard 都允许时，才允许执行 Redis 写入型 Lua。
     */
    @Override
    public boolean isWriteAllowed() {
        return properties.getRedis().isWriteEnabled() && !runtimeClosed.get();
    }

    /**
     * 关闭本节点 Redis 写入，并保留最后一次关闭原因。
     *
     * <p>该方法可以被重复调用；第一次调用用于状态翻转，后续调用刷新原因和上下文，便于排查最近一次故障窗口。
     */
    @Override
    public void closeWrite(String reason, Map<String, Object> context) {
        String actualReason = hasText(reason) ? reason : "UNKNOWN";
        Map<String, Object> safeContext = immutableContext(context);
        Instant now = clock.instant();
        boolean firstClose = runtimeClosed.compareAndSet(false, true);
        lastCloseReason.set(actualReason);
        lastCloseAt.set(now);
        lastCloseContext.set(safeContext);
        if (firstClose) {
            log.warn("Redis HA Guard关闭写入 reason={}, context={}", actualReason, safeContext);
        } else {
            log.warn("Redis HA Guard已关闭，刷新关闭原因 reason={}, context={}", actualReason, safeContext);
        }
    }

    public String lastCloseReason() {
        return lastCloseReason.get();
    }

    public Instant lastCloseAt() {
        return lastCloseAt.get();
    }

    public Map<String, Object> lastCloseContext() {
        return lastCloseContext.get();
    }

    private static Map<String, Object> immutableContext(Map<String, Object> context) {
        if (context == null || context.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(context));
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
