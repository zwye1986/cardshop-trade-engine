package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Redis HA Guard 本地写入熔断测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.3 Redis HA Guard」。当前 D5B 先实现应用节点内
 * fail closed：Nacos 静态开关和运行时关闭任一为 false，都不得继续执行 Redis 写入型 Lua。
 */
class InMemoryRedisHaGuardTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-06T08:30:00Z"), ZoneOffset.UTC);

    @Test
    void disallowsWriteAfterRuntimeCloseEvenWhenConfigSwitchIsEnabled() {
        TradeEngineProperties properties = new TradeEngineProperties();
        properties.getRedis().setWriteEnabled(true);
        var guard = new InMemoryRedisHaGuard(properties, CLOCK);

        guard.closeWrite("WAIT_FAILED_CANCEL_FAILED", Map.of("org_code", "A36"));

        assertThat(guard.isWriteAllowed()).isFalse();
        assertThat(guard.lastCloseReason()).isEqualTo("WAIT_FAILED_CANCEL_FAILED");
        assertThat(guard.lastCloseAt()).isEqualTo(Instant.parse("2026-07-06T08:30:00Z"));
        assertThat(guard.lastCloseContext()).containsEntry("org_code", "A36");
    }

    @Test
    void disallowsWriteWhenConfigSwitchIsDisabledBeforeRuntimeClose() {
        TradeEngineProperties properties = new TradeEngineProperties();
        properties.getRedis().setWriteEnabled(false);
        var guard = new InMemoryRedisHaGuard(properties, CLOCK);

        assertThat(guard.isWriteAllowed()).isFalse();
        assertThat(guard.lastCloseReason()).isEmpty();
    }
}
