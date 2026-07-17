package com.cardshop.trade.engine.application.v1;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * V1 商品缓存时间窗口配置测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 */
class V1ProductCacheSettingsTest {

    @Test
    void acceptsOrderedPositiveDurations() {
        assertThatCode(() -> settings(
                Duration.ofMinutes(25),
                Duration.ofMinutes(30),
                Duration.ofMinutes(60)))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsRefreshAfterAtOrBeyondMaxStaleAge() {
        assertThatThrownBy(() -> settings(
                Duration.ofMinutes(30),
                Duration.ofMinutes(30),
                Duration.ofMinutes(60)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refreshAfter");
    }

    @Test
    void rejectsMaxStaleAgeAtOrBeyondPhysicalTtl() {
        assertThatThrownBy(() -> settings(
                Duration.ofMinutes(25),
                Duration.ofMinutes(60),
                Duration.ofMinutes(60)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxStaleAge");
    }

    @Test
    void rejectsNonPositiveLockAndWaitDurations() {
        assertThatThrownBy(() -> new V1ProductCacheSettings(
                Duration.ofMinutes(25),
                Duration.ofMinutes(30),
                Duration.ofMinutes(60),
                Duration.ZERO,
                Duration.ofSeconds(1),
                Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refreshLockTtl");

        assertThatThrownBy(() -> new V1ProductCacheSettings(
                Duration.ofMinutes(25),
                Duration.ofMinutes(30),
                Duration.ofMinutes(60),
                Duration.ofSeconds(5),
                Duration.ZERO,
                Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refreshWaitTimeout");
    }

    private static V1ProductCacheSettings settings(
            Duration refreshAfter,
            Duration maxStaleAge,
            Duration physicalTtl) {
        return new V1ProductCacheSettings(
                refreshAfter,
                maxStaleAge,
                physicalTtl,
                Duration.ofSeconds(5),
                Duration.ofSeconds(1),
                Duration.ofMinutes(1));
    }
}
