package com.cardshop.trade.engine.application.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class LogStormGuardTest {

    @Test
    void logsFirstOccurrenceForEachEventKey() {
        LogStormGuard guard = LogStormGuard.everyNth(3);

        LogStormGuard.LogDecision redisDecision = guard.shouldLog("redis.session.open");
        LogStormGuard.LogDecision mqDecision = guard.shouldLog("mq.persist.mysql");

        assertThat(redisDecision.shouldLog()).isTrue();
        assertThat(redisDecision.occurrence()).isEqualTo(1);
        assertThat(redisDecision.firstOccurrence()).isTrue();
        assertThat(redisDecision.sampledOccurrence()).isFalse();
        assertThat(mqDecision.shouldLog()).isTrue();
        assertThat(mqDecision.occurrence()).isEqualTo(1);
    }

    @Test
    void suppressesUntilSampleIntervalAndLogsNthOccurrence() {
        LogStormGuard guard = LogStormGuard.everyNth(3);

        LogStormGuard.LogDecision first = guard.shouldLog("redis.session.open");
        LogStormGuard.LogDecision second = guard.shouldLog("redis.session.open");
        LogStormGuard.LogDecision third = guard.shouldLog("redis.session.open");

        assertThat(first.shouldLog()).isTrue();
        assertThat(second.shouldLog()).isFalse();
        assertThat(second.occurrence()).isEqualTo(2);
        assertThat(third.shouldLog()).isTrue();
        assertThat(third.occurrence()).isEqualTo(3);
        assertThat(third.firstOccurrence()).isFalse();
        assertThat(third.sampledOccurrence()).isTrue();
    }

    @Test
    void rejectsNonPositiveSampleInterval() {
        assertThatThrownBy(() -> LogStormGuard.everyNth(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sampleInterval");
    }

    @Test
    void suppressesRepeatedEventsUntilTimeWindowElapses() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-15T04:00:00Z"));
        LogStormGuard guard = LogStormGuard.atMostOncePer(Duration.ofSeconds(10), clock);

        LogStormGuard.LogDecision first = guard.shouldLog("v1-product-cache.redis-failed");
        LogStormGuard.LogDecision suppressed = guard.shouldLog("v1-product-cache.redis-failed");
        clock.advance(Duration.ofSeconds(10));
        LogStormGuard.LogDecision sampled = guard.shouldLog("v1-product-cache.redis-failed");

        assertThat(first.shouldLog()).isTrue();
        assertThat(first.firstOccurrence()).isTrue();
        assertThat(suppressed.shouldLog()).isFalse();
        assertThat(sampled.shouldLog()).isTrue();
        assertThat(sampled.occurrence()).isEqualTo(3);
        assertThat(sampled.sampledOccurrence()).isTrue();
    }

    @Test
    void rejectsNonPositiveTimeWindow() {
        assertThatThrownBy(() -> LogStormGuard.atMostOncePer(Duration.ZERO, Clock.systemUTC()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("window");
    }

    private static final class MutableClock extends Clock {

        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
