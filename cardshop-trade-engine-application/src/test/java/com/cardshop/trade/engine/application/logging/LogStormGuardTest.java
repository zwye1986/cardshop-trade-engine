package com.cardshop.trade.engine.application.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
}
