package com.cardshop.trade.engine.api.v1.security;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class V1TimestampValidatorTest {

    private final V1TimestampValidator validator = new V1TimestampValidator(
            new V1TimestampSettings(Duration.ofSeconds(300)),
            Clock.fixed(Instant.parse("2026-07-05T04:15:58Z"), ZoneOffset.UTC)
    );

    @Test
    void acceptsSecondAndMillisecondTimestampsWithinSkew() {
        assertThat(validator.validate("1783224958").valid()).isTrue();
        assertThat(validator.validate("1783224958000").valid()).isTrue();
    }

    @Test
    void rejectsTimestampsOutsideAllowedSkew() {
        V1TimestampVerifyResult result = validator.validate("1783224557");

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo("TIMESTAMP_OUT_OF_RANGE");
    }

    @Test
    void rejectsMalformedTimestamp() {
        V1TimestampVerifyResult result = validator.validate("2026-07-05 12:15:58");

        assertThat(result.valid()).isFalse();
        assertThat(result.reason()).isEqualTo("TIMESTAMP_INVALID");
    }
}
