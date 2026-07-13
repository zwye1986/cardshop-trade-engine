package com.cardshop.trade.engine.simulator.a39;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class A39SimulatorOptionsTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-07-13T08:00:00Z"), ZoneOffset.UTC);

    @Test
    void productIdDefaultsToA39BusinessProduct1011() {
        A39SimulatorOptions options = A39SimulatorOptions.parse(
                new String[]{"--account", "18186222336"},
                requiredEnv(),
                FIXED_CLOCK);

        assertThat(options.productId()).isEqualTo("1011");
    }

    @Test
    void productIdCanBeOverriddenFromCommandLineArgument() {
        A39SimulatorOptions options = A39SimulatorOptions.parse(
                new String[]{"--product-id", "73301280", "--account", "18186222336"},
                requiredEnv(),
                FIXED_CLOCK);

        assertThat(options.productId()).isEqualTo("73301280");
    }

    @Test
    void appIdAndAccountUseA39BusinessDefaultsWhenNotProvided() {
        A39SimulatorOptions options = A39SimulatorOptions.parse(
                new String[]{},
                secretEnv(),
                FIXED_CLOCK);

        assertThat(options.appId()).isEqualTo("A35cecjXS7lZ1W30NMN");
        assertThat(options.account()).isEqualTo("18186222336");
    }

    private static Map<String, String> requiredEnv() {
        return Map.of(
                "A39_APP_ID", "app-id",
                "A39_APP_SECRET", "app-secret",
                "A39_AES_KEY", "AES-KEY"
        );
    }

    private static Map<String, String> secretEnv() {
        return Map.of(
                "A39_APP_SECRET", "app-secret",
                "A39_AES_KEY", "AES-KEY"
        );
    }
}
