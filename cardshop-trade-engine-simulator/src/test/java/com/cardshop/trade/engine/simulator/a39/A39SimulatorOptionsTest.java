package com.cardshop.trade.engine.simulator.a39;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void requestSm4DefaultsToRealV1ProtocolAndCanBeDisabledExplicitly() {
        A39SimulatorOptions defaultOptions = A39SimulatorOptions.parse(
                new String[]{},
                secretEnv(),
                FIXED_CLOCK);
        A39SimulatorOptions plainOptions = A39SimulatorOptions.parse(
                new String[]{"--request-sm4", "false"},
                secretEnv(),
                FIXED_CLOCK);

        assertThat(defaultOptions.requestSm4Enabled()).isTrue();
        assertThat(defaultOptions.sanitizedSummary()).contains("requestSm4=true");
        assertThat(defaultOptions.sanitizedSummary())
                .contains("mode=", "concurrency=", "totalOrders=", "requestTimeout=");
        assertThat(plainOptions.requestSm4Enabled()).isFalse();
        assertThat(plainOptions.sanitizedSummary()).contains("requestSm4=false");
    }

    @Test
    void scenarioModeIsDefaultAndKeepsCallbackDefaults() {
        A39SimulatorOptions options = A39SimulatorOptions.parse(
                new String[]{},
                secretEnv(),
                FIXED_CLOCK);

        assertThat(options.mode()).isEqualTo(A39SimulatorMode.SCENARIO);
        assertThat(options.notifyUrl()).isEqualTo("http://127.0.0.1:19039/callback/order");
        assertThat(options.concurrency()).isEqualTo(10);
        assertThat(options.totalOrders()).isEqualTo(200);
        assertThat(options.requestTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(options.expectedCreateCodes()).containsExactly("200", "510");
    }

    @Test
    void loadModeDefaultsToBlankNotifyUrlAndConfigurableLoadParameters() {
        A39SimulatorOptions options = A39SimulatorOptions.parse(
                new String[]{
                        "--mode", "load",
                        "--concurrency", "12",
                        "--total-orders", "250",
                        "--request-timeout", "7s",
                        "--out-order-prefix", "PERF-A39",
                        "--expected-create-codes", "200,510,599",
                        "--error-sample-limit", "5",
                        "--progress-interval", "0"
                },
                secretEnv(),
                FIXED_CLOCK);

        assertThat(options.mode()).isEqualTo(A39SimulatorMode.LOAD);
        assertThat(options.notifyUrl()).isEmpty();
        assertThat(options.concurrency()).isEqualTo(12);
        assertThat(options.totalOrders()).isEqualTo(250);
        assertThat(options.requestTimeout()).isEqualTo(Duration.ofSeconds(7));
        assertThat(options.outOrderPrefix()).isEqualTo("PERF-A39");
        assertThat(options.expectedCreateCodes()).containsExactly("200", "510", "599");
        assertThat(options.errorSampleLimit()).isEqualTo(5);
        assertThat(options.progressInterval()).isEqualTo(Duration.ZERO);
    }

    @Test
    void loadModeRejectsNonBlankNotifyUrl() {
        assertThatThrownBy(() -> A39SimulatorOptions.parse(
                new String[]{"--mode", "load", "--notify-url", "http://127.0.0.1:19039/callback/order"},
                secretEnv(),
                FIXED_CLOCK))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("notifyUrl must be blank in load mode");
    }

    @Test
    void explicitBlankNotifyUrlIsPreservedWhenUsingEqualsSyntax() {
        A39SimulatorOptions options = A39SimulatorOptions.parse(
                new String[]{"--mode", "load", "--notify-url="},
                secretEnv(),
                FIXED_CLOCK);

        assertThat(options.notifyUrl()).isEmpty();
    }

    @Test
    void scenarioModeIgnoresBlankCommandLineNotifyUrlAndUsesCallbackDefault() {
        A39SimulatorOptions options = A39SimulatorOptions.parse(
                new String[]{"--notify-url="},
                secretEnv(),
                FIXED_CLOCK);

        assertThat(options.mode()).isEqualTo(A39SimulatorMode.SCENARIO);
        assertThat(options.notifyUrl()).isEqualTo("http://127.0.0.1:19039/callback/order");
    }

    @Test
    void scenarioModeIgnoresBlankEnvironmentNotifyUrlAndUsesCallbackDefault() {
        Map<String, String> env = new LinkedHashMap<>(secretEnv());
        env.put("A39_NOTIFY_URL", "");

        A39SimulatorOptions options = A39SimulatorOptions.parse(
                new String[]{},
                env,
                FIXED_CLOCK);

        assertThat(options.mode()).isEqualTo(A39SimulatorMode.SCENARIO);
        assertThat(options.notifyUrl()).isEqualTo("http://127.0.0.1:19039/callback/order");
    }

    @Test
    void directConstructionRejectsBlankNotifyUrlInScenarioMode() {
        assertThatThrownBy(() -> new A39SimulatorOptions(
                "http://apitest.kalaiwang.com",
                "app-id",
                "app-secret",
                "AES-KEY",
                "73301280",
                "18186222336",
                "OUT-1",
                "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR",
                "1783224958",
                1,
                5200L,
                1,
                "",
                3,
                Duration.ZERO,
                false,
                "127.0.0.1",
                19039,
                Duration.ZERO,
                A39SimulatorMode.SCENARIO,
                10,
                200,
                Duration.ofSeconds(10),
                "A39TEST",
                List.of("200", "510"),
                20,
                Duration.ZERO
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("notifyUrl must not be blank in scenario mode");
    }

    @Test
    void loadOptionsUseEnvironmentAndCommandLineOverridesEnvironment() {
        Map<String, String> env = new LinkedHashMap<>(secretEnv());
        env.put("A39_MODE", "load");
        env.put("A39_LOAD_CONCURRENCY", "8");
        env.put("A39_LOAD_TOTAL_ORDERS", "88");
        env.put("A39_REQUEST_TIMEOUT", "6s");
        env.put("A39_EXPECTED_CREATE_CODES", "200");

        A39SimulatorOptions options = A39SimulatorOptions.parse(
                new String[]{"--concurrency", "9"},
                env,
                FIXED_CLOCK);

        assertThat(options.mode()).isEqualTo(A39SimulatorMode.LOAD);
        assertThat(options.concurrency()).isEqualTo(9);
        assertThat(options.totalOrders()).isEqualTo(88);
        assertThat(options.requestTimeout()).isEqualTo(Duration.ofSeconds(6));
        assertThat(options.expectedCreateCodes()).containsExactly("200");
    }

    @Test
    void blankCommandLineModeOverridesEnvironmentAsScenario() {
        Map<String, String> env = new LinkedHashMap<>(secretEnv());
        env.put("A39_MODE", "load");

        A39SimulatorOptions options = A39SimulatorOptions.parse(
                new String[]{"--mode="},
                env,
                FIXED_CLOCK);

        assertThat(options.mode()).isEqualTo(A39SimulatorMode.SCENARIO);
        assertThat(options.notifyUrl()).isEqualTo("http://127.0.0.1:19039/callback/order");
    }

    @Test
    void invalidLoadParametersAreRejected() {
        assertThatThrownBy(() -> A39SimulatorOptions.parse(
                new String[]{"--mode", "weird"},
                secretEnv(),
                FIXED_CLOCK))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mode must be scenario or load");

        assertThatThrownBy(() -> A39SimulatorOptions.parse(
                new String[]{"--mode", "load", "--concurrency", "0"},
                secretEnv(),
                FIXED_CLOCK))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concurrency must be greater than 0");

        assertThatThrownBy(() -> A39SimulatorOptions.parse(
                new String[]{"--mode", "load", "--total-orders", "0"},
                secretEnv(),
                FIXED_CLOCK))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("totalOrders must be greater than 0");

        assertThatThrownBy(() -> A39SimulatorOptions.parse(
                new String[]{"--mode", "load", "--expected-create-codes", ""},
                secretEnv(),
                FIXED_CLOCK))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expectedCreateCodes must not be empty");

        assertThatThrownBy(() -> A39SimulatorOptions.parse(
                new String[]{"--mode", "load", "--error-sample-limit", "-1"},
                secretEnv(),
                FIXED_CLOCK))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("errorSampleLimit must not be negative");
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
