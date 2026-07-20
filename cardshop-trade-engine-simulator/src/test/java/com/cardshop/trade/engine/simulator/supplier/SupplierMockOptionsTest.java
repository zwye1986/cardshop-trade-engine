package com.cardshop.trade.engine.simulator.supplier;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SupplierMockOptionsTest {

    @Test
    void defaultsWithEmptyArgs() {
        SupplierMockOptions options = SupplierMockOptions.parse(new String[]{}, Map.of());

        assertThat(options.bindHost()).isEqualTo(SupplierMockOptions.DEFAULT_BIND_HOST);
        assertThat(options.port()).isEqualTo(SupplierMockOptions.DEFAULT_PORT);
        assertThat(options.finalResultDelayMsMin()).isEqualTo(SupplierMockOptions.DEFAULT_DELAY_MIN_MS);
        assertThat(options.finalResultDelayMsMax()).isEqualTo(SupplierMockOptions.DEFAULT_DELAY_MAX_MS);
        assertThat(options.successRate()).isEqualTo(SupplierMockOptions.DEFAULT_SUCCESS_RATE);
    }

    @Test
    void rejectsWhenMinGreaterThanMax() {
        assertThatThrownBy(() -> SupplierMockOptions.parse(new String[]{
                "--final-result-delay-ms-min", "9000",
                "--final-result-delay-ms-max", "1000"
        }, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("final-result-delay");
    }

    @Test
    void rejectsInvalidSuccessRate() {
        assertThatThrownBy(() -> SupplierMockOptions.parse(new String[]{
                "--success-rate", "1.5"
        }, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("success-rate");
    }

    @Test
    void parsesDefaultsAndOverrides() {
        SupplierMockOptions options = SupplierMockOptions.parse(new String[]{
                "--bind-host", "127.0.0.1",
                "--port", "18083",
                "--final-result-delay-ms-min", "100",
                "--final-result-delay-ms-max", "200",
                "--success-rate", "0.5"
        }, Map.of());
        assertThat(options.bindHost()).isEqualTo("127.0.0.1");
        assertThat(options.port()).isEqualTo(18083);
        assertThat(options.finalResultDelayMsMin()).isEqualTo(100);
        assertThat(options.finalResultDelayMsMax()).isEqualTo(200);
        assertThat(options.successRate()).isEqualTo(0.5);
    }

    @Test
    void commandLineOverridesEnvironment() {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("SUPPLIER_MOCK_BIND_HOST", "10.0.0.1");
        env.put("SUPPLIER_MOCK_PORT", "19000");
        env.put("SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MIN", "500");
        env.put("SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MAX", "600");
        env.put("SUPPLIER_MOCK_SUCCESS_RATE", "0.8");

        SupplierMockOptions options = SupplierMockOptions.parse(new String[]{
                "--bind-host", "127.0.0.1",
                "--port", "18083"
        }, env);

        assertThat(options.bindHost()).isEqualTo("127.0.0.1");
        assertThat(options.port()).isEqualTo(18083);
        assertThat(options.finalResultDelayMsMin()).isEqualTo(500);
        assertThat(options.finalResultDelayMsMax()).isEqualTo(600);
        assertThat(options.successRate()).isEqualTo(0.8);
    }

    @Test
    void environmentOverridesDefaults() {
        Map<String, String> env = Map.of(
                "SUPPLIER_MOCK_BIND_HOST", "192.168.1.10",
                "SUPPLIER_MOCK_PORT", "19001",
                "SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MIN", "300",
                "SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MAX", "400",
                "SUPPLIER_MOCK_SUCCESS_RATE", "0.25"
        );

        SupplierMockOptions options = SupplierMockOptions.parse(new String[]{}, env);

        assertThat(options.bindHost()).isEqualTo("192.168.1.10");
        assertThat(options.port()).isEqualTo(19001);
        assertThat(options.finalResultDelayMsMin()).isEqualTo(300);
        assertThat(options.finalResultDelayMsMax()).isEqualTo(400);
        assertThat(options.successRate()).isEqualTo(0.25);
    }

    @Test
    void sanitizedSummaryIncludesAllConfiguration() {
        SupplierMockOptions options = SupplierMockOptions.parse(new String[]{
                "--bind-host", "127.0.0.1",
                "--port", "18083",
                "--success-rate", "0.5"
        }, Map.of());

        assertThat(options.sanitizedSummary())
                .contains("bindHost=127.0.0.1")
                .contains("port=18083")
                .contains("finalResultDelayMsMin=")
                .contains("finalResultDelayMsMax=")
                .contains("successRate=0.5");
    }

    @Test
    void usageDocumentsSupplierMockOptions() {
        String usage = SupplierMockOptions.usage();

        assertThat(usage)
                .contains("--bind-host")
                .contains("--port")
                .contains("--final-result-delay-ms-min")
                .contains("--final-result-delay-ms-max")
                .contains("--success-rate")
                .contains("SUPPLIER_MOCK_BIND_HOST")
                .contains("SUPPLIER_MOCK_PORT")
                .contains("SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MIN")
                .contains("SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MAX")
                .contains("SUPPLIER_MOCK_SUCCESS_RATE");
    }
}
