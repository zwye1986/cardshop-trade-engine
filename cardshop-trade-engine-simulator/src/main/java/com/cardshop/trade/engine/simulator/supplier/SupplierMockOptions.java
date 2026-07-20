package com.cardshop.trade.engine.simulator.supplier;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 供应商 HTTP mock 启动参数。
 *
 * <p>独立于 A39 商户凭证解析，供 {@code --mode=supplier-mock} 使用。参见
 * {@code docs/superpowers/specs/2026-07-20-supplier-mock-design.md}「配置契约」。
 */
public record SupplierMockOptions(
        String bindHost,
        int port,
        long finalResultDelayMsMin,
        long finalResultDelayMsMax,
        double successRate) {

    public static final String DEFAULT_BIND_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 18082;
    public static final long DEFAULT_DELAY_MIN_MS = 1200L;
    public static final long DEFAULT_DELAY_MAX_MS = 9800L;
    public static final double DEFAULT_SUCCESS_RATE = 1.0;

    public SupplierMockOptions {
        bindHost = requireText(bindHost, "bindHost");
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        if (finalResultDelayMsMin < 0 || finalResultDelayMsMax < 0) {
            throw new IllegalArgumentException("final-result-delay must not be negative");
        }
        if (finalResultDelayMsMin > finalResultDelayMsMax) {
            throw new IllegalArgumentException("final-result-delay-ms-min must not be greater than final-result-delay-ms-max");
        }
        if (successRate < 0.0 || successRate > 1.0) {
            throw new IllegalArgumentException("success-rate must be between 0.0 and 1.0");
        }
    }

    public static SupplierMockOptions parse(String[] args) {
        return parse(args, System.getenv());
    }

    public static SupplierMockOptions parse(String[] args, Map<String, String> env) {
        Map<String, String> options = parseArgs(args);
        return new SupplierMockOptions(
                option(options, env, "bind-host", "SUPPLIER_MOCK_BIND_HOST", DEFAULT_BIND_HOST),
                integer(option(options, env, "port", "SUPPLIER_MOCK_PORT", String.valueOf(DEFAULT_PORT)), "port"),
                longValue(
                        option(options, env, "final-result-delay-ms-min", "SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MIN",
                                String.valueOf(DEFAULT_DELAY_MIN_MS)),
                        "final-result-delay-ms-min"),
                longValue(
                        option(options, env, "final-result-delay-ms-max", "SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MAX",
                                String.valueOf(DEFAULT_DELAY_MAX_MS)),
                        "final-result-delay-ms-max"),
                doubleValue(option(options, env, "success-rate", "SUPPLIER_MOCK_SUCCESS_RATE",
                        String.valueOf(DEFAULT_SUCCESS_RATE)), "success-rate")
        );
    }

    public String sanitizedSummary() {
        return "bindHost=%s, port=%d, finalResultDelayMsMin=%d, finalResultDelayMsMax=%d, successRate=%s"
                .formatted(bindHost, port, finalResultDelayMsMin, finalResultDelayMsMax, successRate);
    }

    public static String usage() {
        return """
                Supplier mock simulator (--mode=supplier-mock)

                Options:
                  --bind-host HOST
                  --port PORT
                  --final-result-delay-ms-min MS
                  --final-result-delay-ms-max MS
                  --success-rate RATE

                Defaults:
                  bind-host 0.0.0.0, port 18082, delay 1200-9800 ms, success-rate 1.0

                Environment fallbacks:
                  SUPPLIER_MOCK_BIND_HOST SUPPLIER_MOCK_PORT
                  SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MIN SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MAX
                  SUPPLIER_MOCK_SUCCESS_RATE

                Example:
                  mvn -f cardshop-trade-engine-simulator/pom.xml exec:java \\
                    -Dexec.args="--mode supplier-mock --port 18082 --success-rate 0.95"
                """;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < args.length; index++) {
            String arg = args[index];
            if ("--help".equals(arg) || "-h".equals(arg)) {
                values.put("help", "true");
                continue;
            }
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
            String key = arg.substring(2);
            String value;
            int equalsIndex = key.indexOf('=');
            if (equalsIndex >= 0) {
                value = key.substring(equalsIndex + 1);
                key = key.substring(0, equalsIndex);
            } else {
                if (index + 1 >= args.length || args[index + 1].startsWith("--")) {
                    value = "true";
                } else {
                    value = args[++index];
                }
            }
            values.put(key, value);
        }
        return values;
    }

    private static String option(Map<String, String> options, Map<String, String> env, String key, String envKey, String fallback) {
        String value = options.get(key);
        if (hasText(value)) {
            return value;
        }
        value = env.get(envKey);
        if (hasText(value)) {
            return value;
        }
        return fallback;
    }

    private static int integer(String value, String name) {
        try {
            return Integer.parseInt(requireText(value, name));
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(name + " must be an integer", ex);
        }
    }

    private static long longValue(String value, String name) {
        try {
            return Long.parseLong(requireText(value, name));
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(name + " must be a long", ex);
        }
    }

    private static double doubleValue(String value, String name) {
        try {
            return Double.parseDouble(requireText(value, name));
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(name + " must be a number", ex);
        }
    }

    private static String requireText(String value, String name) {
        if (!hasText(value)) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
