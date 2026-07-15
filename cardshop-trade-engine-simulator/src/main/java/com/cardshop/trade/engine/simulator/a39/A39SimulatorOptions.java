package com.cardshop.trade.engine.simulator.a39;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A39 模拟器命令行参数。
 *
 * <p>该类只承载 dev/test 工具配置，不保存真实密钥默认值。参见
 * {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」的商户凭证和签名密钥边界：
 * {@code app_secret/aesKey} 必须由命令行参数或环境变量提供，禁止写入仓库。
 */
public record A39SimulatorOptions(
        String baseUrl,
        String appId,
        String appSecret,
        String aesKey,
        String productId,
        String account,
        String outOrderSn,
        String nonceStr,
        String timestamp,
        int num,
        long orderPrice,
        int productType,
        String notifyUrl,
        int queryAttempts,
        Duration queryInterval,
        boolean requestSm4Enabled,
        String callbackBindHost,
        int callbackPort,
        Duration callbackWait,
        A39SimulatorMode mode,
        int concurrency,
        int totalOrders,
        Duration requestTimeout,
        String outOrderPrefix,
        List<String> expectedCreateCodes,
        int errorSampleLimit,
        Duration progressInterval) {

    public static final String DEFAULT_BASE_URL = "http://apitest.kalaiwang.com";
    public static final String DEFAULT_APP_ID = "A35cecjXS7lZ1W30NMN";
    public static final String DEFAULT_PRODUCT_ID = "1011";
    public static final String DEFAULT_ACCOUNT = "18186222336";
    private static final boolean DEFAULT_REQUEST_SM4_ENABLED = true;
    private static final int DEFAULT_CALLBACK_PORT = 19039;
    private static final int DEFAULT_QUERY_ATTEMPTS = 3;
    private static final Duration DEFAULT_QUERY_INTERVAL = Duration.ofSeconds(30);
    private static final Duration DEFAULT_CALLBACK_WAIT = Duration.ofMinutes(10);
    private static final int DEFAULT_LOAD_CONCURRENCY = 10;
    private static final int DEFAULT_LOAD_TOTAL_ORDERS = 200;
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final int DEFAULT_ERROR_SAMPLE_LIMIT = 20;
    private static final Duration DEFAULT_PROGRESS_INTERVAL = Duration.ofSeconds(10);
    private static final DateTimeFormatter ORDER_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final char[] NONCE_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final SecureRandom RANDOM = new SecureRandom();

    public A39SimulatorOptions {
        baseUrl = requireText(baseUrl, "baseUrl");
        appId = requireText(appId, "appId");
        appSecret = requireText(appSecret, "appSecret");
        aesKey = requireText(aesKey, "aesKey");
        productId = requireText(productId, "productId");
        account = requireText(account, "account");
        outOrderSn = requireText(outOrderSn, "outOrderSn");
        nonceStr = requireText(nonceStr, "nonceStr");
        timestamp = requireText(timestamp, "timestamp");
        mode = mode == null ? A39SimulatorMode.SCENARIO : mode;
        notifyUrl = Objects.toString(notifyUrl, "").trim();
        if (mode == A39SimulatorMode.LOAD) {
            if (hasText(notifyUrl)) {
                throw new IllegalArgumentException("notifyUrl must be blank in load mode");
            }
            notifyUrl = "";
        } else if (!hasText(notifyUrl)) {
            throw new IllegalArgumentException("notifyUrl must not be blank in scenario mode");
        }
        callbackBindHost = requireText(callbackBindHost, "callbackBindHost");
        queryInterval = queryInterval == null || queryInterval.isNegative() ? DEFAULT_QUERY_INTERVAL : queryInterval;
        callbackWait = callbackWait == null || callbackWait.isNegative() ? DEFAULT_CALLBACK_WAIT : callbackWait;
        requestTimeout = requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative()
                ? DEFAULT_REQUEST_TIMEOUT
                : requestTimeout;
        outOrderPrefix = hasText(outOrderPrefix) ? outOrderPrefix.trim() : nextOutOrderSn(Clock.systemDefaultZone());
        expectedCreateCodes = normalizeCodes(expectedCreateCodes);
        progressInterval = progressInterval == null || progressInterval.isNegative() ? DEFAULT_PROGRESS_INTERVAL : progressInterval;
        if (num < 1) {
            throw new IllegalArgumentException("num must be greater than 0");
        }
        if (orderPrice < 0) {
            throw new IllegalArgumentException("orderPrice must not be negative");
        }
        if (queryAttempts < 0) {
            throw new IllegalArgumentException("queryAttempts must not be negative");
        }
        if (callbackPort < 0 || callbackPort > 65_535) {
            throw new IllegalArgumentException("callbackPort is invalid");
        }
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be greater than 0");
        }
        if (totalOrders <= 0) {
            throw new IllegalArgumentException("totalOrders must be greater than 0");
        }
        if (errorSampleLimit < 0) {
            throw new IllegalArgumentException("errorSampleLimit must not be negative");
        }
    }

    public static A39SimulatorOptions parse(String[] args) {
        return parse(args, System.getenv(), Clock.systemDefaultZone());
    }

    static A39SimulatorOptions parse(String[] args, Map<String, String> env, Clock clock) {
        Map<String, String> options = parseArgs(args);
        A39SimulatorMode mode = A39SimulatorMode.parse(optionPreservingBlank(options, env, "mode", "A39_MODE", "scenario"));
        int callbackPort = integer(option(options, env, "callback-port", "A39_CALLBACK_PORT", String.valueOf(DEFAULT_CALLBACK_PORT)), "callback-port");
        String notifyFallback = mode == A39SimulatorMode.LOAD ? "" : "http://127.0.0.1:" + callbackPort + "/callback/order";
        String notifyUrl = mode == A39SimulatorMode.LOAD
                ? optionPreservingBlank(options, env, "notify-url", "A39_NOTIFY_URL", notifyFallback)
                : option(options, env, "notify-url", "A39_NOTIFY_URL", notifyFallback);
        return new A39SimulatorOptions(
                option(options, env, "base-url", "A39_BASE_URL", DEFAULT_BASE_URL),
                option(options, env, "app-id", "A39_APP_ID", DEFAULT_APP_ID),
                option(options, env, "app-secret", "A39_APP_SECRET", null),
                option(options, env, "aes-key", "A39_AES_KEY", null),
                option(options, env, "product-id", "A39_PRODUCT_ID", DEFAULT_PRODUCT_ID),
                option(options, env, "account", "A39_ACCOUNT", DEFAULT_ACCOUNT),
                option(options, env, "out-order-sn", "A39_OUT_ORDER_SN", nextOutOrderSn(clock)),
                option(options, env, "nonce-str", "A39_NONCE_STR", randomNonce()),
                option(options, env, "timestamp", "A39_TIMESTAMP", String.valueOf(clock.millis())),
                integer(option(options, env, "num", "A39_NUM", "1"), "num"),
                longValue(option(options, env, "order-price", "A39_ORDER_PRICE", "0"), "order-price"),
                integer(option(options, env, "product-type", "A39_PRODUCT_TYPE", "1"), "product-type"),
                notifyUrl,
                integer(option(options, env, "query-attempts", "A39_QUERY_ATTEMPTS", String.valueOf(DEFAULT_QUERY_ATTEMPTS)), "query-attempts"),
                duration(option(options, env, "query-interval", "A39_QUERY_INTERVAL", DEFAULT_QUERY_INTERVAL.toString()), "query-interval"),
                bool(option(options, env, "request-sm4", "A39_REQUEST_SM4", String.valueOf(DEFAULT_REQUEST_SM4_ENABLED))),
                option(options, env, "callback-bind-host", "A39_CALLBACK_BIND_HOST", "0.0.0.0"),
                callbackPort,
                duration(option(options, env, "callback-wait", "A39_CALLBACK_WAIT", DEFAULT_CALLBACK_WAIT.toString()), "callback-wait"),
                mode,
                integer(option(options, env, "concurrency", "A39_LOAD_CONCURRENCY", String.valueOf(DEFAULT_LOAD_CONCURRENCY)), "concurrency"),
                integer(option(options, env, "total-orders", "A39_LOAD_TOTAL_ORDERS", String.valueOf(DEFAULT_LOAD_TOTAL_ORDERS)), "total-orders"),
                duration(option(options, env, "request-timeout", "A39_REQUEST_TIMEOUT", DEFAULT_REQUEST_TIMEOUT.toString()), "request-timeout"),
                option(options, env, "out-order-prefix", "A39_OUT_ORDER_PREFIX", nextOutOrderSn(clock)),
                codes(optionPreservingBlank(options, env, "expected-create-codes", "A39_EXPECTED_CREATE_CODES", "200,510")),
                integer(option(options, env, "error-sample-limit", "A39_ERROR_SAMPLE_LIMIT", String.valueOf(DEFAULT_ERROR_SAMPLE_LIMIT)), "error-sample-limit"),
                duration(option(options, env, "progress-interval", "A39_LOAD_PROGRESS_INTERVAL", DEFAULT_PROGRESS_INTERVAL.toString()), "progress-interval")
        );
    }

    public static String usage() {
        return """
                A39 business simulator

                Required: --app-secret, --aes-key
                Defaults: --base-url http://apitest.kalaiwang.com, --app-id A35cecjXS7lZ1W30NMN, --product-id 1011, --account 18186222336, --num 1, --product-type 1, --request-sm4 true

                Environment fallbacks:
                  A39_APP_ID A39_APP_SECRET A39_AES_KEY A39_PRODUCT_ID A39_ACCOUNT A39_REQUEST_SM4

                Example:
                  mvn -f cardshop-trade-engine-simulator/pom.xml exec:java \\
                    -Dexec.args="--account 18186222336"
                """;
    }

    public String sanitizedSummary() {
        return "mode=%s, baseUrl=%s, appId=%s, productId=%s, account=%s, outOrderSn=%s, notifyUrl=%s, queryAttempts=%d, requestSm4=%s, concurrency=%d, totalOrders=%d, requestTimeout=%s"
                .formatted(mode, baseUrl, appId, productId, mask(account), outOrderSn, notifyUrl, queryAttempts,
                        requestSm4Enabled, concurrency, totalOrders, requestTimeout);
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

    private static String optionPreservingBlank(Map<String, String> options, Map<String, String> env, String key, String envKey, String fallback) {
        if (options.containsKey(key)) {
            return options.get(key) == null ? "" : options.get(key).trim();
        }
        if (env.containsKey(envKey)) {
            return env.get(envKey) == null ? "" : env.get(envKey).trim();
        }
        return fallback;
    }

    private static List<String> codes(String value) {
        List<String> values = new ArrayList<>();
        if (value != null) {
            for (String part : value.split(",")) {
                if (hasText(part)) {
                    values.add(part.trim());
                }
            }
        }
        return values;
    }

    private static List<String> normalizeCodes(List<String> values) {
        List<String> normalized = values == null ? List.of() : values.stream()
                .filter(A39SimulatorOptions::hasText)
                .map(String::trim)
                .distinct()
                .toList();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("expectedCreateCodes must not be empty");
        }
        return List.copyOf(normalized);
    }

    private static String nextOutOrderSn(Clock clock) {
        LocalDateTime now = LocalDateTime.now(clock);
        return "A39" + ORDER_TIME_FORMATTER.format(now) + String.format(Locale.ROOT, "%04d", RANDOM.nextInt(10_000));
    }

    static String randomNonce() {
        char[] chars = new char[32];
        for (int index = 0; index < chars.length; index++) {
            chars[index] = NONCE_CHARS[RANDOM.nextInt(NONCE_CHARS.length)];
        }
        return new String(chars);
    }

    private static Duration duration(String value, String name) {
        try {
            if ("0".equals(value)) {
                return Duration.ZERO;
            }
            if (value.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
            }
            if (value.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
            }
            if (value.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1)));
            }
            return Duration.parse(value);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException(name + " must be ISO-8601 duration or use ms/s/m suffix", ex);
        }
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

    private static boolean bool(String value) {
        return Boolean.parseBoolean(Objects.toString(value, "false"));
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

    private static String mask(String value) {
        if (!hasText(value) || value.length() <= 4) {
            return "****";
        }
        return value.substring(0, 3) + "****" + value.substring(value.length() - 2);
    }
}
