# A39 Simulator Load Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a fixed-concurrency `load` mode to `cardshop-trade-engine-simulator` for test188 A39 create-order performance testing, and reorganize README simulator docs into one standalone A39 section.

**Architecture:** Keep the current `scenario` mode intact and introduce a separate load-test path. Extend `A39SimulatorOptions` with explicit mode and load configuration, add a focused `A39LoadTestRunner` that reuses `V1Sm3Signer` and `V1CardApiClient`, and make `HttpV1CardApiClient` support configurable timeout plus optional per-request logging.

**Tech Stack:** Java 21, Maven, JDK `HttpClient`, JDK virtual threads / executor APIs, Jackson, BouncyCastle SM3, JUnit 5, AssertJ.

---

## File Structure

- Modify `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39SimulatorOptions.java`: add `mode`, load parameters, request timeout, explicit empty-value handling, and startup summaries.
- Modify `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39BusinessSimulatorApplication.java`: branch between `scenario` and `load` modes; do not start callback server in `load` mode; return non-zero exit code on load-test failures.
- Modify `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/HttpV1CardApiClient.java`: accept configurable request timeout and a flag to suppress per-request curl/response logs.
- Create `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39SimulatorMode.java`: enum for `SCENARIO` and `LOAD`.
- Create `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39LoadTestRunner.java`: fixed-concurrency create-order runner, metrics collection, and text reporting.
- Create `cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/A39LoadTestRunnerTest.java`: unit tests for concurrency, unique request fields, success classification, metrics, and error samples.
- Modify `cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/A39SimulatorOptionsTest.java`: tests for new parser defaults, env/CLI precedence, explicit blank `notify-url`, and invalid load parameters.
- Modify `cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/HttpV1CardApiClientTest.java`: tests for configurable timeout and disabled request logging.
- Modify `deploy/config/a39-simulator.env.example`: document optional load-test environment variables without real secrets.
- Modify `README.md`: consolidate all A39 simulator content into one standalone `## A39 业务模拟器` chapter with `scenario` and `load` subsections.

### Task 1: Simulator Mode and Load Options

**Files:**
- Create: `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39SimulatorMode.java`
- Modify: `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39SimulatorOptions.java`
- Modify: `cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/A39SimulatorOptionsTest.java`

- [ ] **Step 1: Write failing option parser tests**

Add these tests to `A39SimulatorOptionsTest`:

```java
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
    void invalidLoadParametersAreRejected() {
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
    }
```

Also add imports at the top of the test file:

```java
import java.time.Duration;
import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
```

- [ ] **Step 2: Run option tests red**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH \
mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=A39SimulatorOptionsTest
```

Expected: compilation fails because `A39SimulatorMode`, `mode()`, `concurrency()`, `totalOrders()`, `requestTimeout()`, `outOrderPrefix()`, `expectedCreateCodes()`, `errorSampleLimit()`, and `progressInterval()` do not exist.

- [ ] **Step 3: Add mode enum**

Create `A39SimulatorMode.java`:

```java
package com.cardshop.trade.engine.simulator.a39;

import java.util.Locale;

/**
 * A39 模拟器运行模式。
 *
 * <p>参见 {@code docs/superpowers/specs/2026-07-15-a39-simulator-load-test-design.md}：
 * {@link #SCENARIO} 保持单笔联调语义，{@link #LOAD} 只压创建订单入口。
 */
public enum A39SimulatorMode {

    SCENARIO,
    LOAD;

    static A39SimulatorMode parse(String value) {
        String normalized = value == null || value.isBlank() ? "scenario" : value.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "scenario" -> SCENARIO;
            case "load" -> LOAD;
            default -> throw new IllegalArgumentException("mode must be scenario or load");
        };
    }
}
```

- [ ] **Step 4: Extend `A39SimulatorOptions` minimally**

Modify the record header in `A39SimulatorOptions.java` to append these fields after `callbackWait`:

```java
        Duration callbackWait,
        A39SimulatorMode mode,
        int concurrency,
        int totalOrders,
        Duration requestTimeout,
        String outOrderPrefix,
        List<String> expectedCreateCodes,
        int errorSampleLimit,
        Duration progressInterval) {
```

Add imports:

```java
import java.util.ArrayList;
import java.util.List;
```

Add constants near existing defaults:

```java
    private static final int DEFAULT_LOAD_CONCURRENCY = 10;
    private static final int DEFAULT_LOAD_TOTAL_ORDERS = 200;
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final int DEFAULT_ERROR_SAMPLE_LIMIT = 20;
    private static final Duration DEFAULT_PROGRESS_INTERVAL = Duration.ofSeconds(10);
```

In the compact constructor, add these normalizations and validations after `callbackWait` normalization:

```java
        mode = mode == null ? A39SimulatorMode.SCENARIO : mode;
        requestTimeout = requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative()
                ? DEFAULT_REQUEST_TIMEOUT
                : requestTimeout;
        outOrderPrefix = hasText(outOrderPrefix) ? outOrderPrefix.trim() : nextOutOrderSn(Clock.systemDefaultZone());
        expectedCreateCodes = normalizeCodes(expectedCreateCodes);
        progressInterval = progressInterval == null || progressInterval.isNegative() ? DEFAULT_PROGRESS_INTERVAL : progressInterval;
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be greater than 0");
        }
        if (totalOrders <= 0) {
            throw new IllegalArgumentException("totalOrders must be greater than 0");
        }
        if (errorSampleLimit < 0) {
            throw new IllegalArgumentException("errorSampleLimit must not be negative");
        }
        if (mode == A39SimulatorMode.LOAD && hasText(notifyUrl)) {
            throw new IllegalArgumentException("notifyUrl must be blank in load mode");
        }
```

Replace the current notify URL parsing in `parse` with explicit mode-aware parsing:

```java
        A39SimulatorMode mode = A39SimulatorMode.parse(option(options, env, "mode", "A39_MODE", "scenario"));
        int callbackPort = integer(option(options, env, "callback-port", "A39_CALLBACK_PORT", String.valueOf(DEFAULT_CALLBACK_PORT)), "callback-port");
        String notifyFallback = mode == A39SimulatorMode.LOAD ? "" : "http://127.0.0.1:" + callbackPort + "/callback/order";
        String notifyUrl = optionPreservingBlank(options, env, "notify-url", "A39_NOTIFY_URL", notifyFallback);
```

Append these constructor arguments after `callbackWait`:

```java
                mode,
                integer(option(options, env, "concurrency", "A39_LOAD_CONCURRENCY", String.valueOf(DEFAULT_LOAD_CONCURRENCY)), "concurrency"),
                integer(option(options, env, "total-orders", "A39_LOAD_TOTAL_ORDERS", String.valueOf(DEFAULT_LOAD_TOTAL_ORDERS)), "total-orders"),
                duration(option(options, env, "request-timeout", "A39_REQUEST_TIMEOUT", DEFAULT_REQUEST_TIMEOUT.toString()), "request-timeout"),
                option(options, env, "out-order-prefix", "A39_OUT_ORDER_PREFIX", nextOutOrderSn(clock)),
                codes(optionPreservingBlank(options, env, "expected-create-codes", "A39_EXPECTED_CREATE_CODES", "200,510")),
                integer(option(options, env, "error-sample-limit", "A39_ERROR_SAMPLE_LIMIT", String.valueOf(DEFAULT_ERROR_SAMPLE_LIMIT)), "error-sample-limit"),
                duration(option(options, env, "progress-interval", "A39_LOAD_PROGRESS_INTERVAL", DEFAULT_PROGRESS_INTERVAL.toString()), "progress-interval")
```

Add helper methods:

```java
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
```

Update `sanitizedSummary()` to include the new fields:

```java
        return "mode=%s, baseUrl=%s, appId=%s, productId=%s, account=%s, outOrderSn=%s, notifyUrl=%s, queryAttempts=%d, requestSm4=%s, concurrency=%d, totalOrders=%d, requestTimeout=%s"
                .formatted(mode, baseUrl, appId, productId, mask(account), outOrderSn, notifyUrl, queryAttempts,
                        requestSm4Enabled, concurrency, totalOrders, requestTimeout);
```

- [ ] **Step 5: Update current test object construction**

In `A39BusinessScenarioRunnerTest.options`, append the new constructor values after `Duration.ZERO` callback wait:

```java
                A39SimulatorMode.SCENARIO,
                10,
                200,
                Duration.ofSeconds(10),
                "A39TEST",
                List.of("200", "510"),
                20,
                Duration.ZERO
```

Add import:

```java
import java.util.List;
```

- [ ] **Step 6: Run option tests green**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH \
mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=A39SimulatorOptionsTest,A39BusinessScenarioRunnerTest
```

Expected: both test classes pass.

- [ ] **Step 7: Commit Task 1**

```bash
git add cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39SimulatorMode.java \
  cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39SimulatorOptions.java \
  cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/A39SimulatorOptionsTest.java \
  cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/A39BusinessScenarioRunnerTest.java
git commit -m "feat(simulator): add load mode options"
```

### Task 2: Configurable HTTP Timeout and Quiet Logging

**Files:**
- Modify: `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/HttpV1CardApiClient.java`
- Modify: `cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/HttpV1CardApiClientTest.java`

- [ ] **Step 1: Write failing HTTP client tests**

Add tests to `HttpV1CardApiClientTest` using a package-private fake `HttpClient` that captures the built request and then throws before any network I/O:

```java
    @Test
    void appliesConfiguredRequestTimeoutToPostRequests() throws Exception {
        CapturingTimeoutHttpClient httpClient = new CapturingTimeoutHttpClient();
        HttpV1CardApiClient client = new HttpV1CardApiClient(
                URI.create("http://api.example"),
                httpClient,
                new ObjectMapper(),
                new LegacySm4BodyEncoder(),
                ignored -> {},
                Duration.ofSeconds(3),
                true);

        try {
            client.getToken("app-id", "secret-value");
        } catch (IOException expected) {
            assertThat(expected).hasMessage("stop before network");
        }

        assertThat(httpClient.lastRequest.timeout()).contains(Duration.ofSeconds(3));
    }

    @Test
    void suppressesPerRequestLogsWhenDisabled() throws Exception {
        CapturingTimeoutHttpClient httpClient = new CapturingTimeoutHttpClient();
        List<String> logs = new ArrayList<>();
        HttpV1CardApiClient client = new HttpV1CardApiClient(
                URI.create("http://api.example"),
                httpClient,
                new ObjectMapper(),
                new LegacySm4BodyEncoder(),
                logs::add,
                Duration.ofSeconds(3),
                false);

        try {
            client.getToken("app-id", "secret-value");
        } catch (IOException expected) {
            assertThat(expected).hasMessage("stop before network");
        }

        assertThat(logs).isEmpty();
    }
```

Add imports:

```java
import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.time.Duration;
```

Add this fake client class inside `HttpV1CardApiClientTest`:

```java
    private static final class CapturingTimeoutHttpClient extends HttpClient {

        private HttpRequest lastRequest;

        @Override
        public Optional<CookieHandler> cookieHandler() {
            return Optional.empty();
        }

        @Override
        public Optional<Duration> connectTimeout() {
            return Optional.empty();
        }

        @Override
        public Redirect followRedirects() {
            return Redirect.NEVER;
        }

        @Override
        public Optional<ProxySelector> proxy() {
            return Optional.empty();
        }

        @Override
        public javax.net.ssl.SSLContext sslContext() {
            return null;
        }

        @Override
        public javax.net.ssl.SSLParameters sslParameters() {
            return null;
        }

        @Override
        public Optional<Authenticator> authenticator() {
            return Optional.empty();
        }

        @Override
        public Version version() {
            return Version.HTTP_1_1;
        }

        @Override
        public Optional<Executor> executor() {
            return Optional.empty();
        }

        @Override
        public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) throws IOException {
            this.lastRequest = request;
            throw new IOException("stop before network");
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
            throw new UnsupportedOperationException("sendAsync is not used by the simulator");
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request,
                HttpResponse.BodyHandler<T> responseBodyHandler,
                HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
            throw new UnsupportedOperationException("sendAsync is not used by the simulator");
        }
    }
```

- [ ] **Step 2: Run HTTP client tests red**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH \
mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=HttpV1CardApiClientTest
```

Expected: compilation fails because the new constructor signature does not exist.

- [ ] **Step 3: Add timeout and log switch**

In `HttpV1CardApiClient`, rename the hard-coded `REQUEST_TIMEOUT` constant to `DEFAULT_REQUEST_TIMEOUT`, then add fields:

```java
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private final Duration requestTimeout;
    private final boolean logHttpTraffic;
```

Update constructors:

```java
    public HttpV1CardApiClient(String baseUrl) {
        this(URI.create(baseUrl), HttpClient.newHttpClient(), new ObjectMapper(), new LegacySm4BodyEncoder(), System.out::println,
                DEFAULT_REQUEST_TIMEOUT, true);
    }

    public HttpV1CardApiClient(String baseUrl, Duration requestTimeout, boolean logHttpTraffic) {
        this(URI.create(baseUrl), HttpClient.newHttpClient(), new ObjectMapper(), new LegacySm4BodyEncoder(), System.out::println,
                requestTimeout, logHttpTraffic);
    }

    HttpV1CardApiClient(URI baseUri, HttpClient httpClient, ObjectMapper objectMapper, LegacySm4BodyEncoder sm4BodyEncoder) {
        this(baseUri, httpClient, objectMapper, sm4BodyEncoder, ignored -> {
        }, DEFAULT_REQUEST_TIMEOUT, true);
    }

    HttpV1CardApiClient(
            URI baseUri,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            LegacySm4BodyEncoder sm4BodyEncoder,
            Consumer<String> logSink) {
        this(baseUri, httpClient, objectMapper, sm4BodyEncoder, logSink, DEFAULT_REQUEST_TIMEOUT, true);
    }

    HttpV1CardApiClient(
            URI baseUri,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            LegacySm4BodyEncoder sm4BodyEncoder,
            Consumer<String> logSink,
            Duration requestTimeout,
            boolean logHttpTraffic) {
        this.baseUri = Objects.requireNonNull(baseUri, "baseUri must not be null");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.sm4BodyEncoder = Objects.requireNonNull(sm4BodyEncoder, "sm4BodyEncoder must not be null");
        this.logSink = Objects.requireNonNull(logSink, "logSink must not be null");
        this.requestTimeout = requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative()
                ? DEFAULT_REQUEST_TIMEOUT
                : requestTimeout;
        this.logHttpTraffic = logHttpTraffic;
    }
```

In `post`, replace `.timeout(REQUEST_TIMEOUT)` with:

```java
                .timeout(requestTimeout)
```

Guard request and response logs:

```java
        if (logHttpTraffic) {
            logRequestCurl(path, requestUri, token, contentType, sm4Enabled ? body : objectMapper.writeValueAsString(sanitizedPayload(payload)));
        }
        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (logHttpTraffic) {
            logSink.accept("[A39][HTTP][response] path=%s status=%d body=%s".formatted(path, response.statusCode(), response.body()));
        }
```

- [ ] **Step 4: Run HTTP client tests green**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH \
mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=HttpV1CardApiClientTest
```

Expected: tests pass.

- [ ] **Step 5: Commit Task 2**

```bash
git add cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/HttpV1CardApiClient.java \
  cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/HttpV1CardApiClientTest.java
git commit -m "feat(simulator): support quiet HTTP load requests"
```

### Task 3: Load Test Runner Metrics and Reporting

**Files:**
- Create: `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39LoadTestRunner.java`
- Create: `cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/A39LoadTestRunnerTest.java`

- [ ] **Step 1: Write failing load runner tests**

Create `A39LoadTestRunnerTest.java`:

```java
package com.cardshop.trade.engine.simulator.a39;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class A39LoadTestRunnerTest {

    @Test
    void sendsConfiguredNumberOfCreateRequestsWithUniqueOrderAndNonce() throws Exception {
        CapturingLoadClient client = new CapturingLoadClient(List.of(
                response("200"), response("510"), response("200"), response("510"), response("200")));
        A39SimulatorOptions options = options(2, 5, List.of("200", "510"), 20);

        A39LoadTestRunner.LoadTestResult result = new A39LoadTestRunner(client, new V1Sm3Signer(), Clock.systemUTC(), ignored -> {})
                .run(options);

        assertThat(result.successful()).isEqualTo(5);
        assertThat(result.failed()).isZero();
        assertThat(client.createPayloads).hasSize(5);
        assertThat(client.maxInFlight.get()).isLessThanOrEqualTo(2);
        assertThat(client.createPayloads)
                .extracting(payload -> String.valueOf(payload.get("out_order_sn")))
                .doesNotHaveDuplicates();
        assertThat(client.createPayloads)
                .extracting(payload -> String.valueOf(payload.get("nonce_str")))
                .doesNotHaveDuplicates();
        assertThat(client.createPayloads)
                .allSatisfy(payload -> assertThat(payload.get("notify_url")).isEqualTo(""));
    }

    @Test
    void classifiesUnexpectedCodesAndExceptionsAsFailuresWithLimitedSamples() throws Exception {
        CapturingLoadClient client = new CapturingLoadClient(List.of(
                response("200"),
                response("501"),
                failure(new IllegalStateException("HTTP request failed: path=/v1/cardapi/order/createRechargeOrder, status=503")),
                response("510")));
        A39SimulatorOptions options = options(2, 4, List.of("200", "510"), 1);

        A39LoadTestRunner.LoadTestResult result = new A39LoadTestRunner(client, new V1Sm3Signer(), Clock.systemUTC(), ignored -> {})
                .run(options);

        assertThat(result.completed()).isEqualTo(4);
        assertThat(result.successful()).isEqualTo(2);
        assertThat(result.failed()).isEqualTo(2);
        assertThat(result.businessCodeCounts()).containsEntry("200", 1).containsEntry("510", 1).containsEntry("501", 1);
        assertThat(result.errorCounts()).containsEntry("IllegalStateException", 1);
        assertThat(result.errorSamples()).hasSize(1);
        assertThat(result.exitCode()).isEqualTo(1);
    }

    @Test
    void reportContainsStableSummaryFields() throws Exception {
        CapturingLoadClient client = new CapturingLoadClient(List.of(response("200"), response("510")));
        List<String> logs = new ArrayList<>();
        A39SimulatorOptions options = options(1, 2, List.of("200", "510"), 20);

        A39LoadTestRunner.LoadTestResult result = new A39LoadTestRunner(
                client,
                new V1Sm3Signer(),
                Clock.fixed(Instant.parse("2026-07-15T01:00:00Z"), ZoneId.of("UTC")),
                logs::add)
                .run(options);

        String report = result.toReport();

        assertThat(report)
                .contains("mode=LOAD")
                .contains("concurrency=1")
                .contains("totalOrders=2")
                .contains("successful=2")
                .contains("failed=0")
                .contains("code.200=1")
                .contains("code.510=1")
                .contains("p50Ms=")
                .contains("p95Ms=")
                .contains("p99Ms=");
        assertThat(logs).isEmpty();
    }

    @Test
    void progressLogsStayDisabledWhenProgressIntervalIsZero() throws Exception {
        CapturingLoadClient client = new CapturingLoadClient(List.of(response("200"), response("200")));
        List<String> logs = new ArrayList<>();
        A39SimulatorOptions options = options(1, 2, List.of("200", "510"), 20);

        new A39LoadTestRunner(client, new V1Sm3Signer(), Clock.systemUTC(), logs::add).run(options);

        assertThat(logs).isEmpty();
    }

    private static A39SimulatorOptions options(int concurrency, int totalOrders, List<String> expectedCodes, int errorSampleLimit) {
        return new A39SimulatorOptions(
                "http://apitest.kalaiwang.com",
                "app-id",
                "app-secret",
                "AES-KEY",
                "73301280",
                "18186222336",
                "IGNORED-SCENARIO-OUT-ORDER",
                "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR",
                "1783224958",
                1,
                5200L,
                1,
                "",
                0,
                Duration.ZERO,
                false,
                "127.0.0.1",
                19039,
                Duration.ZERO,
                A39SimulatorMode.LOAD,
                concurrency,
                totalOrders,
                Duration.ofSeconds(10),
                "PERF-A39",
                expectedCodes,
                errorSampleLimit,
                Duration.ZERO);
    }

    private static V1CardApiClient.ApiResponse response(String code) {
        return new V1CardApiClient.ApiResponse(code, "msg-" + code, Map.of());
    }

    private static RuntimeException failure(RuntimeException ex) {
        return ex;
    }

    private static final class CapturingLoadClient implements V1CardApiClient {

        private final List<Object> outcomes;
        private final AtomicInteger next = new AtomicInteger();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger maxInFlight = new AtomicInteger();
        private final List<Map<String, Object>> createPayloads = java.util.Collections.synchronizedList(new ArrayList<>());

        private CapturingLoadClient(List<?> outcomes) {
            this.outcomes = List.copyOf(outcomes);
        }

        @Override
        public String getToken(String appId, String appSecret) {
            return "token";
        }

        @Override
        public V1CardApiClient.ApiResponse createRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            int current = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(current, Math::max);
            try {
                createPayloads.add(new ConcurrentHashMap<>(payload));
                Object outcome = outcomes.get(next.getAndIncrement());
                if (outcome instanceof RuntimeException ex) {
                    throw ex;
                }
                return (V1CardApiClient.ApiResponse) outcome;
            } finally {
                inFlight.decrementAndGet();
            }
        }

        @Override
        public V1CardApiClient.ApiResponse getRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            throw new AssertionError("load mode must not query order");
        }
    }
}
```

- [ ] **Step 2: Run load runner tests red**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH \
mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=A39LoadTestRunnerTest
```

Expected: compilation fails because `A39LoadTestRunner` does not exist.

- [ ] **Step 3: Implement `A39LoadTestRunner`**

Create `A39LoadTestRunner.java` with this structure:

```java
package com.cardshop.trade.engine.simulator.a39;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * A39 下单入口压测编排器。
 *
 * <p>参见 {@code docs/superpowers/specs/2026-07-15-a39-simulator-load-test-design.md}：
 * load 模式只衡量创建订单入口，不查单、不启动回调接收器，并固定使用空 {@code notify_url}。
 */
public final class A39LoadTestRunner {

    private final V1CardApiClient client;
    private final V1Sm3Signer signer;
    private final Clock clock;
    private final Consumer<String> logSink;

    public A39LoadTestRunner(V1CardApiClient client, V1Sm3Signer signer, Clock clock, Consumer<String> logSink) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.signer = Objects.requireNonNull(signer, "signer must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.logSink = Objects.requireNonNull(logSink, "logSink must not be null");
    }

    public LoadTestResult run(A39SimulatorOptions options) throws Exception {
        Objects.requireNonNull(options, "options must not be null");
        Instant tokenStart = Instant.now(clock);
        long tokenStartNanos = System.nanoTime();
        String token = client.getToken(options.appId(), options.appSecret());
        long tokenElapsedNanos = System.nanoTime() - tokenStartNanos;
        long windowStartNanos = System.nanoTime();
        long nextProgressNanos = nextProgressNanos(options, windowStartNanos);
        List<RequestResult> results = Collections.synchronizedList(new ArrayList<>());
        Set<String> expectedCodes = Set.copyOf(options.expectedCreateCodes());

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<RequestResult>> futures = new ArrayList<>();
            for (int next = 0; next < options.totalOrders(); next++) {
                while (futures.size() >= options.concurrency()) {
                    nextProgressNanos = collectFirst(futures, results, options, windowStartNanos, nextProgressNanos);
                }
                int sequence = next + 1;
                futures.add(executor.submit(requestTask(options, token, expectedCodes, sequence)));
            }
            while (!futures.isEmpty()) {
                nextProgressNanos = collectFirst(futures, results, options, windowStartNanos, nextProgressNanos);
            }
        }

        long elapsedNanos = System.nanoTime() - windowStartNanos;
        return LoadTestResult.from(options, tokenStart, Duration.ofNanos(tokenElapsedNanos), Duration.ofNanos(elapsedNanos), List.copyOf(results));
    }

    private Callable<RequestResult> requestTask(A39SimulatorOptions options, String token, Set<String> expectedCodes, int sequence) {
        return () -> {
            String outOrderSn = "%s-%06d".formatted(options.outOrderPrefix(), sequence);
            Map<String, Object> payload = createPayload(options, outOrderSn);
            long startedNanos = System.nanoTime();
            try {
                V1CardApiClient.ApiResponse response = client.createRechargeOrder(
                        token,
                        payload,
                        options.requestSm4Enabled(),
                        options.aesKey());
                long elapsedNanos = System.nanoTime() - startedNanos;
                boolean success = expectedCodes.contains(response.code());
                return RequestResult.business(outOrderSn, response.code(), response.msg(), success, elapsedNanos);
            } catch (Exception ex) {
                long elapsedNanos = System.nanoTime() - startedNanos;
                return RequestResult.error(outOrderSn, ex, elapsedNanos);
            }
        };
    }

    private Map<String, Object> createPayload(A39SimulatorOptions options, String outOrderSn) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("product_id", options.productId());
        payload.put("out_order_sn", outOrderSn);
        payload.put("account", options.account());
        payload.put("timestamp", String.valueOf(clock.millis()));
        payload.put("nonce_str", A39SimulatorOptions.randomNonce());
        payload.put("notify_url", "");
        payload.put("num", options.num());
        payload.put("order_price", options.orderPrice());
        payload.put("product_type", options.productType());
        payload.put("sign", signer.signCreateOrder(payload, options.aesKey()).sign());
        return payload;
    }

    private long collectFirst(
            List<Future<RequestResult>> futures,
            List<RequestResult> results,
            A39SimulatorOptions options,
            long windowStartNanos,
            long nextProgressNanos) throws Exception {
        Future<RequestResult> first = futures.remove(0);
        results.add(first.get());
        return logProgressIfDue(results, options, windowStartNanos, nextProgressNanos);
    }

    private long logProgressIfDue(
            List<RequestResult> results,
            A39SimulatorOptions options,
            long windowStartNanos,
            long nextProgressNanos) {
        if (nextProgressNanos == Long.MAX_VALUE) {
            return nextProgressNanos;
        }
        long nowNanos = System.nanoTime();
        if (nowNanos < nextProgressNanos) {
            return nextProgressNanos;
        }
        long completed = results.size();
        long successful = results.stream().filter(RequestResult::success).count();
        long failed = completed - successful;
        long elapsedNanos = Math.max(1L, nowNanos - windowStartNanos);
        logSink.accept("[A39][LOAD][progress] completed=%d successful=%d failed=%d elapsedMs=%d qps=%.2f".formatted(
                completed,
                successful,
                failed,
                elapsedNanos / 1_000_000L,
                completed * 1_000_000_000.0 / elapsedNanos));
        return nowNanos + options.progressInterval().toNanos();
    }

    private static long nextProgressNanos(A39SimulatorOptions options, long windowStartNanos) {
        return options.progressInterval().isZero() ? Long.MAX_VALUE : windowStartNanos + options.progressInterval().toNanos();
    }

    private record RequestResult(String outOrderSn, String code, String message, boolean success, String errorType, long elapsedNanos) {
        static RequestResult business(String outOrderSn, String code, String message, boolean success, long elapsedNanos) {
            return new RequestResult(outOrderSn, code, message, success, "", elapsedNanos);
        }

        static RequestResult error(String outOrderSn, Exception ex, long elapsedNanos) {
            return new RequestResult(outOrderSn, "", ex.getMessage(), false, ex.getClass().getSimpleName(), elapsedNanos);
        }
    }

    public record LoadTestResult(
            A39SimulatorMode mode,
            String baseUrl,
            String runId,
            int concurrency,
            int totalOrders,
            Duration requestTimeout,
            Instant tokenStart,
            Duration tokenElapsed,
            Duration elapsed,
            int completed,
            int successful,
            int failed,
            Map<String, Integer> businessCodeCounts,
            Map<String, Integer> errorCounts,
            List<String> errorSamples,
            List<Long> latenciesNanos) {

        static LoadTestResult from(
                A39SimulatorOptions options,
                Instant tokenStart,
                Duration tokenElapsed,
                Duration elapsed,
                List<RequestResult> results) {
            Map<String, Integer> codeCounts = new TreeMap<>();
            Map<String, Integer> errorCounts = new TreeMap<>();
            List<String> samples = new ArrayList<>();
            List<Long> latencies = new ArrayList<>();
            int successful = 0;
            for (RequestResult result : results) {
                latencies.add(result.elapsedNanos());
                if (result.success()) {
                    successful++;
                }
                if (!result.code().isBlank()) {
                    codeCounts.merge(result.code(), 1, Integer::sum);
                }
                if (!result.errorType().isBlank()) {
                    errorCounts.merge(result.errorType(), 1, Integer::sum);
                }
                if (!result.success() && samples.size() < options.errorSampleLimit()) {
                    samples.add("outOrderSn=%s code=%s errorType=%s msg=%s".formatted(
                            result.outOrderSn(), result.code(), result.errorType(), sanitizeMessage(result.message())));
                }
            }
            return new LoadTestResult(
                    A39SimulatorMode.LOAD,
                    options.baseUrl(),
                    options.outOrderPrefix(),
                    options.concurrency(),
                    options.totalOrders(),
                    options.requestTimeout(),
                    tokenStart,
                    tokenElapsed,
                    elapsed,
                    results.size(),
                    successful,
                    results.size() - successful,
                    Map.copyOf(codeCounts),
                    Map.copyOf(errorCounts),
                    List.copyOf(samples),
                    List.copyOf(latencies));
        }

        public int exitCode() {
            return failed == 0 && completed == totalOrders ? 0 : 1;
        }

        public String toReport() {
            StringBuilder report = new StringBuilder(512);
            report.append("A39 load test summary\n");
            append(report, "mode", mode);
            append(report, "baseUrl", baseUrl);
            append(report, "runId", runId);
            append(report, "concurrency", concurrency);
            append(report, "totalOrders", totalOrders);
            append(report, "requestTimeout", requestTimeout);
            append(report, "tokenElapsedMs", tokenElapsed.toMillis());
            append(report, "elapsedMs", elapsed.toMillis());
            append(report, "completed", completed);
            append(report, "successful", successful);
            append(report, "failed", failed);
            append(report, "successRate", completed == 0 ? "0.00%" : "%.2f%%".formatted(successful * 100.0 / completed));
            append(report, "qps", elapsed.toNanos() == 0 ? "0.00" : "%.2f".formatted(completed * 1_000_000_000.0 / elapsed.toNanos()));
            append(report, "p50Ms", percentileMillis(0.50));
            append(report, "p95Ms", percentileMillis(0.95));
            append(report, "p99Ms", percentileMillis(0.99));
            append(report, "minMs", latenciesNanos.stream().mapToLong(Long::longValue).min().orElse(0L) / 1_000_000L);
            append(report, "maxMs", latenciesNanos.stream().mapToLong(Long::longValue).max().orElse(0L) / 1_000_000L);
            businessCodeCounts.forEach((code, count) -> append(report, "code." + code, count));
            errorCounts.forEach((type, count) -> append(report, "error." + type, count));
            for (int index = 0; index < errorSamples.size(); index++) {
                append(report, "sample." + (index + 1), errorSamples.get(index));
            }
            append(report, "exitCode", exitCode());
            return report.toString();
        }

        private long percentileMillis(double percentile) {
            if (latenciesNanos.isEmpty()) {
                return 0L;
            }
            List<Long> sorted = new ArrayList<>(latenciesNanos);
            Collections.sort(sorted);
            int index = (int) Math.ceil(percentile * sorted.size()) - 1;
            int boundedIndex = Math.max(0, Math.min(index, sorted.size() - 1));
            return sorted.get(boundedIndex) / 1_000_000L;
        }

        private static void append(StringBuilder builder, String key, Object value) {
            builder.append(key).append('=').append(value).append('\n');
        }

        private static String sanitizeMessage(String message) {
            if (message == null || message.isBlank()) {
                return "";
            }
            return message.replace('\n', ' ').replace('\r', ' ');
        }
    }
}
```

- [ ] **Step 4: Run load runner tests green**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH \
mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=A39LoadTestRunnerTest
```

Expected: tests pass.

- [ ] **Step 5: Commit Task 3**

```bash
git add cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39LoadTestRunner.java \
  cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/A39LoadTestRunnerTest.java
git commit -m "feat(simulator): add A39 load test runner"
```

### Task 4: CLI Mode Branching and Exit Codes

**Files:**
- Modify: `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39BusinessSimulatorApplication.java`
- Create: `cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/A39BusinessSimulatorApplicationTest.java`

- [ ] **Step 1: Write CLI branching tests**

Create `A39BusinessSimulatorApplicationTest.java`. The test intentionally calls a package-private overload that does not exist yet, so the first run fails at compilation:

```java
package com.cardshop.trade.engine.simulator.a39;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class A39BusinessSimulatorApplicationTest {

    @Test
    void loadModeReturnsRunnerExitCodeAndSkipsCallbackServerText() throws Exception {
        List<String> logs = new ArrayList<>();
        int exitCode = A39BusinessSimulatorApplication.run(
                loadOptions(),
                new SingleCreateClient(new V1CardApiClient.ApiResponse("200", "ok", Map.of())),
                logs::add);

        assertThat(exitCode).isZero();
        assertThat(String.join("\n", logs))
                .contains("Load mode skips callback server")
                .contains("A39 load test summary")
                .doesNotContain("Callback server listening");
    }

    private static A39SimulatorOptions loadOptions() {
        return new A39SimulatorOptions(
                "http://apitest.kalaiwang.com", "app-id", "app-secret", "AES-KEY", "73301280", "18186222336",
                "IGNORED", "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR", "1783224958", 1, 5200L, 1, "",
                0, Duration.ZERO, false, "127.0.0.1", 19039, Duration.ZERO,
                A39SimulatorMode.LOAD, 1, 1, Duration.ofSeconds(10), "PERF-A39", List.of("200", "510"), 20, Duration.ZERO);
    }

    private static final class SingleCreateClient implements V1CardApiClient {
        private final ApiResponse response;

        private SingleCreateClient(ApiResponse response) {
            this.response = response;
        }

        @Override
        public String getToken(String appId, String appSecret) {
            return "token";
        }

        @Override
        public ApiResponse createRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            return response;
        }

        @Override
        public ApiResponse getRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            throw new AssertionError("load mode must not query order");
        }
    }
}
```

- [ ] **Step 2: Run app tests red**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH \
mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=A39BusinessSimulatorApplicationTest
```

Expected: compilation fails because `A39BusinessSimulatorApplication.run(A39SimulatorOptions,V1CardApiClient,Consumer<String>)` does not exist.

- [ ] **Step 3: Extract application runner and add load branch**

Modify `A39BusinessSimulatorApplication` so `main` delegates to package-private methods and `load` mode can be tested with an injected client:

```java
    public static void main(String[] args) throws Exception {
        int exitCode = run(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args) throws Exception {
        if (hasHelp(args)) {
            System.out.println(A39SimulatorOptions.usage());
            return 0;
        }
        return run(A39SimulatorOptions.parse(args), System.out::println);
    }

    static int run(A39SimulatorOptions options, java.util.function.Consumer<String> logSink) throws Exception {
        V1CardApiClient client = new HttpV1CardApiClient(
                options.baseUrl(),
                options.requestTimeout(),
                options.mode() == A39SimulatorMode.SCENARIO);
        return run(options, client, logSink);
    }

    static int run(A39SimulatorOptions options, V1CardApiClient client, java.util.function.Consumer<String> logSink) throws Exception {
        V1Sm3Signer signer = new V1Sm3Signer();
        if (options.mode() == A39SimulatorMode.LOAD) {
            logSink.accept("A39 simulator starting: " + options.sanitizedSummary());
            logSink.accept("Load mode skips callback server, query loop, and callback wait.");
            A39LoadTestRunner.LoadTestResult result = new A39LoadTestRunner(
                    client,
                    signer,
                    java.time.Clock.systemDefaultZone(),
                    logSink)
                    .run(options);
            logSink.accept(result.toReport());
            return result.exitCode();
        }
        return runScenario(options, signer, logSink);
    }

    private static int runScenario(A39SimulatorOptions options, V1Sm3Signer signer, java.util.function.Consumer<String> logSink) throws Exception {
        try (A39CallbackServer callbackServer = A39CallbackServer.start(
                options.callbackBindHost(),
                options.callbackPort(),
                options.aesKey(),
                signer)) {
            logSink.accept("A39 simulator starting: " + options.sanitizedSummary());
            logSink.accept("Callback server listening: " + callbackServer.callbackUrl("/callback/order"));
            if (options.notifyUrl().contains("127.0.0.1") || options.notifyUrl().contains("localhost")) {
                logSink.accept("Note: local notify_url only works when the tested platform can reach this host.");
            }

            A39BusinessScenarioRunner runner = new A39BusinessScenarioRunner(
                    new HttpV1CardApiClient(options.baseUrl(), options.requestTimeout(), true),
                    signer,
                    null,
                    logSink);
            A39BusinessScenarioRunner.ScenarioResult result = runner.run(options);
            logSink.accept("Create response: code=%s, msg=%s".formatted(result.createResponse().code(), result.createResponse().msg()));
            for (int index = 0; index < result.queryResponses().size(); index++) {
                V1CardApiClient.ApiResponse response = result.queryResponses().get(index);
                logSink.accept("Query response #%d: code=%s, msg=%s, data=%s".formatted(
                        index + 1,
                        response.code(),
                        response.msg(),
                        response.data()));
            }
            waitForCallbacks(options.callbackWait(), logSink);
            if (!callbackServer.receivedCallbacks().isEmpty()) {
                callbackServer.receivedCallbacks().forEach(callback -> logSink.accept(
                        "Callback received: path=%s, signatureValid=%s, body=%s".formatted(
                                callback.path(),
                                callback.signatureValid(),
                                callback.rawBody())));
            }
            return 0;
        }
    }
```

Replace `waitForCallbacks(Duration duration)` with:

```java
    private static void waitForCallbacks(Duration duration, java.util.function.Consumer<String> logSink) throws InterruptedException {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            return;
        }
        logSink.accept("Waiting for callbacks for " + duration + ". Press Ctrl+C to stop earlier.");
        Thread.sleep(duration.toMillis());
    }
```

This step is a refactor; run existing app-related tests after it.

- [ ] **Step 4: Run app tests green**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH \
mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=A39BusinessSimulatorApplicationTest,A39LoadTestRunnerTest
```

Expected: tests pass.

- [ ] **Step 5: Commit Task 4**

```bash
git add cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39BusinessSimulatorApplication.java \
  cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/A39BusinessSimulatorApplicationTest.java
git commit -m "feat(simulator): wire load mode CLI"
```

### Task 5: README and Env Template

**Files:**
- Modify: `README.md`
- Modify: `deploy/config/a39-simulator.env.example`

- [ ] **Step 1: Update env example**

Append optional load-test settings to `deploy/config/a39-simulator.env.example`:

```bash
# Load-test mode is dev/test only. It creates orders with blank notify_url so merchant callbacks are skipped.
# export A39_MODE='load'
# export A39_LOAD_CONCURRENCY='10'
# export A39_LOAD_TOTAL_ORDERS='200'
# export A39_REQUEST_TIMEOUT='10s'
# export A39_OUT_ORDER_PREFIX='PERF-A39'
# export A39_EXPECTED_CREATE_CODES='200,510'
# export A39_ERROR_SAMPLE_LIMIT='20'
# export A39_LOAD_PROGRESS_INTERVAL='10s'
```

- [ ] **Step 2: Rewrite README A39 chapter into one standalone section**

Replace the current `## A39 业务请求模拟器` section through the end of its `### 日志和结果观察` subsection with a consolidated `## A39 业务模拟器` section. The new section must include these exact subsections:

```markdown
## A39 业务模拟器

### 工具定位与安全边界

### 打包与 test188 配置

### scenario 单笔联调模式

### load 下单压测模式

### 参数与环境变量

### notify_url 口径

### 日志、报告与排障
```

In `load 下单压测模式`, include this baseline command:

```bash
./bin/run-a39-simulator.sh \
  --mode load \
  --concurrency 10 \
  --total-orders 200 \
  --request-timeout 10s \
  --out-order-prefix PERF-A39-$(date +%Y%m%d%H%M%S)
```

State that `load` mode requires a test188 dedicated merchant/product and safe supplier mock, uses blank `notify_url`, counts `200` and `510` as success by default, does not query, does not start callback server, and does not wait for callbacks.

In `参数与环境变量`, include both existing scenario parameters and new load parameters. Keep all secret examples as `***`.

- [ ] **Step 3: Check README for duplicate A39 sections**

Run:

```bash
rg -n "^## A39|^### notify_url|A39 业务请求模拟器|A39 业务模拟器" README.md
```

Expected: exactly one top-level `## A39 业务模拟器`, and `notify_url` appears only as a subsection inside it.

- [ ] **Step 4: Commit Task 5**

```bash
git add README.md deploy/config/a39-simulator.env.example
git commit -m "docs(simulator): document A39 load testing mode"
```

### Task 6: Full Verification and Packaging

**Files:**
- No source edits expected unless verification exposes a real defect.

- [ ] **Step 1: Run full simulator tests**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH \
mvn -f cardshop-trade-engine-simulator/pom.xml test
```

Expected: all simulator tests pass with `BUILD SUCCESS`.

- [ ] **Step 2: Package simulator**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH \
./scripts/package-a39-simulator.sh
```

Expected: `dist/a39-business-simulator-0.1.0-SNAPSHOT.zip` is created.

- [ ] **Step 3: Verify CLI help mentions load options**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH \
java -jar cardshop-trade-engine-simulator/target/cardshop-trade-engine-simulator-0.1.0-SNAPSHOT.jar --help
```

Expected: help output mentions `--mode`, `--concurrency`, `--total-orders`, `--request-timeout`, and `--expected-create-codes`.

- [ ] **Step 4: Inspect git diff for scope**

Run:

```bash
git status --short
git diff --stat HEAD
```

Expected: only simulator source/tests, README, and env example changed since the implementation branch point. Do not revert unrelated existing modifications in `docs/`.

- [ ] **Step 5: Final commit if verification fixes were needed**

If Step 1-3 required any follow-up edits, commit them:

```bash
git add cardshop-trade-engine-simulator README.md deploy/config/a39-simulator.env.example
git commit -m "test(simulator): verify A39 load testing package"
```

If no edits were needed, do not create an empty commit.

## Self Review

Spec coverage:

- Fixed concurrency, total orders, request timeout, expected code handling, empty `notify_url`, and test188 safety boundaries are covered in Tasks 1, 3, 4, and 5.
- Separate `scenario` and `load` modes are covered in Tasks 1 and 4.
- Quiet load logging and stable report fields are covered in Tasks 2 and 3.
- README consolidation is covered in Task 5.
- Verification and packaging are covered in Task 6.

Placeholder scan:

- All implementation steps are explicit and scoped.
- Each task has concrete file paths, commands, expected outcomes, and code snippets.

Type consistency:

- Plan consistently uses `A39SimulatorMode`, `A39LoadTestRunner`, `LoadTestResult`, `mode()`, `concurrency()`, `totalOrders()`, `requestTimeout()`, `outOrderPrefix()`, `expectedCreateCodes()`, `errorSampleLimit()`, and `progressInterval()`.
