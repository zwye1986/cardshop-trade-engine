package com.cardshop.trade.engine.simulator.a39;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * A39 业务请求模拟器入口。
 *
 * <p>该入口只用于 dev/test 联调，不参与交易引擎生产启动。参见
 * {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」和「9.3 回调设计」。
 */
public final class A39BusinessSimulatorApplication {

    private A39BusinessSimulatorApplication() {
    }

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

    static int run(A39SimulatorOptions options, Consumer<String> logSink) throws Exception {
        V1CardApiClient client = new HttpV1CardApiClient(
                options.baseUrl(),
                options.requestTimeout(),
                options.mode() == A39SimulatorMode.SCENARIO);
        return run(options, client, logSink);
    }

    static int run(A39SimulatorOptions options, V1CardApiClient client, Consumer<String> logSink) throws Exception {
        V1Sm3Signer signer = new V1Sm3Signer();
        if (options.mode() == A39SimulatorMode.LOAD) {
            return runLoad(options, client, signer, logSink);
        }
        return runScenario(options, client, signer, logSink);
    }

    private static int runLoad(
            A39SimulatorOptions options,
            V1CardApiClient client,
            V1Sm3Signer signer,
            Consumer<String> logSink) throws Exception {
        // load 模式只压创建订单入口；参见 docs/superpowers/specs/2026-07-15-a39-simulator-load-test-design.md。
        logSink.accept("A39 simulator starting: " + options.sanitizedSummary());
        logSink.accept("Load mode skips callback server, query loop, and callback wait.");
        A39LoadTestRunner.LoadTestResult result = new A39LoadTestRunner(
                client,
                signer,
                Clock.systemDefaultZone(),
                logSink)
                .run(options);
        logSink.accept("A39 load test summary:");
        logSink.accept(result.toReport());
        return result.exitCode();
    }

    private static int runScenario(
            A39SimulatorOptions options,
            V1CardApiClient client,
            V1Sm3Signer signer,
            Consumer<String> logSink) throws Exception {
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
                    client,
                    signer,
                    null,
                    logSink);
            A39BusinessScenarioRunner.ScenarioResult result = runner.run(options);
            logSink.accept("Create response: code=%s, msg=%s".formatted(
                    result.createResponse().code(),
                    result.createResponse().msg()));
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

    private static boolean hasHelp(String[] args) {
        for (String arg : args) {
            if ("--help".equals(arg) || "-h".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    private static void waitForCallbacks(Duration duration, Consumer<String> logSink) throws InterruptedException {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            return;
        }
        logSink.accept("Waiting for callbacks for " + duration + ". Press Ctrl+C to stop earlier.");
        Thread.sleep(duration.toMillis());
    }
}
