package com.cardshop.trade.engine.simulator.a39;

import com.cardshop.trade.engine.simulator.supplier.SupplierMockOptions;
import com.cardshop.trade.engine.simulator.supplier.SupplierMockOrderStore;
import com.cardshop.trade.engine.simulator.supplier.SupplierMockServer;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
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
        Map<String, String> env = System.getenv();
        if (hasHelp(args)) {
            A39SimulatorMode mode = tryResolveMode(args, env);
            if (mode == A39SimulatorMode.SUPPLIER_MOCK) {
                System.out.println(SupplierMockOptions.usage());
            } else {
                System.out.println(A39SimulatorOptions.usage());
            }
            return 0;
        }
        A39SimulatorMode mode = resolveMode(args, env);
        if (mode == A39SimulatorMode.SUPPLIER_MOCK) {
            return runSupplierMock(SupplierMockOptions.parse(args, env), System.out::println);
        }
        return run(A39SimulatorOptions.parse(args), System.out::println);
    }

    /**
     * 启动 supplier-mock HTTP 服务并输出监听信息，不阻塞当前线程；供单测与 {@link #runSupplierMock} 复用。
     *
     * <p>参见 {@code docs/superpowers/specs/2026-07-20-supplier-mock-design.md}「入口分流」。
     */
    static SupplierMockServer startSupplierMockServer(SupplierMockOptions options, Consumer<String> logSink)
            throws IOException {
        SupplierMockOrderStore store = new SupplierMockOrderStore(
                options.finalResultDelayMsMin(),
                options.finalResultDelayMsMax(),
                options.successRate());
        SupplierMockServer server = SupplierMockServer.start(options, store);
        logSink.accept("A39 simulator starting: supplier-mock " + options.sanitizedSummary());
        logSink.accept("Supplier mock listening: " + server.baseUri());
        logSink.accept("Dispatch paths: /supplier/dispatch, /tripartite/coupon/batchGrandCoupon");
        logSink.accept("Query paths: /supplier/query, /tripartite/coupon/queryGrand");
        return server;
    }

    private static int runSupplierMock(SupplierMockOptions options, Consumer<String> logSink) throws Exception {
        SupplierMockServer server = startSupplierMockServer(options, logSink);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "supplier-mock-shutdown"));
        logSink.accept("Press Ctrl+C to stop supplier mock.");
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            server.close();
        }
        return 0;
    }

    static A39SimulatorMode resolveMode(String[] args, Map<String, String> env) {
        return A39SimulatorMode.parse(extractModeValue(args, env));
    }

    static A39SimulatorMode tryResolveMode(String[] args, Map<String, String> env) {
        try {
            return resolveMode(args, env);
        } catch (IllegalArgumentException ex) {
            return A39SimulatorMode.SCENARIO;
        }
    }

    private static String extractModeValue(String[] args, Map<String, String> env) {
        for (int index = 0; index < args.length; index++) {
            String arg = args[index];
            if (arg.startsWith("--mode=")) {
                return arg.substring("--mode=".length());
            }
            if ("--mode".equals(arg)) {
                if (index + 1 < args.length && !args[index + 1].startsWith("--")) {
                    return args[index + 1];
                }
                return "";
            }
        }
        String envMode = env.get("A39_MODE");
        if (envMode != null && !envMode.isBlank()) {
            return envMode.trim();
        }
        return "scenario";
    }

    static int run(A39SimulatorOptions options, Consumer<String> logSink) throws Exception {
        V1CardApiClient client = new HttpV1CardApiClient(
                options.baseUrl(),
                options.requestTimeout(),
                options.mode() == A39SimulatorMode.SCENARIO);
        return run(options, client, logSink);
    }

    static int run(A39SimulatorOptions options, V1CardApiClient client, Consumer<String> logSink) throws Exception {
        V12Signer signer = new V12Signer();
        if (options.mode() == A39SimulatorMode.LOAD) {
            return runLoad(options, client, signer, logSink);
        }
        return runScenario(options, client, signer, logSink);
    }

    private static int runLoad(
            A39SimulatorOptions options,
            V1CardApiClient client,
            V12Signer signer,
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
            V12Signer signer,
            Consumer<String> logSink) throws Exception {
        try (A39CallbackServer callbackServer = A39CallbackServer.start(
                options.callbackBindHost(),
                options.callbackPort(),
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
                    logSink, callbackServer::registerCredentials, callbackServer::registerOrder);
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
