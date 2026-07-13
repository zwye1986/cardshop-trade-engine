package com.cardshop.trade.engine.simulator.a39;

import java.time.Duration;

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
        if (hasHelp(args)) {
            System.out.println(A39SimulatorOptions.usage());
            return;
        }
        A39SimulatorOptions options = A39SimulatorOptions.parse(args);
        V1Sm3Signer signer = new V1Sm3Signer();
        try (A39CallbackServer callbackServer = A39CallbackServer.start(
                options.callbackBindHost(),
                options.callbackPort(),
                options.aesKey(),
                signer)) {
            System.out.println("A39 simulator starting: " + options.sanitizedSummary());
            System.out.println("Callback server listening: " + callbackServer.callbackUrl("/callback/order"));
            if (options.notifyUrl().contains("127.0.0.1") || options.notifyUrl().contains("localhost")) {
                System.out.println("Note: local notify_url only works when the tested platform can reach this host.");
            }

            A39BusinessScenarioRunner runner = new A39BusinessScenarioRunner(
                    new HttpV1CardApiClient(options.baseUrl()),
                    signer,
                    null,
                    System.out::println);
            A39BusinessScenarioRunner.ScenarioResult result = runner.run(options);
            System.out.println("Create response: code=%s, msg=%s".formatted(
                    result.createResponse().code(),
                    result.createResponse().msg()));
            for (int index = 0; index < result.queryResponses().size(); index++) {
                V1CardApiClient.ApiResponse response = result.queryResponses().get(index);
                System.out.println("Query response #%d: code=%s, msg=%s, data=%s".formatted(
                        index + 1,
                        response.code(),
                        response.msg(),
                        response.data()));
            }
            waitForCallbacks(options.callbackWait());
            if (!callbackServer.receivedCallbacks().isEmpty()) {
                callbackServer.receivedCallbacks().forEach(callback -> System.out.println(
                        "Callback received: path=%s, signatureValid=%s, body=%s".formatted(
                                callback.path(),
                                callback.signatureValid(),
                                callback.rawBody())));
            }
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

    private static void waitForCallbacks(Duration duration) throws InterruptedException {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            return;
        }
        System.out.println("Waiting for callbacks for " + duration + ". Press Ctrl+C to stop earlier.");
        Thread.sleep(duration.toMillis());
    }
}
