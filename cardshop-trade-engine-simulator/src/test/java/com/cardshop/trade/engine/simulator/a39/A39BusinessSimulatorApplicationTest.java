package com.cardshop.trade.engine.simulator.a39;

import com.cardshop.trade.engine.simulator.supplier.SupplierMockOptions;
import com.cardshop.trade.engine.simulator.supplier.SupplierMockServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class A39BusinessSimulatorApplicationTest {

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void supplierMockHelpPrintsSupplierMockUsage() throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured));
        try {
            int exitCode = A39BusinessSimulatorApplication.run(new String[]{"--help", "--mode", "supplier-mock"});

            assertThat(exitCode).isZero();
            assertThat(captured.toString()).contains("Supplier mock simulator");
            assertThat(captured.toString()).doesNotContain("Secret sources:");
        } finally {
            System.setOut(original);
        }
    }

    @Test
    void supplierMockModeDoesNotRequireMerchantSecretsAndServesDispatch() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        SupplierMockOptions options = new SupplierMockOptions("127.0.0.1", port, 0, 0, 1.0);
        List<String> logs = new ArrayList<>();
        try (SupplierMockServer server = A39BusinessSimulatorApplication.startSupplierMockServer(options, logs::add)) {
            JsonNode dispatch = parseJson(post(server.baseUri().resolve("/supplier/dispatch"),
                    """
                            {"order_id":"O-1","supplier_request_id":"dispatch-O-1"}
                            """).body());
            assertThat(dispatch.get("status").asText()).isEqualTo("PROCESSING");
            assertThat(String.join("\n", logs)).contains("supplier-mock");
        }
    }

    @Test
    void loadModeReturnsRunnerExitCodeAndSkipsCallbackServerText() throws Exception {
        List<String> logs = new ArrayList<>();
        SingleCreateClient client = new SingleCreateClient(new V1CardApiClient.ApiResponse("200", "ok", Map.of()));

        int exitCode = A39BusinessSimulatorApplication.run(loadOptions(), client, logs::add);

        assertThat(exitCode).isZero();
        assertThat(client.queryCount).isZero();
        assertThat(String.join("\n", logs))
                .contains("Load mode skips callback server")
                .contains("A39 load test summary")
                .contains("mode=LOAD")
                .doesNotContain("Callback server listening");
    }

    @Test
    void loadModeReturnsNonZeroWhenCreateResponsesAreUnexpected() throws Exception {
        List<String> logs = new ArrayList<>();
        SingleCreateClient client = new SingleCreateClient(new V1CardApiClient.ApiResponse("501", "验签不合规", Map.of()));

        int exitCode = A39BusinessSimulatorApplication.run(loadOptions(), client, logs::add);

        assertThat(exitCode).isEqualTo(1);
        assertThat(client.createCount).isEqualTo(1);
        assertThat(client.queryCount).isZero();
        assertThat(String.join("\n", logs))
                .contains("failed=1")
                .contains("exitCode=1")
                .doesNotContain("Callback server listening");
    }

    @Test
    void loadModeReturnsNonZeroAndSkipsCreateWhenTokenFails() throws Exception {
        List<String> logs = new ArrayList<>();
        TokenFailureClient client = new TokenFailureClient();

        int exitCode = A39BusinessSimulatorApplication.run(loadOptions(), client, logs::add);

        assertThat(exitCode).isEqualTo(1);
        assertThat(client.createCount).isZero();
        assertThat(client.queryCount).isZero();
        assertThat(String.join("\n", logs))
                .contains("completed=0")
                .contains("failed=1")
                .contains("error.IllegalStateException=1")
                .contains("exitCode=1");
    }

    private HttpResponse<String> post(URI uri, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode parseJson(String body) throws Exception {
        return objectMapper.readTree(body);
    }

    private static A39SimulatorOptions loadOptions() {
        return new A39SimulatorOptions(
                "http://apitest.kalaiwang.com",
                "app-id",
                "app-secret",
                "AES-KEY",
                "73301280",
                "18186222336",
                "IGNORED",
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
                1,
                1,
                Duration.ofSeconds(10),
                "PERF-A39",
                false,
                List.of("200", "510"),
                20,
                Duration.ZERO,
                Duration.ZERO,
                0);
    }

    private static final class SingleCreateClient implements V1CardApiClient {

        private final ApiResponse response;
        private int createCount;
        private int queryCount;

        private SingleCreateClient(ApiResponse response) {
            this.response = response;
        }

        @Override
        public String getToken(String appId, String appSecret) {
            return "token";
        }

        @Override
        public ApiResponse createRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            createCount++;
            assertThat(payload.get("notify_url")).isEqualTo("");
            return response;
        }

        @Override
        public ApiResponse getRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            queryCount++;
            throw new AssertionError("load mode must not query order");
        }
    }

    private static final class TokenFailureClient implements V1CardApiClient {

        private int createCount;
        private int queryCount;

        @Override
        public String getToken(String appId, String appSecret) {
            throw new IllegalStateException("token service unavailable");
        }

        @Override
        public ApiResponse createRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            createCount++;
            throw new AssertionError("load mode must not create orders when token acquisition fails");
        }

        @Override
        public ApiResponse getRechargeOrder(String token, Map<String, Object> payload, boolean sm4Enabled, String aesKey) {
            queryCount++;
            throw new AssertionError("load mode must not query order");
        }
    }
}
