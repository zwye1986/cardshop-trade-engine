package com.cardshop.trade.engine.simulator.a39;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;

class HttpV1CardApiClientTest {

    @Test
    void baseUrlActsAsRequestPrefixEvenWhenItContainsPathSegments() {
        HttpV1CardApiClient client = new HttpV1CardApiClient(
                URI.create("http://apitest.kalaiwang.com/testSuite/trade-api/dev"),
                HttpClient.newHttpClient(),
                new ObjectMapper(),
                new LegacySm4BodyEncoder(),
                ignored -> {
                });

        assertThat(client.uri("/v1/cardapi/auth/getToken"))
                .isEqualTo(URI.create("http://apitest.kalaiwang.com/testSuite/trade-api/dev/v1/cardapi/auth/getToken"));
    }

    @Test
    void logsSanitizedCurlAndRawResponseForRequests() throws Exception {
        List<String> logs = new ArrayList<>();
        try (TestHttpServer server = TestHttpServer.start()) {
            HttpV1CardApiClient client = new HttpV1CardApiClient(
                    URI.create(server.baseUrl()),
                    HttpClient.newHttpClient(),
                    new ObjectMapper(),
                    new LegacySm4BodyEncoder(),
                    logs::add);

            String token = client.getToken("app-id", "secret-value");

            assertThat(token).isEqualTo("token-value");
            assertThat(logs).anySatisfy(log -> assertThat(log)
                    .contains("[A39][HTTP][request]")
                    .contains("curl -i -X POST")
                    .contains("/v1/cardapi/auth/getToken")
                    .contains("\"app_secret\":\"***\""));
            assertThat(logs).anySatisfy(log -> assertThat(log)
                    .contains("[A39][HTTP][response]")
                    .contains("status=200")
                    .contains("\"token\":\"token-value\""));
            assertThat(String.join("\n", logs)).doesNotContain("secret-value");
        }
    }

    @Test
    void appliesConfiguredRequestTimeoutToPostRequests() throws Exception {
        CapturingTimeoutHttpClient httpClient = new CapturingTimeoutHttpClient();
        HttpV1CardApiClient client = new HttpV1CardApiClient(
                URI.create("http://api.example"),
                httpClient,
                new ObjectMapper(),
                new LegacySm4BodyEncoder(),
                ignored -> {
                },
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

    @Test
    void suppressesRequestAndResponseLogsAfterSuccessfulResponseWhenDisabled() throws Exception {
        List<String> logs = new ArrayList<>();
        try (TestHttpServer server = TestHttpServer.start()) {
            HttpV1CardApiClient client = new HttpV1CardApiClient(
                    URI.create(server.baseUrl()),
                    HttpClient.newHttpClient(),
                    new ObjectMapper(),
                    new LegacySm4BodyEncoder(),
                    logs::add,
                    Duration.ofSeconds(3),
                    false);

            String token = client.getToken("app-id", "secret-value");

            assertThat(token).isEqualTo("token-value");
            assertThat(logs).isEmpty();
        }
    }

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

    private static final class TestHttpServer implements AutoCloseable {

        private final HttpServer server;

        private TestHttpServer(HttpServer server) {
            this.server = server;
        }

        static TestHttpServer start() throws IOException {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/cardapi/auth/getToken", exchange -> {
                byte[] bytes = """
                        {"code":200,"msg":"操作成功","data":{"token":"token-value"}}
                        """.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream outputStream = exchange.getResponseBody()) {
                    outputStream.write(bytes);
                }
            });
            server.start();
            return new TestHttpServer(server);
        }

        String baseUrl() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
