package com.cardshop.trade.engine.infrastructure.supplier.miniso;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 本地真实 HTTP 协议测试；见详细设计「名创 MINISO 专属协议实施约束（2026-10-17）」。 */
class MinisoHttpClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TEST_KEY = "local-test-key";
    private static final String NONCE = "0123456789abcdefghijklmnopqrstuv";
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-17T02:03:04.005Z"), ZoneOffset.UTC);
    private HttpServer server;
    private URI baseUri;
    private final BlockingQueue<CapturedRequest> requests = new LinkedBlockingQueue<>();
    private volatile int status = 200;
    private volatile String responseBody = "{\"code\":200,\"data\":[]}";
    private volatile long delayMillis;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        baseUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        server.createContext("/", exchange -> {
            requests.add(new CapturedRequest(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
                    exchange.getRequestHeaders(), exchange.getRequestBody().readAllBytes()));
            if (delayMillis > 0) {
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("local fake server interrupted", e);
                }
            }
            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void batchGrandPostsNumericBatchAndSignsActualUtf8BodyOnce() throws Exception {
        CountingMapper mapper = new CountingMapper();
        MinisoApiResponse result = client(mapper).batchGrandCoupon("13800138000", 123, 1, "平台订单-1");

        CapturedRequest request = nextRequest();
        assertThat(request.path()).isEqualTo("/tripartite/coupon/batchGrandCoupon");
        assertThat(request.bodyUtf8()).isEqualTo(
                "{\"phone\":\"13800138000\",\"batchList\":[{\"batchId\":123,\"number\":1}],\"orderNo\":\"平台订单-1\"}");
        assertThat(MAPPER.readTree(request.body()).path("batchList").get(0).path("batchId").isInt()).isTrue();
        verifyAuthentication(request, NONCE);
        assertThat(mapper.serializations).isEqualTo(1);
        assertThat(result.httpStatus()).isEqualTo(200);
        assertThat(result.body().path("code").asInt()).isEqualTo(200);
        assertThat(result.rawBody()).isEqualTo(responseBody);
    }

    @Test
    void queryUsesOriginalOrderNumberAndFullProtocolHeaders() throws Exception {
        client(MAPPER).queryGrand("ORDER-1");
        CapturedRequest request = nextRequest();
        assertThat(request.path()).isEqualTo("/tripartite/coupon/queryGrand");
        assertThat(request.bodyUtf8()).isEqualTo("{\"orderNo\":\"ORDER-1\"}");
        verifyAuthentication(request, NONCE);
    }

    @Test
    void abandonAndSelectJoinAtMostTenCouponCodesWithoutChangingOrder() throws Exception {
        MinisoHttpClient client = client(MAPPER);
        client.abandonUserCoupon(List.of("COUPON-2", "COUPON-1"));
        CapturedRequest abandon = nextRequest();
        assertThat(abandon.path()).isEqualTo("/tripartite/coupon/abandonUserCoupon");
        assertThat(abandon.bodyUtf8()).isEqualTo("{\"couponCodes\":\"COUPON-2,COUPON-1\"}");
        verifyAuthentication(abandon, NONCE);

        List<String> codes = IntStream.range(0, 10).mapToObj(i -> "C" + i).toList();
        client.selectUserCouponList(codes);
        CapturedRequest select = nextRequest();
        assertThat(select.path()).isEqualTo("/tripartite/coupon/selectUserCouponList");
        assertThat(MAPPER.readTree(select.body()).path("couponCodes").asText()).isEqualTo(String.join(",", codes));
        verifyAuthentication(select, NONCE);
    }

    @Test
    void refusesInvalidCouponCollectionsBeforeAnyHttpRequest() {
        MinisoHttpClient client = client(MAPPER);
        List<List<String>> invalid = Arrays.asList(null, List.of(), List.of(" "),
                Arrays.asList("C1", null), List.of("C1", "C1"), List.of("C1,,C2"),
                IntStream.range(0, 11).mapToObj(i -> "C" + i).toList());
        for (List<String> codes : invalid) {
            assertThatThrownBy(() -> client.abandonUserCoupon(codes)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> client.selectUserCouponList(codes)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(requests).isEmpty();
    }

    @Test
    void refusesMissingIdentityAndInvalidQuantitiesBeforeSending() {
        MinisoHttpClient client = client(MAPPER);
        assertThatThrownBy(() -> client.queryGrand(" ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> client.batchGrandCoupon(null, 1, 1, "ORDER-1"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> client.batchGrandCoupon("13800138000", 0, 1, "ORDER-1"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> client.batchGrandCoupon("13800138000", 1, 0, "ORDER-1"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(requests).isEmpty();
    }

    @Test
    void keepsNonSuccessHttpStatusAndRawResponseForGatewayClassification() throws Exception {
        status = 503;
        responseBody = "{\"code\":500,\"message\":\"temporary\"}";
        MinisoApiResponse result = client(MAPPER).queryGrand("ORDER-1");
        assertThat(result.httpStatus()).isEqualTo(503);
        assertThat(result.body().path("code").asInt()).isEqualTo(500);
        assertThat(result.rawBody()).isEqualTo(responseBody);
    }

    @Test
    void nonJsonResponseCannotBecomeAConfirmedBusinessOutcome() {
        responseBody = "sensitive-response-coupon";
        assertThatThrownBy(() -> client(MAPPER).queryGrand("ORDER-1"))
                .isInstanceOf(IOException.class)
                .hasMessageNotContaining("sensitive-response-coupon");
    }

    @Test
    void rejectsTrailingResponseContentInsteadOfTrustingOnlyFirstJsonObject() {
        responseBody = "{\"code\":200} {\"couponCode\":\"sensitive-response-coupon\"}";
        assertThatThrownBy(() -> client(MAPPER).queryGrand("ORDER-1"))
                .isInstanceOf(IOException.class)
                .hasMessageNotContaining("sensitive-response-coupon");
    }

    @Test
    void invalidResponseExceptionRetainsHttpAuditEnvelopeWithoutExposingPayloadInStack() {
        status = 502;
        for (String raw : List.of("private-coupon-response", "", "{\"code\":200} private-coupon-response")) {
            responseBody = raw;
            assertThatThrownBy(() -> client(MAPPER).queryGrand("ORDER-1"))
                    .isInstanceOf(MinisoHttpClient.InvalidResponseException.class)
                    .satisfies(failure -> {
                        var invalid = (MinisoHttpClient.InvalidResponseException) failure;
                        assertThat(invalid.response().httpStatus()).isEqualTo(502);
                        assertThat(invalid.response().body()).isNull();
                        assertThat(invalid.response().rawBody()).isEqualTo(raw);
                        assertThat(invalid.getCause()).isNull();
                        StringWriter trace = new StringWriter();
                        invalid.printStackTrace(new PrintWriter(trace));
                        assertThat(trace.toString()).doesNotContain("private-coupon-response", TEST_KEY);
                        assertThat(invalid.toString()).doesNotContain("private-coupon-response", TEST_KEY);
                        assertThat(invalid.response().toString()).doesNotContain("private-coupon-response", TEST_KEY);
                    });
        }
    }

    @Test
    void timeoutPropagatesForUnknownOutcomeClassification() {
        delayMillis = 300;
        MinisoHttpClient client = new MinisoHttpClient(settings(baseUri, Duration.ofMillis(40)),
                HttpClient.newHttpClient(), MAPPER, CLOCK, () -> NONCE);
        assertThatThrownBy(() -> client.queryGrand("ORDER-1")).isInstanceOf(HttpTimeoutException.class);
    }

    @Test
    void productionConstructorGeneratesFreshLowercaseAlphanumericNonce() throws Exception {
        MinisoHttpClient client = new MinisoHttpClient(settings(baseUri, Duration.ofSeconds(2)),
                HttpClient.newHttpClient(), MAPPER);
        client.queryGrand("ORDER-1");
        client.queryGrand("ORDER-1");
        CapturedRequest first = nextRequest();
        CapturedRequest second = nextRequest();
        assertThat(first.headers().getFirst("nonce")).matches("[a-z0-9]{32}")
                .isNotEqualTo(second.headers().getFirst("nonce"));
        assertThat(second.headers().getFirst("nonce")).matches("[a-z0-9]{32}");
    }

    @Test
    void rejectsInvalidNonceFromInjectedSupplierBeforeSending() {
        MinisoHttpClient client = new MinisoHttpClient(settings(baseUri, Duration.ofSeconds(2)),
                HttpClient.newHttpClient(), MAPPER, CLOCK, () -> "Invalid");
        assertThatThrownBy(() -> client.queryGrand("ORDER-1")).isInstanceOf(IllegalArgumentException.class);
        assertThat(requests).isEmpty();
    }

    @Test
    void validatesFixedProtocolSettingsAndHidesSensitiveValuesInToString() {
        assertThatThrownBy(() -> new MinisoSettings(baseUri, "OTHER", "test-partner", "tripartite1.0", TEST_KEY,
                Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MinisoSettings(baseUri, "MINISO", "test-partner", "v2", TEST_KEY,
                Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MinisoSettings(baseUri, "MINISO", " ", "tripartite1.0", TEST_KEY,
                Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MinisoSettings(baseUri, "MINISO", "test-partner", "tripartite1.0", " ",
                Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> settings(baseUri, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThat(settings(baseUri, Duration.ofSeconds(1)).toString()).doesNotContain(TEST_KEY);
        assertThat(new MinisoApiResponse(200, MAPPER.createObjectNode().put("coupon", "sensitive-coupon"),
                "sensitive-coupon").toString()).doesNotContain("sensitive-coupon");
    }

    @Test
    void permitsHttpsAndExplicitLoopbackHttpButRejectsUnsafeBaseUris() {
        for (String valid : List.of("https://supplier.example", "http://localhost:8080", "http://[::1]:8080")) {
            assertThat(settings(URI.create(valid), Duration.ofSeconds(1)).baseUri()).isEqualTo(URI.create(valid));
        }
        for (String invalid : List.of("http://supplier.example", "http://localhost.attacker.example", "ftp://localhost",
                "https://user:secret@supplier.example", "https://supplier.example?key=secret",
                "https://supplier.example#secret", "/relative")) {
            assertThatThrownBy(() -> settings(URI.create(invalid), Duration.ofSeconds(1)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining("secret");
        }
    }

    private MinisoHttpClient client(ObjectMapper mapper) {
        return new MinisoHttpClient(settings(baseUri, Duration.ofSeconds(2)), HttpClient.newHttpClient(),
                mapper, CLOCK, () -> NONCE);
    }

    private static MinisoSettings settings(URI uri, Duration timeout) {
        return new MinisoSettings(uri, "MINISO", "test-partner", "tripartite1.0", TEST_KEY, timeout);
    }

    private CapturedRequest nextRequest() throws InterruptedException {
        CapturedRequest request = requests.poll(2, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        return request;
    }

    private static void verifyAuthentication(CapturedRequest request, String nonce) throws Exception {
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.headers().getFirst("tenant")).isEqualTo("MINISO");
        assertThat(request.headers().getFirst("tripartite")).isEqualTo("test-partner");
        assertThat(request.headers().getFirst("version")).isEqualTo("tripartite1.0");
        assertThat(request.headers().getFirst("nonce")).isEqualTo(nonce);
        assertThat(request.headers().getFirst("time")).isEqualTo(Long.toString(CLOCK.millis()));
        assertThat(request.headers().getFirst("Content-Type")).isEqualTo("application/json; charset=UTF-8");
        String original = TEST_KEY + "nonce" + nonce + "tenantMINISOtime" + CLOCK.millis()
                + "versiontripartite1.0" + request.bodyUtf8();
        String expected = HexFormat.of().withUpperCase().formatHex(
                MessageDigest.getInstance("MD5").digest(original.getBytes(StandardCharsets.UTF_8)));
        assertThat(request.headers().getFirst("sign")).isEqualTo(expected).matches("[A-F0-9]{32}");
    }

    private record CapturedRequest(String method, String path, Headers headers, byte[] body) {
        String bodyUtf8() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    private static class CountingMapper extends ObjectMapper {
        private int serializations;

        @Override
        public byte[] writeValueAsBytes(Object value) throws JsonProcessingException {
            serializations++;
            return super.writeValueAsBytes(value);
        }
    }
}
