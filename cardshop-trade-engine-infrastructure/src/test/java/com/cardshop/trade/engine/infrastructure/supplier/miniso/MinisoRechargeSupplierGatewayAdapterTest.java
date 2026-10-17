package com.cardshop.trade.engine.infrastructure.supplier.miniso;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;
import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.application.recharge.SupplierDispatchGatewayRequest;
import com.cardshop.trade.engine.application.recharge.SupplierGatewayResponse;
import com.cardshop.trade.engine.application.recharge.SupplierGatewayStatus;
import com.cardshop.trade.engine.application.recharge.SupplierInvalidGatewayRequest;
import com.cardshop.trade.engine.application.recharge.SupplierQueryGatewayRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/** 真实本地 HTTP 验证名创保守归一化和精准作废；参见详细设计「名创 MINISO 专属协议实施约束」。 */
class MinisoRechargeSupplierGatewayAdapterTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String ORDER = "ORDER-1";
    private static final String ORG = "ORG-1";
    private static final String PHONE = "13800138000";
    private static final String COUPON = "COUPON-1";
    private HttpServer server;
    private MinisoHttpClient client;
    private final BlockingQueue<Reply> replies = new LinkedBlockingQueue<>();
    private final BlockingQueue<Call> calls = new LinkedBlockingQueue<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            calls.add(new Call(exchange.getRequestURI().getPath(), JSON.readTree(exchange.getRequestBody())));
            Reply reply = replies.remove();
            byte[] bytes = reply.body().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(reply.status(), bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        client = new MinisoHttpClient(new MinisoSettings(uri, "MINISO", "test", "tripartite1.0", "test-key",
                Duration.ofSeconds(2)), HttpClient.newHttpClient(), JSON);
    }

    @AfterEach
    void stopServer() {
        Thread.interrupted();
        server.stop(0);
    }

    @Test
    void dispatchUsesFrozenSnapshotAndPlatformOrderWithDefaultUnusedStatus() {
        String raw = envelope(coupon(null));
        reply(raw);
        SupplierGatewayResponse response = adapter().dispatch(dispatch("MINISO"));
        assertSuccess(response, 0);
        assertThat(response.rawResponse()).isEqualTo(raw);
        Call call = calls.remove();
        assertThat(call.path()).endsWith("/batchGrandCoupon");
        assertThat(call.body().path("orderNo").asText()).isEqualTo(ORDER);
        assertThat(call.body().path("phone").asText()).isEqualTo(PHONE);
        assertThat(call.body().path("batchList").get(0).path("batchId").intValue()).isEqualTo(123);
        assertThat(call.body().path("batchList").get(0).path("number").intValue()).isEqualTo(1);
    }

    @Test
    void acceptsTextSuccessCodeForLegacyCompatibility() {
        reply(envelope(coupon(0)).replace("\"code\":200", "\"code\":\"200\""));
        assertSuccess(adapter().query(query()), 0);
    }

    @Test
    void queryConfirmsIssuanceForEveryDocumentedCouponStatus() {
        for (int status : List.of(0, 1, 2, 3)) {
            reply(envelope(coupon(status)));
            assertSuccess(adapter().query(query()), status);
            Call call = calls.remove();
            assertThat(call.path()).endsWith("/queryGrand");
            assertThat(call.body().path("orderNo").asText()).isEqualTo(ORDER);
        }
    }

    @Test
    void incompleteOrMismatchedReceiptNeverBecomesTerminal() throws Exception {
        JsonNode valid = JSON.readTree(coupon(0));
        for (String field : List.of("batchId", "phone", "couponCode", "beginTime", "endTime", "status")) {
            var damaged = valid.deepCopy();
            ((com.fasterxml.jackson.databind.node.ObjectNode) damaged).remove(field);
            reply(envelope(damaged.toString()));
            assertUnknown(adapter().query(query()));
        }
        for (String damaged : List.of(
                coupon(0).replace("123", "124"), coupon(0).replace(PHONE, "13900139000"),
                coupon(0).replace("123", "\"123\""), coupon(0).replace("123", "123.0"),
                coupon(0).replace(COUPON, " "), coupon(0).replace("2026-10-01 00:00:00", "2026-02-30 00:00:00"),
                coupon(0).replace("2026-10-01 00:00:00", "2026-10-01"),
                coupon(0).replace("2026-12-01", "2026-09-01"),
                coupon(4), coupon(0).replace("\"status\":0", "\"status\":\"0\""),
                coupon(0).replace("\"status\":0", "\"status\":0.0"))) {
            reply(envelope(damaged));
            assertUnknown(adapter().query(query()));
        }
    }

    @Test
    void invalidQueryRequiresCompleteReceiptBeforeAnyAbandon() {
        reply("{\"code\":200,\"data\":[]}");
        assertUnknown(adapter().invalid(invalid()));
        assertThat(calls.remove().path()).endsWith("/queryGrand");
        assertThat(calls).isEmpty();
    }

    @Test
    void invalidDatesPreserveRawEvidenceWithoutTrustingReceipt() {
        String raw = envelope(coupon(0).replace("2026-10-01", "2026-02-30"));
        reply(raw);
        SupplierGatewayResponse response = adapter().query(query());
        assertUnknown(response);
        assertThat(response.rawResponse()).isEqualTo(raw);
    }

    @Test
    void unknownEnvelopeAndHttpFailuresCannotTriggerRefund() {
        for (String body : List.of("{\"code\":200}", "{\"code\":200,\"data\":[]}",
                "{\"code\":200,\"data\":\"[]\"}", "{\"code\":200,\"data\":{}}",
                envelope(coupon(0) + "," + coupon(0)), envelope(coupon(0)).replace("\"code\":200", "\"code\":404"),
                "not-json")) {
            reply(body);
            assertUnknown(adapter().dispatch(dispatch("MINISO")));
        }
        replies.add(new Reply(503, envelope(coupon(0))));
        assertUnknown(adapter().query(query()));
    }

    @Test
    void malformedEmptyAndTrailingResponsesRetainRawAuditForEveryOperation() {
        for (String raw : List.of("private-coupon-response", "", "{\"code\":200} private-coupon-response")) {
            for (String operation : List.of("dispatch", "query", "invalid")) {
                reply(raw);
                SupplierGatewayResponse response = switch (operation) {
                    case "dispatch" -> adapter().dispatch(dispatch("MINISO"));
                    case "query" -> adapter().query(query());
                    default -> adapter().invalid(invalid());
                };
                assertUnknown(response);
                assertThat(response.rawResponse()).isEqualTo(raw);
            }
        }
    }

    @Test
    void malformedAbandonResponseRetainsItsRawEvidenceAfterSuccessfulPreQuery() {
        reply(envelope(coupon(0)));
        String raw = "private-coupon-response";
        reply(raw);
        SupplierGatewayResponse response = adapter().invalid(invalid());
        assertUnknown(response);
        assertThat(response.rawResponse()).isEqualTo(raw);
    }

    @Test
    void sampledInvalidResponseLogContainsSafeStackWithoutAuditPayload() {
        Logger logger = (Logger) LoggerFactory.getLogger(MinisoRechargeSupplierGatewayAdapter.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            // 默认每 100 次采样；其它用例可能已消耗首条，因此至多推进一个完整采样周期。
            for (int attempt = 0; attempt < 100 && appender.list.isEmpty(); attempt++) {
                reply("private-coupon-response " + PHONE + " test-key " + COUPON);
                assertUnknown(adapter().query(query()));
            }
            assertThat(appender.list).isNotEmpty();
            for (ILoggingEvent event : appender.list) {
                assertThat(event.getThrowableProxy()).isNotNull();
                String logged = event.getFormattedMessage() + ThrowableProxyUtil.asString(event.getThrowableProxy());
                assertThat(logged).contains("ORDER-1", "query", "InvalidResponseException")
                        .doesNotContain("private-coupon-response", PHONE, "test-key", COUPON);
            }
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void dispatchRejectsContradictoryCouponStatus() {
        for (int status : List.of(1, 2, 3, 4)) {
            reply(envelope(coupon(status)));
            assertUnknown(adapter().dispatch(dispatch("MINISO")));
        }
    }

    @Test
    void untrustedOrInvalidSnapshotsNeverInvokeSupplier() {
        for (MinisoOrderSnapshot snapshot : List.of(
                new MinisoOrderSnapshot(ORG, ORDER, PHONE, "0", 1),
                new MinisoOrderSnapshot(ORG, ORDER, PHONE, "123.0", 1),
                new MinisoOrderSnapshot(ORG, ORDER, PHONE, "2147483648", 1),
                new MinisoOrderSnapshot(ORG, ORDER, PHONE, "123", 2),
                new MinisoOrderSnapshot("OTHER", ORDER, PHONE, "123", 1),
                new MinisoOrderSnapshot(ORG, "OTHER", PHONE, "123", 1),
                new MinisoOrderSnapshot(ORG, ORDER, "OTHER", "123", 1))) {
            MinisoRechargeSupplierGatewayAdapter gateway = new MinisoRechargeSupplierGatewayAdapter(client,
                    (org, order) -> Optional.of(snapshot));
            assertUnknown(gateway.dispatch(dispatch("MINISO")));
            assertUnknown(gateway.query(query()));
            assertUnknown(gateway.invalid(invalid()));
        }
        assertUnknown(new MinisoRechargeSupplierGatewayAdapter(client, (org, order) -> Optional.empty())
                .dispatch(dispatch("MINISO")));
        assertUnknown(adapter().dispatch(dispatch("OTHER")));
        assertThat(calls).isEmpty();
    }

    @Test
    void invalidQueriesFirstAndConfirmsExactAllSuccess() {
        reply(envelope(coupon(0)));
        reply("{\"code\":200,\"data\":{\"succeed\":[{\"couponCode\":\"COUPON-1\",\"msg\":\"成功\"}],\"failed\":[]}}");
        assertSuccess(adapter().invalid(invalid()), 3);
        assertThat(calls.remove().path()).endsWith("/queryGrand");
        Call abandon = calls.remove();
        assertThat(abandon.path()).endsWith("/abandonUserCoupon");
        assertThat(abandon.body().path("couponCodes").asText()).isEqualTo(COUPON);
    }

    @Test
    void invalidRequiresPersistedReceiptBeforeAbandon() {
        reply(envelope(coupon(0)));
        reply("{\"code\":200,\"data\":{\"succeed\":[{\"couponCode\":\"COUPON-1\",\"msg\":\"成功\"}],\"failed\":[]}}");
        assertUnknown(adapter(Optional.empty()).invalid(invalid()));
        assertThat(calls).noneMatch(call -> call.path().endsWith("/abandonUserCoupon"));
    }

    @Test
    void invalidRejectsEveryImmutablePersistedReceiptMismatchBeforeAbandon() {
        for (MinisoFulfillmentReceipt local : List.of(
                new MinisoFulfillmentReceipt(ORDER, ORG, 123, "OTHER-COUPON", PHONE,
                        "2026-10-01 00:00:00", "2026-12-01 00:00:00", 0),
                new MinisoFulfillmentReceipt(ORDER, ORG, 123, COUPON, PHONE,
                        "2026-10-02 00:00:00", "2026-12-01 00:00:00", 0),
                new MinisoFulfillmentReceipt(ORDER, ORG, 123, COUPON, PHONE,
                        "2026-10-01 00:00:00", "2026-12-02 00:00:00", 0),
                new MinisoFulfillmentReceipt("OTHER-ORDER", ORG, 123, COUPON, PHONE,
                        "2026-10-01 00:00:00", "2026-12-01 00:00:00", 0),
                new MinisoFulfillmentReceipt(ORDER, "OTHER-ORG", 123, COUPON, PHONE,
                        "2026-10-01 00:00:00", "2026-12-01 00:00:00", 0),
                new MinisoFulfillmentReceipt(ORDER, ORG, 124, COUPON, PHONE,
                        "2026-10-01 00:00:00", "2026-12-01 00:00:00", 0),
                new MinisoFulfillmentReceipt(ORDER, ORG, 123, COUPON, "13900139000",
                        "2026-10-01 00:00:00", "2026-12-01 00:00:00", 0))) {
            reply(envelope(coupon(0)));
            reply("{\"code\":200,\"data\":{\"succeed\":[{\"couponCode\":\"COUPON-1\",\"msg\":\"成功\"}],\"failed\":[]}}");
            assertUnknown(adapter(Optional.of(local)).invalid(invalid()));
            assertThat(calls).noneMatch(call -> call.path().endsWith("/abandonUserCoupon"));
            replies.clear();
            calls.clear();
        }
    }

    @Test
    void staleRemoteUnusedCannotDowngradePersistedUsedExpiredOrVoidBeforeAbandon() {
        for (int localStatus : List.of(1, 2, 3)) {
            reply(envelope(coupon(0)));
            reply("{\"code\":200,\"data\":{\"succeed\":[{\"couponCode\":\"COUPON-1\",\"msg\":\"成功\"}],\"failed\":[]}}");
            assertUnknown(adapter(Optional.of(localReceipt(localStatus))).invalid(invalid()));
            assertThat(calls).noneMatch(call -> call.path().endsWith("/abandonUserCoupon"));
            replies.clear();
            calls.clear();
        }
    }

    @Test
    void samePersistedCouponWithRemoteVoidRecoversForEveryLocalStatus() {
        for (int localStatus : List.of(0, 1, 2, 3)) {
            reply(envelope(coupon(3)));
            assertSuccess(adapter(Optional.of(localReceipt(localStatus))).invalid(invalid()), 3);
            assertThat(calls.remove().path()).endsWith("/queryGrand");
            assertThat(calls).isEmpty();
        }
    }

    @Test
    void invalidCannotTreatPartialForeignDuplicateOrMissingResultAsSuccess() {
        for (String data : List.of("{}", "{\"succeed\":[{\"couponCode\":\"COUPON-1\",\"msg\":\"成功\"}]}",
                "{\"succeed\":[],\"failed\":[\"COUPON-1\"]}",
                "{\"succeed\":[{\"couponCode\":\"FOREIGN\",\"msg\":\"成功\"}],\"failed\":[]}",
                "{\"succeed\":[{\"couponCode\":\"COUPON-1\",\"msg\":\"成功\"},{\"couponCode\":\"COUPON-1\",\"msg\":\"成功\"}],\"failed\":[]}",
                "{\"succeed\":[{\"couponCode\":\"COUPON-1\",\"msg\":\"成功\"}],\"failed\":[\"FOREIGN\"]}",
                "{\"succeed\":\"COUPON-1\",\"failed\":[]}",
                "{\"succeed\":[\"COUPON-1\"],\"failed\":[]}",
                "{\"succeed\":[{\"couponCode\":\"COUPON-1\"}],\"failed\":[]}")) {
            reply(envelope(coupon(0)));
            reply("{\"code\":200,\"data\":" + data + "}");
            assertUnknown(adapter().invalid(invalid()));
        }
    }

    @Test
    void alreadyVoidRecoversWithoutRepeatingAbandonAndUsedOrExpiredNeverCancel() {
        for (int status : List.of(3, 1, 2)) {
            reply(envelope(coupon(status)));
            SupplierGatewayResponse response = adapter().invalid(invalid());
            if (status == 3) assertSuccess(response, 3); else assertUnknown(response);
            assertThat(calls.remove().path()).endsWith("/queryGrand");
            assertThat(calls).isEmpty();
        }
    }

    @Test
    void interruptedRequestRestoresFlagAndReturnsUnknown() {
        Thread.currentThread().interrupt();
        assertUnknown(adapter().query(query()));
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    void unavailableTransportReturnsUnknown() {
        server.stop(0);
        assertUnknown(adapter().query(query()));
    }

    private MinisoRechargeSupplierGatewayAdapter adapter() {
        return adapter(Optional.of(localReceipt(0)));
    }

    private MinisoRechargeSupplierGatewayAdapter adapter(Optional<MinisoFulfillmentReceipt> local) {
        return new MinisoRechargeSupplierGatewayAdapter(client, new MinisoOrderSnapshotReader() {
            @Override
            public Optional<MinisoOrderSnapshot> find(String orgCode, String orderId) {
                return Optional.of(new MinisoOrderSnapshot(ORG, ORDER, PHONE, "123", 1));
            }

            @Override
            public Optional<MinisoFulfillmentReceipt> localReceipt(String orgCode, String orderId) {
                assertThat(orgCode).isEqualTo(ORG);
                assertThat(orderId).isEqualTo(ORDER);
                return local;
            }
        });
    }

    private static MinisoFulfillmentReceipt localReceipt(int status) {
        return new MinisoFulfillmentReceipt(ORDER, ORG, 123, COUPON, PHONE,
                "2026-10-01 00:00:00", "2026-12-01 00:00:00", status);
    }

    private static SupplierDispatchGatewayRequest dispatch(String channel) {
        return new SupplierDispatchGatewayRequest("EVENT", ORDER, "OUT", ORG, PHONE, "P", "CHILD", channel, 100,
                Map.of("batchId", "999", "number", 10));
    }
    private static SupplierQueryGatewayRequest query() {
        return new SupplierQueryGatewayRequest("EVENT", ORDER, "OUT", ORG, PHONE, "MINISO", Map.of());
    }
    private static SupplierInvalidGatewayRequest invalid() {
        return new SupplierInvalidGatewayRequest("EVENT", ORDER, "OUT", ORG, PHONE, "MINISO", Map.of());
    }
    private void reply(String body) { replies.add(new Reply(200, body)); }
    private static String envelope(String coupon) { return "{\"code\":200,\"data\":[" + coupon + "]}"; }
    private static String coupon(Integer status) {
        return "{\"batchId\":123,\"phone\":\"" + PHONE + "\",\"couponCode\":\"" + COUPON
                + "\",\"beginTime\":\"2026-10-01 00:00:00\",\"endTime\":\"2026-12-01 00:00:00\""
                + (status == null ? "" : ",\"status\":" + status) + "}";
    }
    private static void assertUnknown(SupplierGatewayResponse response) {
        assertThat(response.status()).isEqualTo(SupplierGatewayStatus.RETRYABLE_FAILED);
        assertThat(response.minisoReceipt()).isNull();
    }
    private static void assertSuccess(SupplierGatewayResponse response, int status) {
        assertThat(response.status()).isEqualTo(SupplierGatewayStatus.SUCCESS);
        assertThat(response.minisoReceipt()).isNotNull();
        assertThat(response.minisoReceipt().couponStatus()).isEqualTo(status);
        assertThat(response.minisoReceipt().couponCode()).isEqualTo(COUPON);
        assertThat(response.minisoReceipt().phone()).isEqualTo(PHONE);
        assertThat(response.minisoReceipt().batchId()).isEqualTo(123);
    }
    private record Reply(int status, String body) { }
    private record Call(String path, JsonNode body) { }
}
