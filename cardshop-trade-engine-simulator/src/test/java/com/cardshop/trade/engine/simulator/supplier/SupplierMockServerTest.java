package com.cardshop.trade.engine.simulator.supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

class SupplierMockServerTest {

    private static final String ORDER_ID = "order-http-001";
    private static final String SUPPLIER_REQUEST_ID = "dispatch-order-http-001";

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void bothDispatchPathsReturnProcessingForNewOrder() throws Exception {
        SupplierMockOrderStore store = immediateSuccessStore();
        try (SupplierMockServer server = SupplierMockServer.start("127.0.0.1", 0, store)) {
            URI baseUri = server.baseUri();

            JsonNode dispatch = parseJson(post(baseUri.resolve("/supplier/dispatch"),
                    dispatchBody(ORDER_ID)).body());
            assertThat(dispatch.get("status").asText()).isEqualTo("PROCESSING");
            assertThat(dispatch.get("code").asText()).isEqualTo("202");
            assertThat(dispatch.get("supplier_order_id").asText()).isNotBlank();

            String minisoOrderId = ORDER_ID + "-miniso";
            JsonNode minisoDispatch = parseJson(post(baseUri.resolve("/tripartite/coupon/batchGrandCoupon"),
                    dispatchBody(minisoOrderId)).body());
            assertThat(minisoDispatch.get("status").asText()).isEqualTo("PROCESSING");
            assertThat(minisoDispatch.get("code").asText()).isEqualTo("202");
            assertThat(minisoDispatch.get("supplier_order_id").asText()).isNotBlank();
        }
    }

    @Test
    void queryPathsReturnSuccessAfterDispatchWithZeroDelay() throws Exception {
        SupplierMockOrderStore store = immediateSuccessStore();
        try (SupplierMockServer server = SupplierMockServer.start("127.0.0.1", 0, store)) {
            URI baseUri = server.baseUri();
            JsonNode dispatch = parseJson(post(baseUri.resolve("/supplier/dispatch"),
                    dispatchBody(ORDER_ID)).body());
            String supplierOrderId = dispatch.get("supplier_order_id").asText();

            JsonNode query = parseJson(post(baseUri.resolve("/supplier/query"),
                    queryBody(ORDER_ID)).body());
            assertThat(query.get("status").asText()).isEqualTo("SUCCESS");
            assertThat(query.get("code").asText()).isEqualTo("0");
            assertThat(query.get("supplier_order_id").asText()).isEqualTo(supplierOrderId);

            JsonNode minisoQuery = parseJson(post(baseUri.resolve("/tripartite/coupon/queryGrand"),
                    queryBody(ORDER_ID)).body());
            assertThat(minisoQuery.get("status").asText()).isEqualTo("SUCCESS");
            assertThat(minisoQuery.get("code").asText()).isEqualTo("0");
            assertThat(minisoQuery.get("supplier_order_id").asText()).isEqualTo(supplierOrderId);
        }
    }

    @Test
    void bodyWithoutOrderIdUsesSupplierRequestId() throws Exception {
        SupplierMockOrderStore store = immediateSuccessStore();
        try (SupplierMockServer server = SupplierMockServer.start("127.0.0.1", 0, store)) {
            URI baseUri = server.baseUri();
            String body = """
                    {"supplier_request_id":"%s","account":"13800138000"}
                    """.formatted(SUPPLIER_REQUEST_ID);

            JsonNode dispatch = parseJson(post(baseUri.resolve("/supplier/dispatch"), body).body());
            assertThat(dispatch.get("status").asText()).isEqualTo("PROCESSING");

            JsonNode query = parseJson(post(baseUri.resolve("/supplier/query"), body).body());
            assertThat(query.get("status").asText()).isEqualTo("SUCCESS");
            assertThat(query.get("supplier_order_id").asText()).isEqualTo(dispatch.get("supplier_order_id").asText());
        }
    }

    @Test
    void unknownPathReturns404() throws Exception {
        SupplierMockOrderStore store = immediateSuccessStore();
        try (SupplierMockServer server = SupplierMockServer.start("127.0.0.1", 0, store)) {
            HttpResponse<String> response = post(server.baseUri().resolve("/unknown/path"),
                    dispatchBody(ORDER_ID));
            assertThat(response.statusCode()).isEqualTo(404);
        }
    }

    /** Task19真实MINISO报文合同，参见详细设计9.8；不能用通用status假成功替代完整凭据。 */
    @Test
    void minisoIssueAndOriginalOrderQueryReturnSameCompleteReceipt() throws Exception {
        try (SupplierMockServer server = SupplierMockServer.start("127.0.0.1", 0, immediateSuccessStore())) {
            URI base = server.baseUri();
            JsonNode issued = parseJson(post(base.resolve("/tripartite/coupon/batchGrandCoupon"), minisoBody("m1")).body());
            assertThat(issued.path("code").isInt()).isTrue();
            assertThat(issued.path("code").asInt()).isEqualTo(200);
            JsonNode receipt = issued.path("data").get(0);
            assertThat(receipt.path("batchId").asInt()).isEqualTo(100023);
            assertThat(receipt.path("phone").asText()).isEqualTo("10000000000");
            assertThat(receipt.path("couponCode").asText()).startsWith("MOCK-MINISO-");
            assertThat(receipt.path("beginTime").asText()).isEqualTo("2026-01-01 00:00:00");
            assertThat(receipt.path("endTime").asText()).isEqualTo("2099-12-31 23:59:59");
            assertThat(receipt.path("status").asInt(-1)).isZero();
            assertThat(parseJson(post(base.resolve("/tripartite/coupon/queryGrand"), "{\"orderNo\":\"m1\"}").body())
                    .path("data")).isEqualTo(issued.path("data"));
            assertThat(parseJson(post(base.resolve("/tripartite/coupon/batchGrandCoupon"), minisoBody("m1")).body())
                    .path("data")).isEqualTo(issued.path("data"));
            JsonNode changed = parseJson(post(base.resolve("/tripartite/coupon/batchGrandCoupon"),
                    minisoBody("m1").replace("100023", "100024")).body());
            assertThat(changed.path("code").asInt()).isNotEqualTo(200);
        }
    }

    @Test
    void minisoUnknownIssuanceStillQueriesOriginalReceiptWithoutReissuing() throws Exception {
        try (SupplierMockServer server = SupplierMockServer.start("127.0.0.1", 0, immediateSuccessStore())) {
            URI base = server.baseUri();
            HttpRequest request = HttpRequest.newBuilder(base.resolve("/tripartite/coupon/batchGrandCoupon"))
                    .header("Content-Type", "application/json")
                    .header("X-Mock-Miniso-Scenario", "UNKNOWN_AFTER_ISSUE")
                    .POST(HttpRequest.BodyPublishers.ofString(minisoBody("uncertain"))).build();
            JsonNode unknown = parseJson(httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body());
            assertThat(unknown.path("code").asText()).isEqualTo("MOCK_RESULT_UNKNOWN");
            assertThat(unknown.path("data").isNull()).isTrue();
            JsonNode query = parseJson(post(base.resolve("/tripartite/coupon/queryGrand"), "{\"orderNo\":\"uncertain\"}").body());
            assertThat(query.path("code").asInt()).isEqualTo(200);
            assertThat(query.path("data").size()).isEqualTo(1);
            assertThat(parseJson(post(base.resolve("/tripartite/coupon/batchGrandCoupon"), minisoBody("uncertain")).body())
                    .path("data")).isEqualTo(query.path("data"));
        }
    }

    @Test
    void minisoMissingOrderIsUnknownAndQueryNeverCreatesCoupons() throws Exception {
        try (SupplierMockServer server = SupplierMockServer.start("127.0.0.1", 0, immediateSuccessStore())) {
            JsonNode missing = parseJson(post(server.baseUri().resolve("/tripartite/coupon/queryGrand"),
                    "{\"orderNo\":\"missing\"}").body());
            assertThat(missing.path("code").asText()).isEqualTo("MOCK_RESULT_UNKNOWN");
            assertThat(missing.path("data").isNull()).isTrue();
        }
    }

    @Test
    void minisoVoidIsExactIdempotentAndSupportsLegacyCommaSeparatedCodes() throws Exception {
        try (SupplierMockServer server = SupplierMockServer.start("127.0.0.1", 0, immediateSuccessStore())) {
            URI base = server.baseUri();
            JsonNode first = parseJson(post(base.resolve("/tripartite/coupon/batchGrandCoupon"), minisoBody("void1").replace("\"number\":1", "\"number\":2")).body());
            assertThat(first.path("code").asInt()).isEqualTo(200);
            assertThat(first.path("data").size()).isEqualTo(2);
            String coupon = first.path("data").get(0).path("couponCode").asText();
            String second = first.path("data").get(1).path("couponCode").asText();
            post(base.resolve("/tripartite/coupon/batchGrandCoupon"), minisoBody("keep2"));
            String request = objectMapper.writeValueAsString(java.util.Map.of("couponCodes", java.util.List.of(coupon, "foreign")));
            JsonNode mixed = parseJson(post(base.resolve("/tripartite/coupon/abandonUserCoupon"), request).body());
            assertThat(mixed.path("code").asInt()).isEqualTo(200);
            assertThat(mixed.path("data").path("succeed").get(0).path("couponCode").asText()).isEqualTo(coupon);
            assertThat(mixed.path("data").path("succeed").get(0).path("msg").asText()).isNotBlank();
            assertThat(mixed.path("data").path("failed").get(0).path("couponCode").asText()).isEqualTo("foreign");
            JsonNode repeat = parseJson(post(base.resolve("/tripartite/coupon/abandonUserCoupon"),
                    objectMapper.writeValueAsString(java.util.Map.of("couponCodes", coupon + "," + second))).body());
            assertThat(repeat.path("data").path("failed").isEmpty()).isTrue();
            assertThat(repeat.path("data").path("succeed").size()).isEqualTo(2);
            assertThat(parseJson(post(base.resolve("/tripartite/coupon/queryGrand"), "{\"orderNo\":\"void1\"}").body())
                    .path("data").get(0).path("status").asInt()).isEqualTo(3);
            assertThat(parseJson(post(base.resolve("/tripartite/coupon/queryGrand"), "{\"orderNo\":\"keep2\"}").body())
                    .path("data").get(0).path("status").asInt(-1)).isZero();
        }
    }

    @Test
    void minisoInvalidIssuanceAndDuplicateVoidCodesCannotReportSuccess() throws Exception {
        try (SupplierMockServer server = SupplierMockServer.start("127.0.0.1", 0, immediateSuccessStore())) {
            URI base = server.baseUri();
            for (String body : java.util.List.of(minisoBody("bad").replace("100023", "\"100023\""),
                    minisoBody("bad").replace("\"number\":1", "\"number\":0"), "{\"orderNo\":42}")) {
                assertThat(parseJson(post(base.resolve("/tripartite/coupon/batchGrandCoupon"), body).body())
                        .path("code").asInt()).isNotEqualTo(200);
            }
            HttpResponse<String> duplicate = post(base.resolve("/tripartite/coupon/abandonUserCoupon"),
                    "{\"couponCodes\":[\"duplicate\",\"duplicate\"]}");
            assertThat(duplicate.statusCode()).isEqualTo(200);
            JsonNode invalid = parseJson(duplicate.body());
            assertThat(invalid.path("code").asInt()).isNotEqualTo(200);
        }
    }

    @Test
    void minisoPrefixAndBank2104PathsCannotReportSuccess() throws Exception {
        try (SupplierMockServer server = SupplierMockServer.start("127.0.0.1", 0, immediateSuccessStore())) {
            assertThat(post(server.baseUri().resolve("/tripartite/coupon/batchGrandCoupon/extra"), minisoBody("bad")).statusCode())
                    .isEqualTo(404);
            assertThat(post(server.baseUri().resolve("/2104"), "{}").statusCode()).isEqualTo(404);
        }
    }

    private static String minisoBody(String orderNo) {
        return """
                {"orderNo":"%s","phone":"10000000000","batchList":[{"batchId":100023,"number":1}]}
                """.formatted(orderNo);
    }

    private static SupplierMockOrderStore immediateSuccessStore() {
        Clock clock = Clock.fixed(Instant.parse("2026-07-20T10:00:00Z"), ZoneId.of("UTC"));
        return new SupplierMockOrderStore(0L, 0L, 1.0, clock, delta -> 0L, () -> 0.0);
    }

    private static String dispatchBody(String orderId) {
        return """
                {"order_id":"%s","supplier_request_id":"dispatch-%s","account":"13800138000"}
                """.formatted(orderId, orderId);
    }

    private static String queryBody(String orderId) {
        return """
                {"order_id":"%s","supplier_request_id":"query-%s","out_order_sn":"OUT-1","supplier_channel":"FNK_CGS_BX"}
                """.formatted(orderId, orderId);
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
}
