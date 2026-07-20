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
