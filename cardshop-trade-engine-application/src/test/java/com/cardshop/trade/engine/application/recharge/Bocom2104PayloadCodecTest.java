package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.*;

import com.cardshop.trade.engine.domain.notify.MerchantNotifyOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/** 交行正文编码契约；参见详细设计「交行 2104 通知实施细化」。 */
class Bocom2104PayloadCodecTest {
    private final Bocom2104PayloadCodec codec = new Bocom2104PayloadCodec();
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String ORDER_JSON = """
            {"operate_type":"1","order_id":"order-1","out_order_sn":"merchant-1","total_price":1950,"type":"1","total_settle_price":1877,"timestamp":"1792440000000","nonce_str":"abc","order_status":"12","account":"测试账号"}""";

    @Test
    void freezesOrderFieldsAsJsonWithIntegerAmountsAndNoBusinessSignature() throws Exception {
        var actual = JSON.readTree(codec.encode(MerchantNotifyOperation.ORDER_STATUS, orderFields()));
        assertThat(actual).isEqualTo(JSON.readTree(ORDER_JSON));
        assertThat(actual.get("total_price").isIntegralNumber()).isTrue();
        assertThat(actual.get("total_settle_price").isIntegralNumber()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(value = MerchantNotifyOperation.class, names = {"REDEEMED", "CANCELLED"})
    void preservesEmptyFieldsAndColonsInsideEventTimes(MerchantNotifyOperation operation) throws Exception {
        var fields = voucherFields();
        fields.put("use_time", operation == MerchantNotifyOperation.REDEEMED ? "2026-10-20 12:34:56" : "");
        fields.put("invalid_time", operation == MerchantNotifyOperation.CANCELLED ? "2026-10-20 12:34:56" : "");
        fields.put("use_state", operation == MerchantNotifyOperation.REDEEMED ? "2" : "3");
        var expected = new LinkedHashMap<>(fields);
        expected.put("operate_type", operation.code());
        assertThat(JSON.readTree(codec.encode(operation, fields))).isEqualTo(JSON.valueToTree(expected));
    }

    @ParameterizedTest
    @ValueSource(strings = {"sign", "keyId", "route_snapshot", "operate_type"})
    void refusesUnknownOrInternalFields(String field) {
        var fields = orderFields();
        fields.put(field, "must-not-leak");
        assertThatIllegalArgumentException().isThrownBy(() -> codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields));
    }

    @Test
    void refusesMissingOrNullFieldWithoutSilentlyFillingIt() {
        var fields = orderFields();
        fields.remove("account");
        assertThatIllegalArgumentException().isThrownBy(() -> codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields));
        fields.put("account", null);
        assertThatIllegalArgumentException().isThrownBy(() -> codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields));
    }

    @Test
    void roundTripsJsonSpecialCharactersWithoutChangingFieldValues() throws Exception {
        var fields = orderFields();
        String account = "中😀,colon:quote\"slash\\line\n\t\u0000";
        fields.put("account", account);
        var actual = JSON.readTree(codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields));
        assertThat(actual.get("account").asText()).isEqualTo(account);
        assertThat(actual.size()).isEqualTo(10);
    }

    @Test
    void refusesFractionalNegativeOrTextAmounts() {
        for (Object value : new Object[] {new BigDecimal("19.50"), -1L, "1950", 1.95d}) {
            var fields = orderFields();
            fields.put("total_price", value);
            assertThatIllegalArgumentException().isThrownBy(() -> codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "中", "😀"})
    void countsCharactersNotUtf8BytesOrUtf16Units(String character) {
        assertThatCode(() -> codec.validate(character.repeat(2499))).doesNotThrowAnyException();
        assertThatCode(() -> codec.validate(character.repeat(2500))).doesNotThrowAnyException();
        assertThatIllegalArgumentException().isThrownBy(() -> codec.validate(character.repeat(2501)));
        int overhead = ORDER_JSON.replace("测试账号", "").length();
        var fields = orderFields();
        for (int length : new int[] {2499, 2500}) {
            fields.put("account", character.repeat(length - overhead));
            String encoded = codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields);
            assertThat(encoded.codePointCount(0, encoded.length())).isEqualTo(length);
        }
        fields.put("account", character.repeat(2501 - overhead));
        assertThatIllegalArgumentException().isThrownBy(() -> codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields));
    }

    @Test
    void refusesMalformedUnicodeAndEmptyContent() {
        for (String text : new String[] {"\uD800", "\uDC00", "\uD800x", "", " "}) {
            assertThatIllegalArgumentException().isThrownBy(() -> codec.validate(text));
        }
        for (String text : new String[] {"\uD800", "\uDC00", "\uD800x"}) {
            var fields = orderFields();
            fields.put("account", text);
            assertThatIllegalArgumentException().isThrownBy(() -> codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields));
        }
    }

    @Test
    void validatesWholeEncodedPayloadIncludingFieldNamesAndDelimiters() {
        var fields = orderFields();
        fields.put("account", "中".repeat(2500));
        assertThatIllegalArgumentException().isThrownBy(() -> codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields));
        fields.put("account", "\"".repeat(1200)); // JSON转义后超限，原字段值未超限。
        assertThatIllegalArgumentException().isThrownBy(() -> codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields));
    }

    static Map<String, Object> orderFields() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("account", "测试账号"); // deliberately not in wire order
        fields.put("order_status", "12");
        fields.put("nonce_str", "abc");
        fields.put("timestamp", "1792440000000");
        fields.put("total_settle_price", 1877L);
        fields.put("type", "1");
        fields.put("total_price", 1950L);
        fields.put("out_order_sn", "merchant-1");
        fields.put("order_id", "order-1");
        return fields;
    }

    static Map<String, Object> voucherFields() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("order_id", "order-1");
        fields.put("out_order_sn", "merchant-1");
        fields.put("timestamp", "1792440000000");
        fields.put("nonce_str", "abc");
        fields.put("account", "测试账号");
        fields.put("use_time", "");
        fields.put("state_type", "3");
        fields.put("use_state", "2");
        fields.put("invalid_time", "");
        return fields;
    }
}
