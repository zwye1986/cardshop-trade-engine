package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.*;

import com.cardshop.trade.engine.domain.notify.MerchantNotifyOperation;
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

    @Test
    void freezesOrderFieldsInProtocolOrderWithoutJsonOrBusinessSignature() {
        assertThat(codec.encode(MerchantNotifyOperation.ORDER_STATUS, orderFields()))
                .isEqualTo("operate_type:1,order_id:order-1,out_order_sn:merchant-1,total_price:1950,"
                        + "type:1,total_settle_price:1950,timestamp:1792440000000,nonce_str:abc,"
                        + "order_status:12,account:测试账号");
    }

    @ParameterizedTest
    @EnumSource(value = MerchantNotifyOperation.class, names = {"REDEEMED", "CANCELLED"})
    void preservesEmptyFieldsAndColonsInsideEventTimes(MerchantNotifyOperation operation) {
        var fields = voucherFields();
        fields.put("use_time", operation == MerchantNotifyOperation.REDEEMED ? "2026-10-20 12:34:56" : "");
        fields.put("invalid_time", operation == MerchantNotifyOperation.CANCELLED ? "2026-10-20 12:34:56" : "");
        fields.put("use_state", operation == MerchantNotifyOperation.REDEEMED ? "2" : "3");
        assertThat(codec.encode(operation, fields))
                .startsWith("operate_type:" + operation.code() + ",order_id:order-1,")
                .contains("use_time:" + fields.get("use_time") + ",state_type:3,use_state:" + fields.get("use_state"))
                .endsWith("invalid_time:" + fields.get("invalid_time"));
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
    void refusesCommaRatherThanInventingEscapes() {
        var fields = orderFields();
        fields.put("account", "not,a,single,value");
        assertThatIllegalArgumentException().isThrownBy(() -> codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields))
                .withMessageNotContaining("not,a,single,value");
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
    }

    @Test
    void refusesMalformedUnicodeAndEmptyContent() {
        for (String text : new String[] {"\uD800", "\uDC00", "\uD800x", "", " "}) {
            assertThatIllegalArgumentException().isThrownBy(() -> codec.validate(text));
        }
    }

    @Test
    void validatesWholeEncodedPayloadIncludingFieldNamesAndDelimiters() {
        var fields = orderFields();
        fields.put("account", "中".repeat(2500));
        assertThatIllegalArgumentException().isThrownBy(() -> codec.encode(MerchantNotifyOperation.ORDER_STATUS, fields));
    }

    static Map<String, Object> orderFields() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("account", "测试账号"); // deliberately not in wire order
        fields.put("order_status", "12");
        fields.put("nonce_str", "abc");
        fields.put("timestamp", "1792440000000");
        fields.put("total_settle_price", 1950L);
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
