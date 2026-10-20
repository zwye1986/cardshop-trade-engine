package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.notify.MerchantNotifyOperation;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * 交行 2104 业务正文编码器，不承担开放平台 form 或签名编码。
 *
 * <p>参见 docs/交行直充业务系统详细设计.md「本次交行回调对接确认」及「交行 2104 通知实施细化」。
 * 只允许通知类型定义的字段，空字符串原样保留，金额以整数分输出；重试应发送持久化的原文，不能重新调用此编码器改写。
 * 银行未定义逗号转义协议，因此拒绝违反“值不含逗号”约束的数据，不截断、不自行转义。
 */
public final class Bocom2104PayloadCodec {
    public static final int MAX_CHARACTERS = 2500;
    private static final String OPERATE_TYPE = "operate_type";
    private static final String FIELD_SEPARATOR = ",";
    private static final String KEY_VALUE_SEPARATOR = ":";
    private static final List<String> ORDER_FIELDS = List.of(
            "order_id", "out_order_sn", "total_price", "type", "total_settle_price",
            "timestamp", "nonce_str", "order_status", "account");
    private static final List<String> VOUCHER_FIELDS = List.of(
            "order_id", "out_order_sn", "timestamp", "nonce_str", "account",
            "use_time", "state_type", "use_state", "invalid_time");
    private static final Set<String> AMOUNT_FIELDS = Set.of("total_price", "total_settle_price");

    /** 首次创建任务时从完整业务快照编码，白名单顺序不受 Map 迭代顺序影响。 */
    public String encode(MerchantNotifyOperation operation, Map<String, ?> fields) {
        Objects.requireNonNull(operation, "notification operation required");
        Objects.requireNonNull(fields, "notification fields required");
        List<String> expected = operation == MerchantNotifyOperation.ORDER_STATUS ? ORDER_FIELDS : VOUCHER_FIELDS;
        if (fields.size() != expected.size() || !fields.keySet().containsAll(expected)) {
            // 不包含字段值，避免 account/未知敏感字段进入异常日志。
            throw new IllegalArgumentException("bank notification fields do not match operation schema");
        }
        StringJoiner encoded = new StringJoiner(FIELD_SEPARATOR);
        encoded.add(OPERATE_TYPE + KEY_VALUE_SEPARATOR + operation.code());
        for (String name : expected) {
            String value = wireValue(name, fields.get(name));
            if (value.contains(FIELD_SEPARATOR)) {
                throw new IllegalArgumentException("bank notification value contains unsupported delimiter");
            }
            encoded.add(name + KEY_VALUE_SEPARATOR + value);
        }
        String result = encoded.toString();
        validate(result);
        return result;
    }

    /** 按 Unicode 码点限制真实正文，拒绝不可无损 UTF-8 编码的孤立代理项。 */
    public void validate(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("bank notification content required");
        }
        for (int index = 0; index < text.length(); index++) {
            char current = text.charAt(index);
            if (Character.isHighSurrogate(current)) {
                if (++index >= text.length() || !Character.isLowSurrogate(text.charAt(index))) {
                    throw new IllegalArgumentException("invalid Unicode in bank notification");
                }
            } else if (Character.isLowSurrogate(current)) {
                throw new IllegalArgumentException("invalid Unicode in bank notification");
            }
        }
        if (text.codePointCount(0, text.length()) > MAX_CHARACTERS) {
            throw new IllegalArgumentException("bank notification exceeds character limit");
        }
    }

    private static String wireValue(String name, Object value) {
        if (AMOUNT_FIELDS.contains(name)) {
            if (!(value instanceof Byte || value instanceof Short || value instanceof Integer
                    || value instanceof Long || value instanceof BigInteger)
                    || new BigInteger(value.toString()).signum() < 0) {
                throw new IllegalArgumentException("bank notification amount must be nonnegative integer cents");
            }
            return value.toString();
        }
        if (!(value instanceof String text)) {
            throw new IllegalArgumentException("bank notification text field missing or invalid");
        }
        return text;
    }
}
