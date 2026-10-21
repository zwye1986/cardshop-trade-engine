package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 可信名创凭据到旧卡管卡密数组的兼容投影，不产生新的履约事实。
 * <p>参见《交行直充业务系统详细设计》「名创卡密导出兼容修复（2026-10-21）」。
 * couponKey沿用旧名创实现的券码明文约定，有效期原样保存；已有卡密冲突时禁止覆盖。
 */
public final class MinisoLegacyOrderData {
    private static final int MAX_DATA_CHARACTERS = 65_536;
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private MinisoLegacyOrderData() { }

    /** 只接受已校验归属的凭据；本方法不接收外部传入的卡密JSON。 */
    public static String fromReceipt(MinisoFulfillmentReceipt receipt) {
        try {
            return JSON.writeValueAsString(List.of(Map.of(
                    "couponCode", receipt.couponCode(),
                    "couponKey", receipt.couponCode(),
                    "validTime", receipt.endTime())));
        } catch (IOException failure) {
            // 立即安全包装重抛，Jackson异常可能包含券码，不能把原cause带到Worker日志。
            throw new IllegalStateException("LEGACY_DATA_SERIALIZATION_FAILED");
        }
    }

    /** 空白可补齐；已有非空数组必须与可信凭据一致，保留其原始字节及额外展示字段。 */
    public static void requireCompatible(String existing, MinisoFulfillmentReceipt receipt) {
        if (existing == null || existing.isBlank()) return;
        if (existing.length() > MAX_DATA_CHARACTERS) throw conflict();
        try {
            var rows = JSON.readTree(existing);
            var expected = JSON.readTree(fromReceipt(receipt)).get(0);
            if (rows == null || !rows.isArray() || rows.size() != 1 || !rows.get(0).isObject()) throw conflict();
            for (var fields = expected.fields(); fields.hasNext();) {
                var field = fields.next();
                if (!field.getValue().equals(rows.get(0).get(field.getKey()))) throw conflict();
            }
        } catch (IOException failure) {
            // 立即安全包装重抛；JSON解析异常携带原始输入，不能泄漏卡密。
            throw conflict();
        }
    }

    private static IllegalStateException conflict() {
        return new IllegalStateException("LEGACY_DATA_CONFLICT");
    }
}
