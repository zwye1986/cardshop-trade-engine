package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.bouncycastle.crypto.digests.SM3Digest;

/**
 * V1 商户回调 SM3 签名器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.12 回调设计」。签名串沿用旧 V1
 * 协议的 ASCII 升序 {@code key=value,key=value} 规则，内部加入商户 {@code aesKey}，但响应 JSON
 * 只输出业务字段和 {@code sign}。
 */
public class V1MerchantNotifyPayloadSigner implements MerchantNotifyPayloadSigner {

    private final V1MerchantProfilePort merchantProfilePort;

    public V1MerchantNotifyPayloadSigner(V1MerchantProfilePort merchantProfilePort) {
        this.merchantProfilePort = Objects.requireNonNull(merchantProfilePort, "merchantProfilePort must not be null");
    }

    @Override
    public Map<String, String> signOrderStatus(RechargeFulfillmentOrder order, String orderStatus, Instant now) {
        V1MerchantProfile merchant = merchant(order.orgCode());
        String timestamp = String.valueOf(now.getEpochSecond());
        String nonce = nonce(order.orderId(), orderStatus, timestamp);
        Map<String, String> payload = orderedPayload();
        payload.put("orderId", normalize(order.orderId()));
        payload.put("outOrderSn", normalize(order.outOrderSn()));
        payload.put("totalPrice", String.valueOf(order.orderPrice()));
        payload.put("type", "");
        payload.put("totalSettlePrice", String.valueOf(order.orderPrice()));
        payload.put("timestamp", timestamp);
        payload.put("nonceStr", nonce);
        payload.put("orderStatus", normalize(orderStatus));
        payload.put("account", normalize(order.account()));
        payload.put("sign", sign(payload, merchant.aesKey()));
        return payload;
    }

    @Override
    public Map<String, String> signWriteOff(RechargeFulfillmentOrder order, Instant invalidTime) {
        V1MerchantProfile merchant = merchant(order.orgCode());
        String timestamp = String.valueOf(invalidTime.getEpochSecond());
        String nonce = nonce(order.orderId(), "writeoff", timestamp);
        Map<String, String> payload = orderedPayload();
        payload.put("orderId", normalize(order.orderId()));
        payload.put("outOrderSn", normalize(order.outOrderSn()));
        payload.put("timestamp", timestamp);
        payload.put("nonceStr", nonce);
        payload.put("account", normalize(order.account()));
        payload.put("useTime", "");
        payload.put("stateType", "1");
        payload.put("useState", "2");
        payload.put("invalidTime", timestamp);
        payload.put("sign", sign(payload, merchant.aesKey()));
        return payload;
    }

    private V1MerchantProfile merchant(String orgCode) {
        return merchantProfilePort.findByOrgCode(orgCode)
                .filter(V1MerchantProfile::enabled)
                .filter(profile -> profile.aesKey() != null && !profile.aesKey().isBlank())
                .orElseThrow(() -> new IllegalStateException("merchant notify signing profile not found: orgCode=" + orgCode));
    }

    private static Map<String, String> orderedPayload() {
        return new LinkedHashMap<>();
    }

    private static String nonce(String orderId, String bizStatus, String timestamp) {
        return digestHex(orderId + ":" + bizStatus + ":" + timestamp).substring(0, 32);
    }

    private static String sign(Map<String, String> payload, String aesKey) {
        TreeMap<String, String> params = new TreeMap<>();
        payload.forEach((key, value) -> {
            if (!"sign".equals(key)) {
                params.put(key, normalize(value));
            }
        });
        params.put("aesKey", normalize(aesKey));
        return digestHex(source(params));
    }

    private static String source(Map<String, String> params) {
        StringBuilder builder = new StringBuilder(128);
        params.forEach((key, value) -> builder.append(key).append('=').append(value).append(','));
        if (!builder.isEmpty()) {
            builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    private static String digestHex(String source) {
        byte[] input = source.getBytes(StandardCharsets.UTF_8);
        SM3Digest digest = new SM3Digest();
        digest.update(input, 0, input.length);
        byte[] output = new byte[digest.getDigestSize()];
        digest.doFinal(output, 0);
        return hex(output);
    }

    private static String hex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            builder.append(Character.forDigit((value >> 4) & 0x0F, 16));
            builder.append(Character.forDigit(value & 0x0F, 16));
        }
        return builder.toString();
    }

    private static String normalize(String value) {
        return value == null ? "" : value;
    }
}
