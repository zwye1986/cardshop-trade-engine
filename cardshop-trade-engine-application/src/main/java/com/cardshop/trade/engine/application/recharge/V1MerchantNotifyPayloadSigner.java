package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredential;
import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort;
import com.cardshop.trade.engine.application.v1.crypto.V12MessageSignatures;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.bouncycastle.crypto.digests.SM3Digest;

/**
 * V1.2商户回调签名器；对应docs/交行直充业务系统详细设计.md「3.1」「9.3 回调设计」。
 * 新回调任务使用商户当前未撤销密钥；普通重试保留已生成的签名原文，由发送边界检查历史密钥。
 * 密钥不绑定订单或token，不读取aesKey。
 * 对外字段采用协议下划线命名；空字符串仍参与认证，keyId与业务字段共同签名。
 */
public class V1MerchantNotifyPayloadSigner implements MerchantNotifyPayloadSigner {

    private static final String SIGN_FIELD = "sign";
    private static final String WRITE_OFF_NONCE_STATUS = "writeoff";
    private static final String WRITE_OFF_STATE_TYPE = "1";
    private static final String WRITE_OFF_USE_STATE_CANCELLED = "2";
    private static final int NONCE_HEX_LENGTH = 32;
    private static final String RECHARGE_TYPE = "1";
    private static final java.time.format.DateTimeFormatter CALLBACK_TIME = java.time.format.DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss").withZone(java.time.ZoneId.of("Asia/Shanghai"));

    private final V12SigningCredentialStorePort credentialStore;

    public V1MerchantNotifyPayloadSigner(V12SigningCredentialStorePort credentialStore) {
        this.credentialStore = Objects.requireNonNull(credentialStore);
    }

    @Override
    public Map<String, String> signOrderStatus(RechargeFulfillmentOrder order, String orderStatus, Instant now) {
        V12SigningCredential credential = currentCredential(order.orgCode());
        String timestamp = String.valueOf(now.getEpochSecond());
        String nonce = nonce(order.orderId(), orderStatus, timestamp);
        Map<String, String> payload = orderedPayload();
        payload.put("order_id", normalize(order.orderId()));
        payload.put("out_order_sn", normalize(order.outOrderSn()));
        payload.put("total_price", String.valueOf(order.orderPrice()));
        payload.put("type", RECHARGE_TYPE);
        payload.put("total_settle_price", String.valueOf(order.orderPrice()));
        payload.put("timestamp", timestamp);
        payload.put("nonce_str", nonce);
        payload.put("order_status", normalize(orderStatus));
        payload.put("account", normalize(order.account()));
        payload.put("keyId", credential.keyId());
        payload.put(SIGN_FIELD, V12MessageSignatures.sign(V12MessageSignatures.Operation.ORDER_CALLBACK, payload, credential.signKey()));
        return payload;
    }

    @Override
    public Map<String, String> signWriteOff(RechargeFulfillmentOrder order, Instant invalidTime) {
        V12SigningCredential credential = currentCredential(order.orgCode());
        String timestamp = String.valueOf(invalidTime.getEpochSecond());
        String nonce = nonce(order.orderId(), WRITE_OFF_NONCE_STATUS, timestamp);
        Map<String, String> payload = orderedPayload();
        payload.put("order_id", normalize(order.orderId()));
        payload.put("out_order_sn", normalize(order.outOrderSn()));
        payload.put("timestamp", timestamp);
        payload.put("nonce_str", nonce);
        payload.put("account", normalize(order.account()));
        payload.put("use_time", "");
        payload.put("state_type", WRITE_OFF_STATE_TYPE);
        payload.put("use_state", WRITE_OFF_USE_STATE_CANCELLED);
        payload.put("invalid_time", CALLBACK_TIME.format(invalidTime));
        payload.put("keyId", credential.keyId());
        payload.put(SIGN_FIELD, V12MessageSignatures.sign(V12MessageSignatures.Operation.WRITEOFF_CALLBACK, payload, credential.signKey()));
        return payload;
    }

    /** 按可信订单所属商户取当前密钥，防止存储实现错误造成跨商户签名；参见详细设计3.1。 */
    private V12SigningCredential currentCredential(String orgCode) {
        return credentialStore.findCurrent(orgCode)
                .filter(credential -> credential.orgCode().equals(orgCode))
                .orElseThrow(() -> new IllegalStateException("merchant current signing key missing or revoked"));
    }

    private static Map<String, String> orderedPayload() {
        return new LinkedHashMap<>();
    }

    private static String nonce(String orderId, String bizStatus, String timestamp) {
        return digestHex(orderId + ":" + bizStatus + ":" + timestamp).substring(0, NONCE_HEX_LENGTH);
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
