package com.cardshop.trade.engine.application.recharge;

import static com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason.*;

import com.cardshop.trade.engine.application.v1.crypto.*;

import java.security.SecureRandom;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/** 名创已核销通知独立签名器，不复用作废invalidTime语义；参见docs/交行直充业务系统详细设计.md「9.5」。 */
public final class V12RedemptionNotifyPayloadSigner {
    // 名创核销为3/2，作废为3/3，按用户最终确认保留通用券分类；参见详细设计9.6节。
    public static final String STATE_TYPE_GENERAL = "3";
    public static final String USE_STATE_REDEEMED = "2";
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
    private final V12SigningCredentialStorePort credentials;

    public V12RedemptionNotifyPayloadSigner(V12SigningCredentialStorePort credentials) {
        this.credentials = Objects.requireNonNull(credentials);
    }

    public Map<String, String> sign(
            RechargeFulfillmentOrder order, LocalDateTime useTime, Instant now) {
        var key =
                credentials
                        .findCurrent(order.orgCode())
                        .filter(k -> k.orgCode().equals(order.orgCode()))
                        .orElseThrow(() -> new MinisoWriteOffException(MERCHANT_KEY_UNAVAILABLE));
        byte[] nonce = new byte[16];
        RANDOM.nextBytes(nonce);
        Map<String, String> payload = new LinkedHashMap<>();
        payload.put("order_id", order.orderId());
        payload.put("out_order_sn", order.outOrderSn());
        payload.put("timestamp", Long.toString(now.toEpochMilli()));
        payload.put("nonce_str", HexFormat.of().formatHex(nonce));
        payload.put("account", order.account());
        payload.put("use_time", TIME.format(useTime));
        payload.put("state_type", STATE_TYPE_GENERAL);
        payload.put("use_state", USE_STATE_REDEEMED);
        payload.put("invalid_time", "");
        payload.put("keyId", key.keyId());
        payload.put(
                "sign",
                V12MessageSignatures.sign(
                        V12MessageSignatures.Operation.WRITEOFF_CALLBACK, payload, key.signKey()));
        return payload;
    }
}
