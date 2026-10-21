package com.cardshop.trade.engine.application.recharge;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.security.SecureRandom;
import java.util.*;
import com.cardshop.trade.engine.domain.notify.MerchantNotifyOperation;
import com.cardshop.trade.engine.domain.order.VoucherUseState;
import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;

/** 三类通知统一准备边界；参见详细设计「交行 2104 通知实施细化」。 */
public final class MerchantNotifyPreparationService {
    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final String ORDER_SUCCESS=Integer.toString(LegacyOrderStatusCode.SUCCESS.code());
    private static final String ORDER_FAILED=Integer.toString(LegacyOrderStatusCode.FAILED.code());
    private static final String RECHARGE_TYPE="1";
    private final MerchantNotifyRoutePort routes;
    private final MerchantNotifyPayloadSigner direct;
    private final V12RedemptionNotifyPayloadSigner redemption;
    private final Clock clock;
    private final Bocom2104PayloadCodec codec = new Bocom2104PayloadCodec();

    public MerchantNotifyPreparationService(MerchantNotifyRoutePort routes, MerchantNotifyPayloadSigner direct,
            V12RedemptionNotifyPayloadSigner redemption, Clock clock) {
        this.routes = Objects.requireNonNull(routes);
        this.direct = Objects.requireNonNull(direct);
        this.redemption = redemption;
        this.clock = Objects.requireNonNull(clock);
    }

    public Optional<RechargeNotifyTaskDraft> orderStatus(RechargeFulfillmentOrder order, String status, int retries) {
        if (!ORDER_SUCCESS.equals(status) && !ORDER_FAILED.equals(status)) throw new IllegalArgumentException("invalid terminal order status");
        return prepare(order, MerchantNotifyOperation.ORDER_STATUS, "notify-order-" + order.orderId() + "-" + status,
                status, null, null, retries);
    }
    public Optional<RechargeNotifyTaskDraft> cancelled(RechargeFulfillmentOrder order, Instant time, int retries) {
        if (time == null) throw new IllegalArgumentException("cancellation fact time required");
        return prepare(order, MerchantNotifyOperation.CANCELLED, "notify-writeoff-" + order.orderId() + "-cancelled",
                "CANCELLED", null, time, retries);
    }
    public Optional<RechargeNotifyTaskDraft> redeemed(RechargeFulfillmentOrder order, String eventId,
            LocalDateTime time, int retries) {
        if (time == null || eventId == null || eventId.isBlank()) throw new IllegalArgumentException("redemption fact required");
        return prepare(order, MerchantNotifyOperation.REDEEMED,
                "notify-miniso-redeemed-" + MinisoWriteOffNotification.digest(order.orderId() + ":" + eventId),
                "REDEEMED", time, null, retries);
    }

    /** 仅在首次建任务时准备；持久任务重试发送旧原文，不重新取路由/密钥或生成nonce。 */
    private Optional<RechargeNotifyTaskDraft> prepare(RechargeFulfillmentOrder order, MerchantNotifyOperation operation,
            String taskId, String status, LocalDateTime useTime, Instant invalidTime, int retries) {
        Objects.requireNonNull(order);
        if (retries < 1) throw new IllegalArgumentException("positive notify retry limit required");
        var resolved = routes.resolve(order.orgCode(), order.notifyUrl());
        if (resolved.isEmpty()) return Optional.empty();
        var route = resolved.orElseThrow();
        if (!order.orgCode().equals(route.orgCode())) throw new IllegalArgumentException("notification route ownership mismatch");
        Instant now = clock.instant();
        Map<String, Object> payload;
        String encoded = null;
        if (route.channel() == NotifyDeliveryChannel.BOCOM_OPENAPI) {
            payload = bankFields(order, operation, status, now, useTime, invalidTime);
            encoded = codec.encode(operation, payload);
        } else {
            if (operation == MerchantNotifyOperation.REDEEMED) {
                if (redemption == null) throw new IllegalStateException("direct redemption signer unavailable");
                payload = new LinkedHashMap<>(redemption.sign(order, useTime, now));
            } else {
                payload = new LinkedHashMap<>(operation == MerchantNotifyOperation.ORDER_STATUS
                        ? direct.signOrderStatus(order, status, now) : direct.signWriteOff(order, invalidTime));
            }
        }
        return Optional.of(new RechargeNotifyTaskDraft(taskId,
                operation == MerchantNotifyOperation.ORDER_STATUS ? "ORDER_STATUS" : "VOUCHER_STATE",
                order.orgCode(), order.orderId(), order.outOrderSn(), route.gatewayUrl(), status,
                Collections.unmodifiableMap(payload), retries, LocalDateTime.ofInstant(now, ZONE),
                route.channel(), route.channel() == NotifyDeliveryChannel.BOCOM_OPENAPI ? route : null, encoded));
    }

    private Map<String, Object> bankFields(RechargeFulfillmentOrder order, MerchantNotifyOperation operation,
            String status, Instant now, LocalDateTime useTime, Instant invalidTime) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("order_id", order.orderId());
        fields.put("out_order_sn", order.outOrderSn());
        fields.put("timestamp", Long.toString(now.toEpochMilli()));
        byte[] random = new byte[16];
        RANDOM.nextBytes(random);
        fields.put("nonce_str", HexFormat.of().formatHex(random));
        fields.put("account", order.account());
        if (operation == MerchantNotifyOperation.ORDER_STATUS) {
            fields.put("total_price", order.orderPrice());
            fields.put("total_settle_price", order.orderPrice());
            fields.put("type", RECHARGE_TYPE);
            fields.put("order_status", status);
        } else {
            fields.put("use_time", useTime == null ? "" : TIME.format(useTime));
            fields.put("invalid_time", invalidTime == null ? "" : TIME.format(invalidTime.atZone(ZONE)));
            fields.put("state_type", VoucherUseState.stateTypeFor(order.supplierChannel()));
            fields.put("use_state", operation == MerchantNotifyOperation.REDEEMED
                    ? VoucherUseState.REDEEMED.code() : VoucherUseState.CANCELLED.code());
        }
        return fields;
    }
}
