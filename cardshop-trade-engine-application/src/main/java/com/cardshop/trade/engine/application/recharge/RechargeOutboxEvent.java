package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Worker 处理用的 outbox 事件视图。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」。该对象只暴露执行路由、
 * CAS 版本和非敏感业务定位字段，避免 application 层依赖 MyBatis DO。
 *
 * @param eventId outbox 事件唯一 ID，对应 {@code recharge_outbox.event_id}
 * @param eventType 事件类型，对应 {@code recharge_outbox.event_type}
 * @param orgCode 商户编码，对应 {@code recharge_outbox.org_code}
 * @param orderId 平台订单号，对应 {@code recharge_outbox.order_id}
 * @param outOrderSn 商户外部订单号，对应 {@code recharge_outbox.out_order_sn}
 * @param payload 事件载荷 JSON，对应 {@code recharge_outbox.payload}
 * @param retryCount 已重试次数，对应 {@code recharge_outbox.retry_count}
 * @param maxRetryCount 最大重试次数，对应 {@code recharge_outbox.max_retry_count}
 * @param version CAS 版本号，对应 {@code recharge_outbox.version}
 * @param nextRetryTime 下次可执行时间，对应 {@code recharge_outbox.next_retry_time}
 * @param lastError 最近一次失败原因，对应 {@code recharge_outbox.last_error}
 */
public record RechargeOutboxEvent(
        String eventId,
        OutboxEventType eventType,
        String orgCode,
        String orderId,
        String outOrderSn,
        String payload,
        int retryCount,
        int maxRetryCount,
        long version,
        LocalDateTime nextRetryTime,
        String lastError) {

    public RechargeOutboxEvent {
        eventId = requireText(eventId, "eventId");
        eventType = Objects.requireNonNull(eventType, "eventType must not be null");
        orgCode = requireText(orgCode, "orgCode");
        orderId = requireText(orderId, "orderId");
        outOrderSn = requireText(outOrderSn, "outOrderSn");
        payload = payload == null ? "{}" : payload;
        nextRetryTime = Objects.requireNonNull(nextRetryTime, "nextRetryTime must not be null");
        lastError = lastError == null ? "" : lastError;
    }

    public RechargeOutboxEvent claimedCopy() {
        return new RechargeOutboxEvent(
                eventId,
                eventType,
                orgCode,
                orderId,
                outOrderSn,
                payload,
                retryCount,
                maxRetryCount,
                version + 1,
                nextRetryTime,
                lastError);
    }

    /**
     * 判断事件是否已经达到最大重试次数。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」。达到上限的事件不能继续被
     * Worker 执行业务动作，需要转入 {@code MANUAL_REQUIRED}，避免永久失败事件造成稳定重试和日志风暴。
     */
    public boolean retryExhausted() {
        return retryCount >= maxRetryCount;
    }

    /**
     * 判断当前这次失败是否会达到最大重试次数。
     */
    public boolean nextFailureReachesMaxRetry() {
        return retryCount + 1 >= maxRetryCount;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
