package com.cardshop.trade.engine.application.recharge;

import java.util.Map;
import java.util.Objects;

/**
 * Redis pending 中等待 MQ 补投的订单快照。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.11 XXL-Job Handler 契约」
 * 和 {@code docs/交行直充业务系统高可用方案设计.md}「17.3 MQ 补投任务」。补投服务只依赖该最小快照
 * 重新构造 {@link PersistTradeOrderMessage}，Redis org_index、claim 锁和二次确认留给基础设施端口实现。
 *
 * @param messageId 补投使用的 MQ 消息幂等号
 * @param orgCode 商户编码
 * @param orderId 平台订单号
 * @param outOrderSn 商户外部订单号
 * @param requestHash 原始请求摘要
 * @param orderVersion Redis 订单版本
 * @param initialOrderSnapshot MQ initial snapshot 兜底载荷
 */
public record PendingPersistOrderSnapshot(
        String messageId,
        String orgCode,
        String orderId,
        String outOrderSn,
        String requestHash,
        long orderVersion,
        Map<String, Object> initialOrderSnapshot) {

    public PendingPersistOrderSnapshot {
        messageId = requireText(messageId, "messageId");
        orgCode = requireText(orgCode, "orgCode");
        orderId = requireText(orderId, "orderId");
        outOrderSn = requireText(outOrderSn, "outOrderSn");
        requestHash = requireText(requestHash, "requestHash");
        if (orderVersion <= 0) {
            throw new IllegalArgumentException("orderVersion must be positive");
        }
        initialOrderSnapshot = Map.copyOf(Objects.requireNonNull(initialOrderSnapshot, "initialOrderSnapshot"));
    }

    PersistTradeOrderMessage toPersistMessage() {
        return new PersistTradeOrderMessage(
                messageId,
                orgCode,
                orderId,
                outOrderSn,
                requestHash,
                orderVersion,
                initialOrderSnapshot);
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
