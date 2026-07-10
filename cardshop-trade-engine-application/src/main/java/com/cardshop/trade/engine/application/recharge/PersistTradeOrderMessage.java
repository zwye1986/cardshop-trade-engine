package com.cardshop.trade.engine.application.recharge;

import java.util.Map;
import java.util.Objects;

/**
 * Redis 订单异步落 MySQL 的 RabbitMQ 消息。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「7. RabbitMQ 设计」。
 * 消息必须携带 initial snapshot，避免 Redis 在 MySQL 落库前发生不可恢复回滚时无法兜底落库。
 *
 * @param messageId RabbitMQ publisher confirm 关联 ID
 * @param orgCode 商户机构号
 * @param orderId 平台订单号
 * @param outOrderSn 商户订单号
 * @param requestHash 请求幂等摘要
 * @param orderVersion Redis 订单版本
 * @param initialOrderSnapshot Redis 接单后的最小订单快照
 */
public record PersistTradeOrderMessage(
        String messageId,
        String orgCode,
        String orderId,
        String outOrderSn,
        String requestHash,
        long orderVersion,
        Map<String, Object> initialOrderSnapshot) {

    public PersistTradeOrderMessage {
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

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
