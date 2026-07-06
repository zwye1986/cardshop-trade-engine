package com.cardshop.trade.engine.application.recharge;

/**
 * RabbitMQ 持久化消息 publisher confirm 结果。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.5 下单服务实现细节」
 * 和「20.9 RabbitMQ 声明契约」。confirm 成功只表示 broker 已接收消息，MySQL 落库仍由 Consumer 异步完成。
 *
 * @param confirmed broker 是否确认接收
 * @param messageId 持久化消息 ID
 * @param attemptCount 本次接口线程内发布尝试次数
 * @param failCode 发布失败码
 * @param failReason 发布失败原因
 */
public record PersistMessagePublishResult(
        boolean confirmed,
        String messageId,
        int attemptCount,
        String failCode,
        String failReason) {

    public PersistMessagePublishResult {
        messageId = messageId == null ? "" : messageId;
        attemptCount = Math.max(1, attemptCount);
        failCode = failCode == null ? "" : failCode;
        failReason = failReason == null ? "" : failReason;
    }

    public static PersistMessagePublishResult confirmed(String messageId, int attemptCount) {
        return new PersistMessagePublishResult(true, messageId, attemptCount, "", "");
    }

    public static PersistMessagePublishResult failed(
            String messageId,
            int attemptCount,
            String failCode,
            String failReason) {
        return new PersistMessagePublishResult(false, messageId, attemptCount, failCode, failReason);
    }

    public PersistMessagePublishResult withMessageId(String newMessageId) {
        return new PersistMessagePublishResult(confirmed, newMessageId, attemptCount, failCode, failReason);
    }
}
