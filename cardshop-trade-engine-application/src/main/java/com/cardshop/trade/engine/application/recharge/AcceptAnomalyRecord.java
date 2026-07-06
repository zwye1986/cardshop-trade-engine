package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Redis 接单状态未知异常登记请求。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」中的
 * `recharge_accept_anomaly` 流程。该对象只携带后续核查所需的非敏感上下文，不能写入原始请求体、密钥、
 * token 或手机号等敏感载荷；后续核查任务应基于这里的 `orgCode/outOrderSn/orderId/requestHash` 再回查
 * Redis、RabbitMQ 和 MySQL。
 *
 * @param anomalyId 异常记录唯一 ID，调用方应使用同一订单同一异常类型的稳定 ID，避免重复登记
 * @param anomalyType 异常类型，例如 WAIT 失败且 cancel 未确认
 * @param orgCode 商户编码
 * @param outOrderSn 商户订单号
 * @param orderId 平台订单号；Lua 状态未知时可能为空
 * @param requestHash 标准化请求摘要，用于核查重复请求是否为同一业务意图
 * @param persistMsgId RabbitMQ 持久化消息 ID；WAIT/cancel 阶段通常为空
 * @param rawContext 受理异常原始上下文，只放状态码、失败原因和版本等非敏感字段；其中
 * `balance_reserved`、`dispatch_allowed` 等开关字段按 JSON boolean 保存，D5B 核查任务读取时不能按字符串解析
 * @param lastError 最近一次登记时看到的失败原因
 */
public record AcceptAnomalyRecord(
        String anomalyId,
        AcceptAnomalyType anomalyType,
        String orgCode,
        String outOrderSn,
        String orderId,
        String requestHash,
        String persistMsgId,
        Map<String, Object> rawContext,
        String lastError) {

    public AcceptAnomalyRecord {
        if (!hasText(anomalyId)) {
            throw new IllegalArgumentException("anomalyId must not be blank");
        }
        anomalyType = Objects.requireNonNull(anomalyType, "anomalyType must not be null");
        if (!hasText(orgCode)) {
            throw new IllegalArgumentException("orgCode must not be blank");
        }
        if (!hasText(outOrderSn)) {
            throw new IllegalArgumentException("outOrderSn must not be blank");
        }
        orderId = normalize(orderId);
        requestHash = normalize(requestHash);
        persistMsgId = normalize(persistMsgId);
        rawContext = copyRawContext(rawContext);
        lastError = normalize(lastError);
    }

    /**
     * 拷贝核查上下文并冻结，防止调用方在登记后继续修改证据快照。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
     * `raw_context` 最终写入 JSON，允许保存“字段存在但暂时未知”的 null 值；因此不能使用
     * {@link Map#copyOf(Map)}，它会拒绝 null value 并在异常路径上反而丢失核查入口。
     */
    private static Map<String, Object> copyRawContext(Map<String, Object> rawContext) {
        if (rawContext == null || rawContext.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : rawContext.entrySet()) {
            if (!hasText(entry.getKey())) {
                throw new IllegalArgumentException("rawContext key must not be blank");
            }
            copy.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(copy);
    }

    private static String normalize(String value) {
        return value == null ? "" : value;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
