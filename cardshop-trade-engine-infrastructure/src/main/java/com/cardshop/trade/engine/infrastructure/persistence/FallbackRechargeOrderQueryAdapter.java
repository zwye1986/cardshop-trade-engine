package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import com.cardshop.trade.engine.domain.order.PersistStatus;
import java.util.Objects;
import java.util.Optional;

/**
 * Redis 受理期优先、已持久化订单读取 MySQL 最终事实的查单组合端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。Redis 是受理期短期事实来源，
 * MySQL 是最终事实来源；Redis 未持久化时保留受理期快照，已持久化时优先返回同一订单的 MySQL 事实，
 * 避免 Redis 旧受理状态遮蔽履约终态。Redis 未命中仍由 MySQL 兜底，MySQL 缺失或身份不一致时保守保留
 * 已有 Redis 快照，不能将暂时不可见的最终事实解释为订单失败或不存在。
 */
public class FallbackRechargeOrderQueryAdapter implements RechargeOrderQueryPort {

    private final RechargeOrderQueryPort primary;
    private final RechargeOrderQueryPort fallback;

    public FallbackRechargeOrderQueryAdapter(
            RechargeOrderQueryPort primary,
            RechargeOrderQueryPort fallback) {
        this.primary = Objects.requireNonNull(primary, "primary must not be null");
        this.fallback = Objects.requireNonNull(fallback, "fallback must not be null");
    }

    @Override
    public Optional<RechargeOrderSnapshot> findByOrgCodeAndOutOrderSn(String orgCode, String outOrderSn) {
        Optional<RechargeOrderSnapshot> primarySnapshot = primary.findByOrgCodeAndOutOrderSn(orgCode, outOrderSn);
        if (primarySnapshot.isEmpty()) {
            return fallback.findByOrgCodeAndOutOrderSn(orgCode, outOrderSn);
        }
        RechargeOrderSnapshot redisSnapshot = primarySnapshot.orElseThrow();
        if (!PersistStatus.DB_PERSISTED.name().equals(redisSnapshot.persistStatus())) {
            return primarySnapshot;
        }
        // 两端都使用可信商户参数查询；再核对平台单号和请求外单号，防止错误快照覆盖本单事实。参见详细设计「3. API/V1 接入层」。
        return fallback.findByOrgCodeAndOutOrderSn(orgCode, outOrderSn)
                .filter(mysqlSnapshot -> sameOrder(redisSnapshot, mysqlSnapshot, outOrderSn))
                .or(() -> primarySnapshot);
    }

    private static boolean sameOrder(
            RechargeOrderSnapshot redisSnapshot, RechargeOrderSnapshot mysqlSnapshot, String outOrderSn) {
        return redisSnapshot.orderId() != null
                && !redisSnapshot.orderId().isBlank()
                && redisSnapshot.orderId().equals(mysqlSnapshot.orderId())
                && outOrderSn != null
                && !outOrderSn.isBlank()
                && outOrderSn.equals(redisSnapshot.outOrderSn())
                && outOrderSn.equals(mysqlSnapshot.outOrderSn());
    }
}
