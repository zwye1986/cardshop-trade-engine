package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import java.util.Objects;
import java.util.Optional;

/**
 * Redis 优先、MySQL 兜底的查单组合端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。Redis 是受理期短期事实来源，
 * MySQL 是最终事实来源；因此查单应先读 Redis 快照，Redis 未命中时再兜底读取 `mc_order`，避免已落库订单在
 * Redis TTL 或修复窗口后被误报为不存在。
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
        if (primarySnapshot.isPresent()) {
            return primarySnapshot;
        }
        return fallback.findByOrgCodeAndOutOrderSn(orgCode, outOrderSn);
    }
}
