package com.cardshop.trade.engine.application.v1;

import java.util.Optional;

/**
 * 直充订单查询端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.10 查单设计」。查单应 Redis 优先、
 * MySQL 兜底；D4 先接入 Redis 快照，MySQL 兜底在异步落库闭环完成后继续补齐。
 */
public interface RechargeOrderQueryPort {

    /**
     * 根据商户机构和商户订单号查找订单快照。
     */
    Optional<RechargeOrderSnapshot> findByOrgCodeAndOutOrderSn(String orgCode, String outOrderSn);
}
