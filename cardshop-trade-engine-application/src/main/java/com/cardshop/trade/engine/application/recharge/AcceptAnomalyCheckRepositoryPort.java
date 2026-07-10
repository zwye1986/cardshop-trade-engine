package com.cardshop.trade.engine.application.recharge;

import java.util.List;

/**
 * Redis 接单异常自动核查仓储端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「14.3 MySQL 表字段」和
 * 「6. Redis 受理与缓存层」。application 层通过该端口领取待核查异常并推进核查状态，
 * 不直接依赖 `recharge_accept_anomaly` 表结构或 MyBatis。
 */
public interface AcceptAnomalyCheckRepositoryPort {

    /**
     * 读取一批仍需核查的异常记录。
     *
     * <p>当前 D5B 表结构尚未包含 `locked_until`/worker 租约字段，因此实现必须控制批量大小，并由外层
     * XXL-Job 分片降低并发重复扫描风险。后续补齐租约字段后可在该端口内部升级为 CAS claim。
     */
    List<AcceptAnomalyCase> findOpenForCheck(int limit);

    /**
     * 标记进入自动核查中，便于运营区分 INIT 与正在处理的记录。
     */
    void markChecking(String anomalyId, String workerId);

    /**
     * 核查证明订单已被 Redis 或 MySQL 接受。
     */
    void markResolvedAccepted(String anomalyId, AcceptAnomalyCheckEvidence evidence, String workerId);

    /**
     * 核查证明订单未被接受，允许同一商户订单号后续重新下单。
     */
    void markResolvedNotAccepted(String anomalyId, AcceptAnomalyCheckEvidence evidence, String workerId);

    /**
     * 自动核查发现证据冲突或证据不足，转人工处理。
     */
    void markManualRequired(
            String anomalyId,
            AcceptAnomalyCheckEvidence evidence,
            String reason,
            String workerId);
}
