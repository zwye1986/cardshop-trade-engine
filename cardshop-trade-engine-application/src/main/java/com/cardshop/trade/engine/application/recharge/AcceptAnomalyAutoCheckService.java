package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis 接单异常自动核查服务。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.3 Redis HA Guard」和
 * 「20.13.6 Redis Lua 下单结果」。该服务只负责“证据足够时推进状态，证据冲突时转人工”的决策；
 * 不在 application 层直接访问 Redis hash、RabbitMQ broker 或 MySQL 表。
 */
public class AcceptAnomalyAutoCheckService {

    private static final Logger log = LoggerFactory.getLogger(AcceptAnomalyAutoCheckService.class);
    private static final String WORKER_FALLBACK = "accept-anomaly-auto-check";

    private final AcceptAnomalyCheckRepositoryPort repository;
    private final RechargeOrderQueryPort orderQueryPort;

    public AcceptAnomalyAutoCheckService(
            AcceptAnomalyCheckRepositoryPort repository,
            RechargeOrderQueryPort orderQueryPort) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.orderQueryPort = Objects.requireNonNull(orderQueryPort, "orderQueryPort must not be null");
    }

    /**
     * 核查一批接单异常。
     *
     * <p>D5B 当前使用 {@link RechargeOrderQueryPort} 读取 Redis 优先、MySQL 兜底的订单快照。该端口能够安全证明
     * “订单已存在”，但不能证明“Redis 一定不存在订单”，因为 Redis 查询失败可能被降级到 MySQL 兜底。
     * 因此查不到快照时先转 `MANUAL_REQUIRED`，等待 D7/D8 的显式 Redis/MQ probe 补齐后再自动判定未受理。
     *
     * <p>TODO(D7/D8): 增加显式 Redis order/idem/reservation/pending probe、RabbitMQ confirm/consumer 轨迹和
     * MySQL `mc_order/recharge_order_ext` 三方核查；只有三方都证明没有受理事实时，才允许标记
     * `RESOLVED_NOT_ACCEPTED` 并释放同一 `outOrderSn` 的重试。参见 docs/交行直充业务系统详细设计.md
     * 「20.13.6 Redis Lua 下单结果」的 `recharge_accept_anomaly` 处理流程。
     */
    public AcceptAnomalyCheckBatchResult checkBatch(int limit, String workerId) {
        if (limit <= 0) {
            return AcceptAnomalyCheckBatchResult.empty();
        }
        String actualWorkerId = hasText(workerId) ? workerId : WORKER_FALLBACK;
        int checked = 0;
        int resolvedAccepted = 0;
        int resolvedNotAccepted = 0;
        int manualRequired = 0;
        int failed = 0;

        for (AcceptAnomalyCase anomaly : repository.findOpenForCheck(limit)) {
            checked++;
            try {
                repository.markChecking(anomaly.anomalyId(), actualWorkerId);
                Optional<RechargeOrderSnapshot> snapshot = orderQueryPort.findByOrgCodeAndOutOrderSn(
                        anomaly.orgCode(),
                        anomaly.outOrderSn());
                AcceptAnomalyCheckEvidence evidence = evidence(anomaly, snapshot);
                String manualReason = manualReason(anomaly, snapshot);
                if (manualReason != null) {
                    repository.markManualRequired(anomaly.anomalyId(), evidence, manualReason, actualWorkerId);
                    manualRequired++;
                } else if (snapshot.isPresent()) {
                    repository.markResolvedAccepted(anomaly.anomalyId(), evidence, actualWorkerId);
                    resolvedAccepted++;
                }
            } catch (RuntimeException ex) {
                /*
                 * 自动核查失败不能在批处理中断后续异常，否则一个坏数据会阻塞整个商户的 Redis 写入恢复。
                 * 这里必须记录 application 视角的定位字段；底层 repository/query 只能记录局部异常，无法保证带上
                 * anomalyId、orgCode、outOrderSn 等后续人工核查必需的完整上下文。
                 */
                log.error(
                        "接单异常自动核查失败 anomalyId={}, anomalyType={}, orgCode={}, outOrderSn={}, orderId={}, workerId={}",
                        anomaly.anomalyId(),
                        anomaly.anomalyType(),
                        anomaly.orgCode(),
                        anomaly.outOrderSn(),
                        anomaly.orderId(),
                        actualWorkerId,
                        ex);
                failed++;
            }
        }

        return new AcceptAnomalyCheckBatchResult(
                checked,
                resolvedAccepted,
                resolvedNotAccepted,
                manualRequired,
                failed);
    }

    private static String manualReason(
            AcceptAnomalyCase anomaly,
            Optional<RechargeOrderSnapshot> snapshot) {
        if (snapshot.isPresent()
                && hasText(anomaly.orderId())
                && !anomaly.orderId().equals(snapshot.get().orderId())) {
            return "order_id mismatch: anomaly=%s, snapshot=%s"
                    .formatted(anomaly.orderId(), snapshot.get().orderId());
        }
        if (snapshot.isEmpty() && hasText(anomaly.persistMsgId())) {
            /*
             * persist_msg_id 非空说明消息可能已经进入 RabbitMQ；D5B 尚未接入 broker 轨迹查询，不能仅凭
             * Redis/MySQL 暂时查不到就自动判定未受理。
             */
            return "mq evidence not checked for persist_msg_id=%s".formatted(anomaly.persistMsgId());
        }
        if (snapshot.isEmpty()) {
            /*
             * 当前查询端口会在 Redis 读取失败时继续尝试 MySQL 兜底，因此 empty 不能证明 Redis 里没有订单。
             * 为避免错误释放同一 outOrderSn 的重试，这里保守转人工。
             */
            return "snapshot missing: D5B merged query cannot prove redis/mysql/mq all empty";
        }
        return null;
    }

    private static AcceptAnomalyCheckEvidence evidence(
            AcceptAnomalyCase anomaly,
            Optional<RechargeOrderSnapshot> snapshot) {
        Map<String, Object> redisResult = new LinkedHashMap<>();
        redisResult.put("checked", false);
        redisResult.put("reason", "D5B uses RechargeOrderQueryPort merged Redis/MySQL read path");

        Map<String, Object> mysqlResult = new LinkedHashMap<>();
        mysqlResult.put("org_code", anomaly.orgCode());
        mysqlResult.put("out_order_sn", anomaly.outOrderSn());
        mysqlResult.put("found", snapshot.isPresent());
        snapshot.ifPresent(order -> {
            mysqlResult.put("order_id", order.orderId());
            mysqlResult.put("order_status", order.orderStatus());
            mysqlResult.put("persist_status", order.persistStatus());
            mysqlResult.put("validation_status", order.validationStatus());
        });

        Map<String, Object> mqResult = new LinkedHashMap<>();
        mqResult.put("checked", false);
        mqResult.put("persist_msg_id", anomaly.persistMsgId());
        mqResult.put("reason", hasText(anomaly.persistMsgId())
                ? "D7/D8 will verify broker confirm or consumer state"
                : "MQ publish had not started before this anomaly");
        return new AcceptAnomalyCheckEvidence(redisResult, mysqlResult, mqResult);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
