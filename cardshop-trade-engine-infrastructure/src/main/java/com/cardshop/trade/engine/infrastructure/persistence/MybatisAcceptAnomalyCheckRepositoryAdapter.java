package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCase;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckEvidence;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyStatus;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeAcceptAnomalyDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 MyBatis 的接单异常自动核查仓储适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.8 接单异常核查表」和
 * 「20.13.6 Redis Lua 下单结果」。该适配器负责把 application 层的核查状态推进写回
 * `recharge_accept_anomaly`；核查决策本身由 {@code AcceptAnomalyAutoCheckService} 完成。
 */
public class MybatisAcceptAnomalyCheckRepositoryAdapter implements AcceptAnomalyCheckRepositoryPort {

    private static final Logger log = LoggerFactory.getLogger(MybatisAcceptAnomalyCheckRepositoryAdapter.class);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final AcceptAnomalyCheckStore store;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    /**
     * 生产构造器，使用 `RechargeAcceptAnomalyMapper` 的受控批量读取和状态推进方法。
     */
    public MybatisAcceptAnomalyCheckRepositoryAdapter(
            RechargeAcceptAnomalyMapper mapper,
            ObjectMapper objectMapper,
            Clock clock) {
        this(new MapperAcceptAnomalyCheckStore(mapper), objectMapper, clock);
    }

    /**
     * 测试构造器，通过极薄 store 抽象验证字段映射，避免单元测试启动 MyBatis 容器。
     */
    MybatisAcceptAnomalyCheckRepositoryAdapter(
            AcceptAnomalyCheckStore store,
            ObjectMapper objectMapper,
            Clock clock) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public List<AcceptAnomalyCase> findOpenForCheck(int limit) {
        return store.findOpenForCheck(Math.max(1, limit))
                .stream()
                .map(this::toCase)
                .toList();
    }

    @Override
    public void markChecking(String anomalyId, String workerId) {
        store.markChecking(anomalyId, now());
    }

    @Override
    public void markResolvedAccepted(String anomalyId, AcceptAnomalyCheckEvidence evidence, String workerId) {
        store.updateCheckResult(updateRow(
                anomalyId,
                AcceptAnomalyStatus.RESOLVED_ACCEPTED,
                "RESOLVED_ACCEPTED",
                evidence,
                workerId,
                ""));
    }

    @Override
    public void markResolvedNotAccepted(String anomalyId, AcceptAnomalyCheckEvidence evidence, String workerId) {
        store.updateCheckResult(updateRow(
                anomalyId,
                AcceptAnomalyStatus.RESOLVED_NOT_ACCEPTED,
                "RESOLVED_NOT_ACCEPTED",
                evidence,
                workerId,
                ""));
    }

    @Override
    public void markManualRequired(
            String anomalyId,
            AcceptAnomalyCheckEvidence evidence,
            String reason,
            String workerId) {
        RechargeAcceptAnomalyDO row = updateRow(
                anomalyId,
                AcceptAnomalyStatus.MANUAL_REQUIRED,
                null,
                evidence,
                "",
                reason);
        row.setResolvedAt(null);
        store.updateCheckResult(row);
    }

    /**
     * 将异常表行映射为 application 层核查模型。
     */
    private AcceptAnomalyCase toCase(RechargeAcceptAnomalyDO row) {
        return new AcceptAnomalyCase(
                row.getAnomalyId(),
                anomalyType(row.getAnomalyType()),
                row.getOrgCode(),
                row.getOutOrderSn(),
                row.getOrderId(),
                row.getRequestHash(),
                row.getPersistMsgId(),
                rawContext(row));
    }

    private RechargeAcceptAnomalyDO updateRow(
            String anomalyId,
            AcceptAnomalyStatus status,
            String resolution,
            AcceptAnomalyCheckEvidence evidence,
            String workerId,
            String lastError) {
        RechargeAcceptAnomalyDO row = new RechargeAcceptAnomalyDO();
        LocalDateTime now = now();
        row.setAnomalyId(anomalyId);
        row.setStatus(status.name());
        row.setRedisCheckResult(json(evidence.redisCheckResult()));
        row.setMysqlCheckResult(json(evidence.mysqlCheckResult()));
        row.setMqCheckResult(json(evidence.mqCheckResult()));
        row.setResolution(resolution);
        row.setResolvedBy(workerId);
        row.setResolvedAt(now);
        row.setLastError(lastError);
        row.setUpdatedAt(now);
        return row;
    }

    private Map<String, Object> rawContext(RechargeAcceptAnomalyDO row) {
        if (row.getRawContext() == null || row.getRawContext().isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(row.getRawContext(), MAP_TYPE);
        } catch (JsonProcessingException ex) {
            /*
             * raw_context 解析失败说明历史数据或手工修复数据不合规。这里不丢弃记录，而是把解析错误写入上下文，
             * 让自动核查服务能够继续转人工或保守处理。
             */
            log.error("接单异常raw_context解析失败 anomalyId={}, orgCode={}, outOrderSn={}",
                    row.getAnomalyId(),
                    row.getOrgCode(),
                    row.getOutOrderSn(),
                    ex);
            return Map.of("raw_context_parse_error", ex.getOriginalMessage());
        }
    }

    private String json(Map<String, Object> value) {
        try {
            return objectMapper.writeValueAsString(value == null ? Map.of() : value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("serialize accept anomaly check evidence failed", ex);
        }
    }

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
    }

    private static AcceptAnomalyType anomalyType(String value) {
        try {
            return AcceptAnomalyType.valueOf(value);
        } catch (RuntimeException ex) {
            log.error("接单异常类型解析失败 anomalyType={}", value, ex);
            return AcceptAnomalyType.ACCEPT_STATE_UNKNOWN;
        }
    }

    private record MapperAcceptAnomalyCheckStore(RechargeAcceptAnomalyMapper mapper)
            implements AcceptAnomalyCheckStore {

        @Override
        public List<RechargeAcceptAnomalyDO> findOpenForCheck(int limit) {
            return mapper.selectOpenForCheck(limit);
        }

        @Override
        public int markChecking(String anomalyId, LocalDateTime now) {
            return mapper.markChecking(anomalyId, now);
        }

        @Override
        public int updateCheckResult(RechargeAcceptAnomalyDO row) {
            return mapper.updateCheckResult(row);
        }
    }
}

/**
 * 接单异常核查表访问极薄抽象，便于单元测试脱离 MyBatis 容器。
 */
interface AcceptAnomalyCheckStore {

    List<RechargeAcceptAnomalyDO> findOpenForCheck(int limit);

    int markChecking(String anomalyId, LocalDateTime now);

    int updateCheckResult(RechargeAcceptAnomalyDO row);
}
