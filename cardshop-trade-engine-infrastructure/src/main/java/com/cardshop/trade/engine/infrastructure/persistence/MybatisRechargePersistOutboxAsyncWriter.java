package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.RechargePersistOutboxAsyncWriter;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeBalanceDeltaOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeDispatchOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeBalanceDeltaOutboxMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 基于 MyBatis 的 persist 批路径 outbox 异步写入适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer · Outbox 异步落库模式」。
 * 本 adapter 只负责事实事务提交后的第二段短事务：使用独立连接池对应的
 * {@link SqlSessionFactory} 与事务管理器幂等写入 {@code recharge_dispatch_outbox} 和
 * {@code recharge_balance_delta_outbox}，避免 timer claim 锁等待回压第一段订单事实事务。
 */
public class MybatisRechargePersistOutboxAsyncWriter implements RechargePersistOutboxAsyncWriter {

    private static final String PHASE_RECHARGE_DISPATCH_OUTBOX = "recharge_dispatch_outbox";
    private static final String PHASE_RECHARGE_BALANCE_DELTA_OUTBOX = "recharge_balance_delta_outbox";

    private static final Comparator<OrderPersistAggregate> STABLE_ORDER =
            Comparator.comparing((OrderPersistAggregate aggregate) -> aggregate.snapshot().orgCode())
                    .thenComparing(aggregate -> aggregate.snapshot().outOrderSn())
                    .thenComparing(aggregate -> aggregate.snapshot().orderId());

    private final RechargeDispatchOutboxSinglePersistWriter dispatchSingleWriter;
    private final RechargeDispatchOutboxBatchPersistWriter dispatchBatchWriter;
    private final RechargeBalanceDeltaOutboxSinglePersistWriter balanceSingleWriter;
    private final RechargeBalanceDeltaOutboxBatchPersistWriter balanceBatchWriter;
    private final RechargeOrderPersistenceRowConverter rowConverter;
    private final TradeEngineObservabilityMetrics observabilityMetrics;
    private final boolean batchEnabled;
    private final TransactionOperations transactionOperations;

    /**
     * 使用独立 writer SqlSessionFactory 与事务管理器创建 outbox writer。
     *
     * @param sqlSessionFactory writer 独立连接池对应的 MyBatis 会话工厂
     * @param transactionManager writer 独立连接池对应的事务管理器
     */
    public MybatisRechargePersistOutboxAsyncWriter(
            SqlSessionFactory sqlSessionFactory,
            ObjectMapper objectMapper,
            TradeEngineObservabilityMetrics observabilityMetrics,
            Duration dispatchInitialDelay,
            boolean batchEnabled,
            PlatformTransactionManager transactionManager) {
        this(
                outboxMappers(sqlSessionFactory),
                objectMapper,
                observabilityMetrics,
                dispatchInitialDelay,
                batchEnabled,
                new TransactionTemplate(Objects.requireNonNull(
                        transactionManager,
                        "transactionManager must not be null")));
    }

    private MybatisRechargePersistOutboxAsyncWriter(
            OutboxMapperBundle mappers,
            ObjectMapper objectMapper,
            TradeEngineObservabilityMetrics observabilityMetrics,
            Duration dispatchInitialDelay,
            boolean batchEnabled,
            TransactionOperations transactionOperations) {
        this(
                mappers.dispatchMapper()::insertIfAbsent,
                mappers.dispatchMapper()::insertDispatchOutboxBatchIfAbsent,
                mappers.balanceMapper()::insertIfAbsent,
                mappers.balanceMapper()::insertBalanceDeltaBatchIfAbsent,
                objectMapper,
                observabilityMetrics,
                dispatchInitialDelay,
                batchEnabled,
                transactionOperations);
    }

    MybatisRechargePersistOutboxAsyncWriter(
            RechargeDispatchOutboxSinglePersistWriter dispatchSingleWriter,
            RechargeDispatchOutboxBatchPersistWriter dispatchBatchWriter,
            RechargeBalanceDeltaOutboxSinglePersistWriter balanceSingleWriter,
            RechargeBalanceDeltaOutboxBatchPersistWriter balanceBatchWriter,
            ObjectMapper objectMapper,
            TradeEngineObservabilityMetrics observabilityMetrics,
            Duration dispatchInitialDelay,
            boolean batchEnabled,
            TransactionOperations transactionOperations) {
        this.dispatchSingleWriter = Objects.requireNonNull(
                dispatchSingleWriter,
                "dispatchSingleWriter must not be null");
        this.dispatchBatchWriter = Objects.requireNonNull(
                dispatchBatchWriter,
                "dispatchBatchWriter must not be null");
        this.balanceSingleWriter = Objects.requireNonNull(
                balanceSingleWriter,
                "balanceSingleWriter must not be null");
        this.balanceBatchWriter = Objects.requireNonNull(
                balanceBatchWriter,
                "balanceBatchWriter must not be null");
        this.rowConverter = new RechargeOrderPersistenceRowConverter(objectMapper, dispatchInitialDelay);
        this.observabilityMetrics = observabilityMetrics == null
                ? TradeEngineObservabilityMetrics.noop()
                : observabilityMetrics;
        this.batchEnabled = batchEnabled;
        this.transactionOperations = Objects.requireNonNull(
                transactionOperations,
                "transactionOperations must not be null");
    }

    @Override
    public void writeBatch(List<OrderPersistAggregate> aggregates) {
        Objects.requireNonNull(aggregates, "aggregates must not be null");
        if (aggregates.isEmpty()) {
            return;
        }
        List<OrderPersistAggregate> sorted = aggregates.stream()
                .map(aggregate -> Objects.requireNonNull(aggregate, "aggregates must not contain null"))
                .sorted(STABLE_ORDER)
                .toList();
        long startedAtNanos = System.nanoTime();
        try {
            transactionOperations.execute(status -> {
                writeSorted(sorted);
                return null;
            });
        } finally {
            observabilityMetrics.recordPersistOutboxWriterDuration(
                    Duration.ofNanos(System.nanoTime() - startedAtNanos));
        }
    }

    private void writeSorted(List<OrderPersistAggregate> sorted) {
        List<RechargeDispatchOutboxDO> dispatchRows = new ArrayList<>(sorted.size());
        List<RechargeBalanceDeltaOutboxDO> balanceRows = new ArrayList<>(sorted.size());
        for (OrderPersistAggregate aggregate : sorted) {
            if (aggregate.dispatchOutboxRequired()) {
                dispatchRows.add(rowConverter.toDispatchOutbox(aggregate));
            }
            if (aggregate.snapshot().balanceReserved()) {
                balanceRows.add(rowConverter.toBalanceDeltaFlushOutbox(aggregate));
            }
        }
        if (!dispatchRows.isEmpty()) {
            recordPhase(PHASE_RECHARGE_DISPATCH_OUTBOX, () -> writeDispatchRows(dispatchRows));
        }
        if (!balanceRows.isEmpty()) {
            recordPhase(PHASE_RECHARGE_BALANCE_DELTA_OUTBOX, () -> writeBalanceRows(balanceRows));
        }
    }

    private void writeDispatchRows(List<RechargeDispatchOutboxDO> rows) {
        if (batchEnabled) {
            dispatchBatchWriter.insertDispatchOutboxBatchIfAbsent(rows);
            return;
        }
        rows.forEach(dispatchSingleWriter::insertDispatchOutboxIfAbsent);
    }

    private void writeBalanceRows(List<RechargeBalanceDeltaOutboxDO> rows) {
        if (batchEnabled) {
            balanceBatchWriter.insertBalanceDeltaBatchIfAbsent(rows);
            return;
        }
        rows.forEach(balanceSingleWriter::insertBalanceDeltaIfAbsent);
    }

    private void recordPhase(String phase, Runnable operation) {
        long startedAtNanos = System.nanoTime();
        try {
            operation.run();
        } finally {
            observabilityMetrics.recordPersistBatchMysqlPhaseDuration(
                    phase,
                    Duration.ofNanos(System.nanoTime() - startedAtNanos));
        }
    }

    private static OutboxMapperBundle outboxMappers(SqlSessionFactory sqlSessionFactory) {
        Objects.requireNonNull(sqlSessionFactory, "sqlSessionFactory must not be null");
        SqlSessionTemplate sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory);
        return new OutboxMapperBundle(
                sqlSessionTemplate.getMapper(RechargeDispatchOutboxMapper.class),
                sqlSessionTemplate.getMapper(RechargeBalanceDeltaOutboxMapper.class));
    }

    private record OutboxMapperBundle(
            RechargeDispatchOutboxMapper dispatchMapper,
            RechargeBalanceDeltaOutboxMapper balanceMapper) {
    }
}

interface RechargeDispatchOutboxSinglePersistWriter {

    int insertDispatchOutboxIfAbsent(RechargeDispatchOutboxDO row);
}

interface RechargeBalanceDeltaOutboxSinglePersistWriter {

    int insertBalanceDeltaIfAbsent(RechargeBalanceDeltaOutboxDO row);
}
