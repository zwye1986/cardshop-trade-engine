package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.RechargeOrderBatchPersistencePort;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartRecordDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 基于 MyBatis 显式 multi-row SQL 的 Redis/MQ 订单批量落库适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。本 adapter 只服务
 * RabbitMQ 批消费灰度路径，单条消息仍走 {@link MybatisRechargeOrderPersistenceAdapter}。批模式按订单稳定排序、
 * 按表分组写入，并调用 mapper 的显式 multi-row 方法，降低并发批次死锁概率和 JDBC rewrite 不确定性。
 */
public class MybatisBatchRechargeOrderPersistenceAdapter
        implements RechargeOrderBatchPersistencePort {

    private static final String PHASE_MC_ORDER = "mc_order";
    private static final String PHASE_RECHARGE_ORDER_EXT = "recharge_order_ext";
    private static final String PHASE_MC_DEPART_RECORD = "mc_depart_record";
    private static final String PHASE_RECHARGE_OUTBOX = "recharge_outbox";
    private static final String PHASE_COMMIT = "commit";

    private static final Comparator<OrderPersistAggregate> STABLE_ORDER =
            Comparator.comparing((OrderPersistAggregate aggregate) -> aggregate.snapshot().orgCode())
                    .thenComparing(aggregate -> aggregate.snapshot().outOrderSn())
                    .thenComparing(aggregate -> aggregate.snapshot().orderId());

    private final McOrderBatchPersistWriter orderWriter;
    private final RechargeOrderExtBatchPersistWriter extWriter;
    private final RechargeOutboxBatchPersistWriter outboxWriter;
    private final McDepartRecordBatchPersistWriter departRecordWriter;
    private final RechargeOrderPersistenceRowConverter rowConverter;
    private final TradeEngineObservabilityMetrics observabilityMetrics;

    public MybatisBatchRechargeOrderPersistenceAdapter(
            SqlSessionFactory sqlSessionFactory,
            ObjectMapper objectMapper) {
        this(sqlSessionFactory, objectMapper, TradeEngineObservabilityMetrics.noop());
    }

    public MybatisBatchRechargeOrderPersistenceAdapter(
            SqlSessionFactory sqlSessionFactory,
            ObjectMapper objectMapper,
            TradeEngineObservabilityMetrics observabilityMetrics) {
        this(batchMappers(sqlSessionFactory), objectMapper, observabilityMetrics);
    }

    private MybatisBatchRechargeOrderPersistenceAdapter(
            BatchMapperBundle mappers,
            ObjectMapper objectMapper,
            TradeEngineObservabilityMetrics observabilityMetrics) {
        this(
                mappers.orderMapper()::insertOrderBatchIfAbsent,
                mappers.extMapper()::insertExtBatchOrUpdatePersistState,
                mappers.outboxMapper()::insertOutboxBatchIfAbsent,
                mappers.departRecordMapper()::insertDepartRecordBatchIfAbsent,
                objectMapper,
                observabilityMetrics);
    }

    MybatisBatchRechargeOrderPersistenceAdapter(
            McOrderBatchPersistWriter orderWriter,
            RechargeOrderExtBatchPersistWriter extWriter,
            RechargeOutboxBatchPersistWriter outboxWriter,
            McDepartRecordBatchPersistWriter departRecordWriter,
            ObjectMapper objectMapper) {
        this(orderWriter, extWriter, outboxWriter, departRecordWriter, objectMapper, TradeEngineObservabilityMetrics.noop());
    }

    MybatisBatchRechargeOrderPersistenceAdapter(
            McOrderBatchPersistWriter orderWriter,
            RechargeOrderExtBatchPersistWriter extWriter,
            RechargeOutboxBatchPersistWriter outboxWriter,
            McDepartRecordBatchPersistWriter departRecordWriter,
            ObjectMapper objectMapper,
            TradeEngineObservabilityMetrics observabilityMetrics) {
        this.orderWriter = Objects.requireNonNull(orderWriter, "orderWriter must not be null");
        this.extWriter = Objects.requireNonNull(extWriter, "extWriter must not be null");
        this.outboxWriter = Objects.requireNonNull(outboxWriter, "outboxWriter must not be null");
        this.departRecordWriter = Objects.requireNonNull(departRecordWriter, "departRecordWriter must not be null");
        this.rowConverter = new RechargeOrderPersistenceRowConverter(objectMapper);
        this.observabilityMetrics = observabilityMetrics == null
                ? TradeEngineObservabilityMetrics.noop()
                : observabilityMetrics;
    }

    /**
     * 在一个 MySQL 事务内批量写入订单事实、扩展状态和必要 outbox。
     *
     * <p>批写路径不能依赖 {@code mc_order.insertIfAbsent} 的即时 row count。资金流水直接走
     * {@code mc_depart_record.insertDepartRecordBatchIfAbsent}，由 {@code uk_mc_depart_record_trade_engine_key}
     * 唯一键保证重复 MQ/补投重试不会产生重复扣款流水。
     */
    @Override
    @Transactional
    public void persistBatch(List<OrderPersistAggregate> aggregates) {
        Objects.requireNonNull(aggregates, "aggregates must not be null");
        if (aggregates.isEmpty()) {
            return;
        }
        long startedAtNanos = System.nanoTime();
        AtomicLong bodyFinishedAtNanos = new AtomicLong(startedAtNanos);
        /*
         * 总事务耗时挂在 afterCompletion，覆盖 commit/rollback 与提交阶段锁等待；各表 SQL 在方法体内
         * 分段记录 phase 耗时。无事务上下文时（例如直接单测）总耗时退化为方法体计时。
         * 参见 docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」。
         */
        boolean timedByTransaction = registerPersistBatchMysqlDurationAfterCompletion(
                startedAtNanos,
                bodyFinishedAtNanos);
        try {
            List<OrderPersistAggregate> sorted = aggregates.stream()
                    .map(aggregate -> Objects.requireNonNull(aggregate, "aggregates must not contain null"))
                    .sorted(STABLE_ORDER)
                    .toList();

            List<McOrderDO> orderRows = sorted.stream()
                    .map(rowConverter::toMcOrder)
                    .toList();
            recordPhase(PHASE_MC_ORDER, () -> orderWriter.insertOrderBatchIfAbsent(orderRows));

            List<RechargeOrderExtDO> extRows = sorted.stream()
                    .map(rowConverter::toExt)
                    .toList();
            recordPhase(PHASE_RECHARGE_ORDER_EXT, () -> extWriter.insertBatchOrUpdatePersistState(extRows));

            List<McDepartRecordDO> departRecordRows = sorted.stream()
                    .filter(RechargeOrderPersistenceRowConverter::preDeductRecordRequired)
                    .map(rowConverter::toPreDeductRecord)
                    .toList();
            if (!departRecordRows.isEmpty()) {
                recordPhase(PHASE_MC_DEPART_RECORD, () ->
                        departRecordWriter.insertPreDeductBatchIfAbsent(departRecordRows));
            }

            List<RechargeOutboxDO> outboxRows = new ArrayList<>(sorted.size() * 2);
            for (OrderPersistAggregate aggregate : sorted) {
                if (aggregate.dispatchOutboxRequired()) {
                    outboxRows.add(rowConverter.toDispatchOutbox(aggregate));
                }
                if (aggregate.snapshot().balanceReserved()) {
                    outboxRows.add(rowConverter.toBalanceDeltaFlushOutbox(aggregate));
                }
            }
            if (!outboxRows.isEmpty()) {
                recordPhase(PHASE_RECHARGE_OUTBOX, () -> outboxWriter.insertOutboxBatchIfAbsent(outboxRows));
            }
        } finally {
            long finishedAtNanos = System.nanoTime();
            bodyFinishedAtNanos.set(finishedAtNanos);
            if (!timedByTransaction) {
                observabilityMetrics.recordPersistBatchMysqlDuration(
                        Duration.ofNanos(finishedAtNanos - startedAtNanos));
            }
        }
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

    private boolean registerPersistBatchMysqlDurationAfterCompletion(
            long startedAtNanos,
            AtomicLong bodyFinishedAtNanos) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            return false;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                long completedAtNanos = System.nanoTime();
                observabilityMetrics.recordPersistBatchMysqlDuration(Duration.ofNanos(completedAtNanos - startedAtNanos));
                observabilityMetrics.recordPersistBatchMysqlPhaseDuration(
                        PHASE_COMMIT,
                        Duration.ofNanos(Math.max(0L, completedAtNanos - bodyFinishedAtNanos.get())));
            }
        });
        return true;
    }

    private static BatchMapperBundle batchMappers(SqlSessionFactory sqlSessionFactory) {
        Objects.requireNonNull(sqlSessionFactory, "sqlSessionFactory must not be null");
        SqlSessionTemplate sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory);
        return new BatchMapperBundle(
                sqlSessionTemplate.getMapper(McOrderMapper.class),
                sqlSessionTemplate.getMapper(RechargeOrderExtMapper.class),
                sqlSessionTemplate.getMapper(RechargeOutboxMapper.class),
                sqlSessionTemplate.getMapper(McDepartRecordMapper.class));
    }

    private record BatchMapperBundle(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            RechargeOutboxMapper outboxMapper,
            McDepartRecordMapper departRecordMapper) {
    }
}

interface McOrderBatchPersistWriter {

    int insertOrderBatchIfAbsent(List<McOrderDO> rows);
}

interface RechargeOrderExtBatchPersistWriter {

    int insertBatchOrUpdatePersistState(List<RechargeOrderExtDO> rows);
}

interface RechargeOutboxBatchPersistWriter {

    int insertOutboxBatchIfAbsent(List<RechargeOutboxDO> rows);
}

interface McDepartRecordBatchPersistWriter {

    int insertPreDeductBatchIfAbsent(List<McDepartRecordDO> rows);
}
