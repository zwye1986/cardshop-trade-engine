package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.RechargeOrderBatchPersistencePort;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartRecordDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeBalanceDeltaOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeDispatchOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeBalanceDeltaOutboxMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
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
 *
 * <p>写表顺序固定为 {@code mc_order -> recharge_order_ext -> mc_depart_record ->
 * recharge_dispatch_outbox -> recharge_balance_delta_outbox}；派单与余额刷账拆表后分别上报 phase
 *（详细设计「9.1 派单 Outbox」「9.2 余额 Delta 刷账 Outbox」），不再使用旧混合表
 * {@code recharge_outbox} 及其 phase。
 */
public class MybatisBatchRechargeOrderPersistenceAdapter
        implements RechargeOrderBatchPersistencePort {

    private static final String PHASE_MC_ORDER = "mc_order";
    private static final String PHASE_RECHARGE_ORDER_EXT = "recharge_order_ext";
    private static final String PHASE_MC_DEPART_RECORD = "mc_depart_record";
    private static final String PHASE_RECHARGE_DISPATCH_OUTBOX = "recharge_dispatch_outbox";
    private static final String PHASE_RECHARGE_BALANCE_DELTA_OUTBOX = "recharge_balance_delta_outbox";
    private static final String PHASE_COMMIT = "commit";

    private static final Comparator<OrderPersistAggregate> STABLE_ORDER =
            Comparator.comparing((OrderPersistAggregate aggregate) -> aggregate.snapshot().orgCode())
                    .thenComparing(aggregate -> aggregate.snapshot().outOrderSn())
                    .thenComparing(aggregate -> aggregate.snapshot().orderId());

    private final McOrderBatchPersistWriter orderWriter;
    private final RechargeOrderExtBatchPersistWriter extWriter;
    private final RechargeDispatchOutboxBatchPersistWriter dispatchOutboxWriter;
    private final RechargeBalanceDeltaOutboxBatchPersistWriter balanceDeltaOutboxWriter;
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
                mappers.dispatchOutboxMapper()::insertDispatchOutboxBatchIfAbsent,
                mappers.balanceDeltaOutboxMapper()::insertBalanceDeltaBatchIfAbsent,
                mappers.departRecordMapper()::insertDepartRecordBatchIfAbsent,
                objectMapper,
                observabilityMetrics);
    }

    MybatisBatchRechargeOrderPersistenceAdapter(
            McOrderBatchPersistWriter orderWriter,
            RechargeOrderExtBatchPersistWriter extWriter,
            RechargeDispatchOutboxBatchPersistWriter dispatchOutboxWriter,
            RechargeBalanceDeltaOutboxBatchPersistWriter balanceDeltaOutboxWriter,
            McDepartRecordBatchPersistWriter departRecordWriter,
            ObjectMapper objectMapper) {
        this(
                orderWriter,
                extWriter,
                dispatchOutboxWriter,
                balanceDeltaOutboxWriter,
                departRecordWriter,
                objectMapper,
                TradeEngineObservabilityMetrics.noop());
    }

    MybatisBatchRechargeOrderPersistenceAdapter(
            McOrderBatchPersistWriter orderWriter,
            RechargeOrderExtBatchPersistWriter extWriter,
            RechargeDispatchOutboxBatchPersistWriter dispatchOutboxWriter,
            RechargeBalanceDeltaOutboxBatchPersistWriter balanceDeltaOutboxWriter,
            McDepartRecordBatchPersistWriter departRecordWriter,
            ObjectMapper objectMapper,
            TradeEngineObservabilityMetrics observabilityMetrics) {
        this.orderWriter = Objects.requireNonNull(orderWriter, "orderWriter must not be null");
        this.extWriter = Objects.requireNonNull(extWriter, "extWriter must not be null");
        this.dispatchOutboxWriter = Objects.requireNonNull(
                dispatchOutboxWriter, "dispatchOutboxWriter must not be null");
        this.balanceDeltaOutboxWriter = Objects.requireNonNull(
                balanceDeltaOutboxWriter, "balanceDeltaOutboxWriter must not be null");
        this.departRecordWriter = Objects.requireNonNull(departRecordWriter, "departRecordWriter must not be null");
        this.rowConverter = new RechargeOrderPersistenceRowConverter(objectMapper);
        this.observabilityMetrics = observabilityMetrics == null
                ? TradeEngineObservabilityMetrics.noop()
                : observabilityMetrics;
    }

    /**
     * 在一个 MySQL 事务内批量写入订单事实、扩展状态和拆表后的必要 outbox。
     *
     * <p>批写路径不能依赖 {@code mc_order.insertIfAbsent} 的即时 row count。资金流水直接走
     * {@code mc_depart_record.insertDepartRecordBatchIfAbsent}，由 {@code uk_mc_depart_record_trade_engine_key}
     * 唯一键保证重复 MQ/补投重试不会产生重复扣款流水。派单与余额刷账必须调用各自表的显式 multi-row 方法，
     * 并分别记录 phase（详细设计「8.3」）。
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
                recordPhase(PHASE_RECHARGE_DISPATCH_OUTBOX, () ->
                        dispatchOutboxWriter.insertDispatchOutboxBatchIfAbsent(dispatchRows));
            }
            if (!balanceRows.isEmpty()) {
                recordPhase(PHASE_RECHARGE_BALANCE_DELTA_OUTBOX, () ->
                        balanceDeltaOutboxWriter.insertBalanceDeltaBatchIfAbsent(balanceRows));
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
                sqlSessionTemplate.getMapper(RechargeDispatchOutboxMapper.class),
                sqlSessionTemplate.getMapper(RechargeBalanceDeltaOutboxMapper.class),
                sqlSessionTemplate.getMapper(McDepartRecordMapper.class));
    }

    private record BatchMapperBundle(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            RechargeDispatchOutboxMapper dispatchOutboxMapper,
            RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper,
            McDepartRecordMapper departRecordMapper) {
    }
}

interface McOrderBatchPersistWriter {

    int insertOrderBatchIfAbsent(List<McOrderDO> rows);
}

interface RechargeOrderExtBatchPersistWriter {

    int insertBatchOrUpdatePersistState(List<RechargeOrderExtDO> rows);
}

/**
 * 派单 outbox 批写端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」「9.1 派单 Outbox」。
 */
interface RechargeDispatchOutboxBatchPersistWriter {

    int insertDispatchOutboxBatchIfAbsent(List<RechargeDispatchOutboxDO> rows);
}

/**
 * 余额刷账 outbox 批写端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」「9.2 余额 Delta 刷账 Outbox」。
 */
interface RechargeBalanceDeltaOutboxBatchPersistWriter {

    int insertBalanceDeltaBatchIfAbsent(List<RechargeBalanceDeltaOutboxDO> rows);
}

interface McDepartRecordBatchPersistWriter {

    int insertPreDeductBatchIfAbsent(List<McDepartRecordDO> rows);
}
