package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.RechargeOrderBatchPersistencePort;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 基于 MyBatis BATCH executor 的 Redis/MQ 订单批量落库适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。本 adapter 只服务
 * RabbitMQ 批消费灰度路径，单条消息仍走 {@link MybatisRechargeOrderPersistenceAdapter}。批模式按订单稳定排序、
 * 按表分组写入，降低并发批次死锁概率，并让 MyBatis/JDBC 能聚合同类 SQL。
 */
public class MybatisBatchRechargeOrderPersistenceAdapter
        implements RechargeOrderBatchPersistencePort {

    private static final Comparator<OrderPersistAggregate> STABLE_ORDER =
            Comparator.comparing((OrderPersistAggregate aggregate) -> aggregate.snapshot().orgCode())
                    .thenComparing(aggregate -> aggregate.snapshot().outOrderSn())
                    .thenComparing(aggregate -> aggregate.snapshot().orderId());

    private final McOrderPersistWriter orderWriter;
    private final RechargeOrderExtPersistWriter extWriter;
    private final RechargeOutboxPersistWriter outboxWriter;
    private final McDepartRecordPersistWriter departRecordWriter;
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
                mappers.orderMapper()::insertIfAbsent,
                mappers.extMapper()::insertOrUpdatePersistState,
                mappers.outboxMapper()::insertIfAbsent,
                mappers.departRecordMapper()::insertIfAbsent,
                objectMapper,
                observabilityMetrics);
    }

    MybatisBatchRechargeOrderPersistenceAdapter(
            McOrderPersistWriter orderWriter,
            RechargeOrderExtPersistWriter extWriter,
            RechargeOutboxPersistWriter outboxWriter,
            McDepartRecordPersistWriter departRecordWriter,
            ObjectMapper objectMapper) {
        this(orderWriter, extWriter, outboxWriter, departRecordWriter, objectMapper, TradeEngineObservabilityMetrics.noop());
    }

    MybatisBatchRechargeOrderPersistenceAdapter(
            McOrderPersistWriter orderWriter,
            RechargeOrderExtPersistWriter extWriter,
            RechargeOutboxPersistWriter outboxWriter,
            McDepartRecordPersistWriter departRecordWriter,
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
     * <p>批写路径不能依赖 {@code mc_order.insertIfAbsent} 的即时 row count：MyBatis BATCH executor 会延迟
     * flush，部分驱动返回值也可能只是占位。资金流水因此直接走 {@code mc_depart_record.insertIfAbsent}，
     * 由 {@code uk_mc_depart_record_trade_engine_key} 唯一键保证重复 MQ/补投重试不会产生重复扣款流水。
     */
    @Override
    @Transactional
    public void persistBatch(List<OrderPersistAggregate> aggregates) {
        Objects.requireNonNull(aggregates, "aggregates must not be null");
        if (aggregates.isEmpty()) {
            return;
        }
        long startedAtNanos = System.nanoTime();
        /*
         * MyBatis-Spring 的 BATCH flush/commit 发生在事务同步 beforeCommit()，晚于本方法返回。
         * 因此批事务耗时必须挂在 afterCompletion，才能覆盖 flush、commit 和锁等待；方法 finally 只会量到
         * “攒 SQL”阶段，观测值系统性偏低。无事务上下文时（例如直接单测）退化为方法体计时。
         * 参见 docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」。
         */
        boolean timedByTransaction = registerPersistBatchMysqlDurationAfterCompletion(startedAtNanos);
        try {
            List<OrderPersistAggregate> sorted = aggregates.stream()
                    .map(aggregate -> Objects.requireNonNull(aggregate, "aggregates must not contain null"))
                    .sorted(STABLE_ORDER)
                    .toList();

            for (OrderPersistAggregate aggregate : sorted) {
                orderWriter.insertIfAbsent(rowConverter.toMcOrder(aggregate));
            }
            for (OrderPersistAggregate aggregate : sorted) {
                extWriter.insertOrUpdatePersistState(rowConverter.toExt(aggregate));
            }
            for (OrderPersistAggregate aggregate : sorted) {
                if (RechargeOrderPersistenceRowConverter.preDeductRecordRequired(aggregate)) {
                    departRecordWriter.insertPreDeductIfAbsent(rowConverter.toPreDeductRecord(aggregate));
                }
            }
            for (OrderPersistAggregate aggregate : sorted) {
                if (aggregate.dispatchOutboxRequired()) {
                    outboxWriter.insertIfAbsent(rowConverter.toDispatchOutbox(aggregate));
                }
                if (aggregate.snapshot().balanceReserved()) {
                    outboxWriter.insertIfAbsent(rowConverter.toBalanceDeltaFlushOutbox(aggregate));
                }
            }
        } finally {
            if (!timedByTransaction) {
                observabilityMetrics.recordPersistBatchMysqlDuration(
                        Duration.ofNanos(System.nanoTime() - startedAtNanos));
            }
        }
    }

    private boolean registerPersistBatchMysqlDurationAfterCompletion(long startedAtNanos) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            return false;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                observabilityMetrics.recordPersistBatchMysqlDuration(
                        Duration.ofNanos(System.nanoTime() - startedAtNanos));
            }
        });
        return true;
    }

    private static BatchMapperBundle batchMappers(SqlSessionFactory sqlSessionFactory) {
        Objects.requireNonNull(sqlSessionFactory, "sqlSessionFactory must not be null");
        SqlSessionTemplate sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory, ExecutorType.BATCH);
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
