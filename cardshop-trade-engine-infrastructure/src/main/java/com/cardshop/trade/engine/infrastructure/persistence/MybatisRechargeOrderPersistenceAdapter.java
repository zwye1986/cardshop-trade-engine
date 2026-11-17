package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
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
import java.util.List;
import java.util.Objects;
import org.springframework.transaction.annotation.Transactional;

/**
 * 基于 MyBatis 的 Redis/MQ 订单落库适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
 * 本类是 D7/D8 的事务边界：同一个事务内按固定顺序写 {@code mc_order}、{@code recharge_order_ext}、
 * 必要的 {@code mc_depart_record(PRE_DEDUCT)}，允许派单时写入 {@code recharge_dispatch_outbox}
 *（详细设计「9.1 派单 Outbox」），Redis 已预扣余额时写入 {@code recharge_balance_delta_outbox}
 *（详细设计「9.2 余额 Delta 刷账 Outbox」）。不再写入旧混合表 {@code recharge_outbox}。
 * 事务成功后由 application service 调用 Redis {@code markDbPersisted}。
 */
public class MybatisRechargeOrderPersistenceAdapter implements RechargeOrderPersistencePort {

    private final McOrderPersistWriter orderWriter;
    private final RechargeOrderExtPersistWriter extWriter;
    private final RechargeDispatchOutboxPersistWriter dispatchOutboxWriter;
    private final RechargeBalanceDeltaOutboxPersistWriter balanceDeltaOutboxWriter;
    private final McDepartRecordPersistWriter departRecordWriter;
    private final RechargeOrderPersistenceRowConverter rowConverter;
    private final RechargeOrderIdentityVerifier identityVerifier;

    public MybatisRechargeOrderPersistenceAdapter(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            RechargeDispatchOutboxMapper dispatchOutboxMapper,
            RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper,
            McDepartRecordMapper departRecordMapper,
            ObjectMapper objectMapper) {
        this(
                orderMapper,
                extMapper,
                dispatchOutboxMapper,
                balanceDeltaOutboxMapper,
                departRecordMapper,
                objectMapper,
                Duration.ofSeconds(1));
    }

    /**
     * @param dispatchInitialDelay 落库后首次派单延迟，对应
     *     {@code trade.engine.fulfillment.dispatch-initial-delay-seconds}
     */
    public MybatisRechargeOrderPersistenceAdapter(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            RechargeDispatchOutboxMapper dispatchOutboxMapper,
            RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper,
            McDepartRecordMapper departRecordMapper,
            ObjectMapper objectMapper,
            Duration dispatchInitialDelay) {
        this(
                orderMapper::insertIfAbsent,
                extMapper::insertOrUpdatePersistState,
                dispatchOutboxMapper::insertIfAbsent,
                balanceDeltaOutboxMapper::insertIfAbsent,
                departRecordMapper::insertIfAbsent,
                new RechargeOrderIdentityVerifier(orderMapper, extMapper),
                objectMapper,
                dispatchInitialDelay);
    }

    MybatisRechargeOrderPersistenceAdapter(
            McOrderPersistWriter orderWriter,
            RechargeOrderExtPersistWriter extWriter,
            RechargeDispatchOutboxPersistWriter dispatchOutboxWriter,
            RechargeBalanceDeltaOutboxPersistWriter balanceDeltaOutboxWriter,
            McDepartRecordPersistWriter departRecordWriter,
            RechargeOrderIdentityVerifier identityVerifier,
            ObjectMapper objectMapper) {
        this(
                orderWriter,
                extWriter,
                dispatchOutboxWriter,
                balanceDeltaOutboxWriter,
                departRecordWriter,
                identityVerifier,
                objectMapper,
                Duration.ofSeconds(1));
    }

    MybatisRechargeOrderPersistenceAdapter(
            McOrderPersistWriter orderWriter,
            RechargeOrderExtPersistWriter extWriter,
            RechargeDispatchOutboxPersistWriter dispatchOutboxWriter,
            RechargeBalanceDeltaOutboxPersistWriter balanceDeltaOutboxWriter,
            McDepartRecordPersistWriter departRecordWriter,
            RechargeOrderIdentityVerifier identityVerifier,
            ObjectMapper objectMapper,
            Duration dispatchInitialDelay) {
        this.orderWriter = Objects.requireNonNull(orderWriter, "orderWriter must not be null");
        this.extWriter = Objects.requireNonNull(extWriter, "extWriter must not be null");
        this.dispatchOutboxWriter = Objects.requireNonNull(
                dispatchOutboxWriter, "dispatchOutboxWriter must not be null");
        this.balanceDeltaOutboxWriter = Objects.requireNonNull(
                balanceDeltaOutboxWriter, "balanceDeltaOutboxWriter must not be null");
        this.departRecordWriter = Objects.requireNonNull(departRecordWriter, "departRecordWriter must not be null");
        this.identityVerifier = Objects.requireNonNull(identityVerifier);
        this.rowConverter = new RechargeOrderPersistenceRowConverter(objectMapper, dispatchInitialDelay);
    }

    /**
     * 在一个数据库事务内写入订单事实、扩展状态和拆表后的必要 outbox。
     *
     * <p>写序必须与详细设计「8.3 异步落库 Consumer」一致：
     * {@code mc_order -> recharge_order_ext -> mc_depart_record -> recharge_dispatch_outbox ->
     * recharge_balance_delta_outbox}。
     *
     * <p>重复 MQ 消息会再次进入该方法，因此 Mapper 必须使用幂等 insert/upsert：
     * {@code mc_order} 保持首次事实并作为串行点，先确认完整身份再写幂等资金流水，
     * {@code recharge_order_ext} 只前进版本，资金流水按 {@code trade_engine_record_key} DB 唯一键兜底，
     * 派单/余额刷账两张 outbox 表各自按 {@code event_id} 幂等；D9 派单事件与 D12 余额刷账事件在同一事务内落地，
     * 避免订单已落库但余额沉淀或缺派单事件。
     */
    @Override
    @Transactional
    public void persist(OrderPersistAggregate aggregate) {
        Objects.requireNonNull(aggregate, "aggregate must not be null");
        var selected = List.of(aggregate);
        // JDBC的FOUND_ROWS选项会让ODKU无操作也返回1，不能据affected rows认定首次插入。
        // 插入前读用于识别已有孤儿；插入唯一键串行后再核完整身份，流水仍以唯一键最终幂等。
        boolean existed = identityVerifier.readOrders(selected).containsKey(aggregate.snapshot().orderId());
        orderWriter.insertIfAbsent(rowConverter.toMcOrder(aggregate));
        identityVerifier.verifyOrders(selected);
        // 已有主单不能借新消息认领缺失或不同摘要的扩展；主单锁后再核扩展，保持全链路锁序。
        if (existed) {
            identityVerifier.verifyExistingExtensions(selected);
        }
        extWriter.insertOrUpdatePersistState(rowConverter.toExt(aggregate));
        identityVerifier.verifyExtensions(selected);
        if (!existed && RechargeOrderPersistenceRowConverter.preDeductRecordRequired(aggregate)) {
            departRecordWriter.insertPreDeductIfAbsent(rowConverter.toPreDeductRecord(aggregate));
        }
        if (aggregate.dispatchOutboxRequired()) {
            dispatchOutboxWriter.insertIfAbsent(rowConverter.toDispatchOutbox(aggregate));
        }
        if (aggregate.snapshot().balanceReserved()) {
            balanceDeltaOutboxWriter.insertIfAbsent(rowConverter.toBalanceDeltaFlushOutbox(aggregate));
        }
    }

}

interface McOrderPersistWriter {

    int insertIfAbsent(McOrderDO order);
}

interface RechargeOrderExtPersistWriter {

    int insertOrUpdatePersistState(RechargeOrderExtDO ext);
}

/**
 * 派单 outbox 单条幂等写入端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。
 */
interface RechargeDispatchOutboxPersistWriter {

    int insertIfAbsent(RechargeDispatchOutboxDO outbox);
}

/**
 * 余额刷账 outbox 单条幂等写入端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」。
 */
interface RechargeBalanceDeltaOutboxPersistWriter {

    int insertIfAbsent(RechargeBalanceDeltaOutboxDO outbox);
}

interface McDepartRecordPersistWriter {

    int insertPreDeductIfAbsent(McDepartRecordDO record);
}
