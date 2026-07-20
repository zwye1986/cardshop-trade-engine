package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartRecordDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import org.springframework.transaction.annotation.Transactional;

/**
 * 基于 MyBatis 的 Redis/MQ 订单落库适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
 * 本类是 D7/D8 的事务边界：同一个事务内写 `mc_order`、`recharge_order_ext`、必要的
 * `mc_depart_record(PRE_DEDUCT)`，在允许派单时写入 `SUPPLIER_DISPATCH` outbox，并在 Redis 已预扣余额时写入
 * `BALANCE_DELTA_FLUSH` outbox 供 D12 余额沉淀任务处理。
 * 事务成功后由 application service 调用 Redis `markDbPersisted`。
 */
public class MybatisRechargeOrderPersistenceAdapter implements RechargeOrderPersistencePort {

    private final McOrderPersistWriter orderWriter;
    private final RechargeOrderExtPersistWriter extWriter;
    private final RechargeOutboxPersistWriter outboxWriter;
    private final McDepartRecordPersistWriter departRecordWriter;
    private final RechargeOrderPersistenceRowConverter rowConverter;

    public MybatisRechargeOrderPersistenceAdapter(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            RechargeOutboxMapper outboxMapper,
            McDepartRecordMapper departRecordMapper,
            ObjectMapper objectMapper) {
        this(
                orderMapper::insertIfAbsent,
                extMapper::insertOrUpdatePersistState,
                outboxMapper::insertIfAbsent,
                departRecordMapper::insertIfAbsent,
                objectMapper);
    }

    MybatisRechargeOrderPersistenceAdapter(
            McOrderPersistWriter orderWriter,
            RechargeOrderExtPersistWriter extWriter,
            RechargeOutboxPersistWriter outboxWriter,
            McDepartRecordPersistWriter departRecordWriter,
            ObjectMapper objectMapper) {
        this.orderWriter = Objects.requireNonNull(orderWriter, "orderWriter must not be null");
        this.extWriter = Objects.requireNonNull(extWriter, "extWriter must not be null");
        this.outboxWriter = Objects.requireNonNull(outboxWriter, "outboxWriter must not be null");
        this.departRecordWriter = Objects.requireNonNull(departRecordWriter, "departRecordWriter must not be null");
        this.rowConverter = new RechargeOrderPersistenceRowConverter(objectMapper);
    }

    /**
     * 在一个数据库事务内写入订单事实、扩展状态和必要 outbox。
     *
     * <p>重复 MQ 消息会再次进入该方法，因此 Mapper 必须使用幂等 insert/upsert：
     * `mc_order` 保持首次事实并作为减少重复写的串行点（orderInserted == 1 才写资金流水），
     * `recharge_order_ext` 只前进版本，资金流水按 `trade_engine_record_key` DB 唯一键兜底，
     * `recharge_outbox` 按 event_id 幂等；D12 余额刷账事件和 D9 派单事件在同一事务内落地，避免订单已落库
     * 但余额沉淀缺事件。
     */
    @Override
    @Transactional
    public void persist(OrderPersistAggregate aggregate) {
        Objects.requireNonNull(aggregate, "aggregate must not be null");
        /*
         * 以 mc_order 的唯一键 uk_direct_recharge_out_order_key 作为同订单落库的第一道串行点：
         * insertIfAbsent 返回 1 表示本事务是首次落库该订单，返回 0 表示订单事实已由先前提交的事务写入。
         * mc_order 与 mc_depart_record 在同一事务内提交，orderInserted == 1 时才尝试写 PRE_DEDUCT；
         * 资金流水本身仍依赖 `trade_engine_record_key` 唯一键做最终幂等兜底。参见
         * docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」资金流水幂等约束。
         */
        int orderInserted = orderWriter.insertIfAbsent(rowConverter.toMcOrder(aggregate));
        extWriter.insertOrUpdatePersistState(rowConverter.toExt(aggregate));
        if (orderInserted == 1 && RechargeOrderPersistenceRowConverter.preDeductRecordRequired(aggregate)) {
            departRecordWriter.insertPreDeductIfAbsent(rowConverter.toPreDeductRecord(aggregate));
        }
        if (aggregate.dispatchOutboxRequired()) {
            outboxWriter.insertIfAbsent(rowConverter.toDispatchOutbox(aggregate));
        }
        if (aggregate.snapshot().balanceReserved()) {
            outboxWriter.insertIfAbsent(rowConverter.toBalanceDeltaFlushOutbox(aggregate));
        }
    }

}

interface McOrderPersistWriter {

    int insertIfAbsent(McOrderDO order);
}

interface RechargeOrderExtPersistWriter {

    int insertOrUpdatePersistState(RechargeOrderExtDO ext);
}

interface RechargeOutboxPersistWriter {

    int insertIfAbsent(RechargeOutboxDO outbox);
}

interface McDepartRecordPersistWriter {

    int insertPreDeductIfAbsent(McDepartRecordDO record);
}
