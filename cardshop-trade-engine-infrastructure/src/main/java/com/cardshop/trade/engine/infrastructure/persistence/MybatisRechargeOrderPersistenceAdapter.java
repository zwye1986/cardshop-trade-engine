package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.PersistStatus;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartRecordDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;
import org.springframework.transaction.annotation.Transactional;

/**
 * 基于 MyBatis 的 Redis/MQ 订单落库适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。
 * 本类是 D7/D8 的事务边界：同一个事务内写 `mc_order`、`recharge_order_ext`、必要的
 * `mc_depart_record(PRE_DEDUCT)`，并在允许派单时写入 `SUPPLIER_DISPATCH` outbox。
 * 事务成功后由 application service 调用 Redis `markDbPersisted`。
 */
public class MybatisRechargeOrderPersistenceAdapter implements RechargeOrderPersistencePort {

    private static final String DATA_STATUS_PROCESSED = "PROCESSED";
    private static final String PRE_DEDUCT_RECORD_TYPE = "1";
    private static final int ORDER_STATUS_PROCESSING = 2;
    private static final int ORDER_STATUS_FAILED = 6;
    private static final int DEFAULT_MAX_RETRY_COUNT = 10;
    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");

    private final McOrderPersistWriter orderWriter;
    private final RechargeOrderExtPersistWriter extWriter;
    private final RechargeOutboxPersistWriter outboxWriter;
    private final McDepartRecordPersistWriter departRecordWriter;
    private final ObjectMapper objectMapper;

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
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    /**
     * 在一个数据库事务内写入订单事实、扩展状态和必要 outbox。
     *
     * <p>重复 MQ 消息会再次进入该方法，因此 Mapper 必须使用幂等 insert/upsert：
     * `mc_order` 保持首次事实并作为减少重复写的串行点（orderInserted == 1 才写资金流水），
     * `recharge_order_ext` 只前进版本，资金流水按 `trade_engine_record_key` DB 唯一键兜底，
     * `recharge_outbox` 按 event_id 幂等。
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
         * docs/交行直充业务系统详细设计.md「20.13.8 异步落库 Consumer」资金流水幂等约束。
         */
        int orderInserted = orderWriter.insertIfAbsent(toMcOrder(aggregate));
        extWriter.insertOrUpdatePersistState(toExt(aggregate));
        if (orderInserted == 1 && preDeductRecordRequired(aggregate)) {
            departRecordWriter.insertPreDeductIfAbsent(toPreDeductRecord(aggregate));
        }
        if (aggregate.dispatchOutboxRequired()) {
            outboxWriter.insertIfAbsent(toDispatchOutbox(aggregate));
        }
    }

    private McOrderDO toMcOrder(OrderPersistAggregate aggregate) {
        PersistOrderSnapshot snapshot = aggregate.snapshot();
        LocalDateTime persistedAt = persistedAt(aggregate);
        McOrderDO order = new McOrderDO();
        order.setOrgCode(snapshot.orgCode());
        order.setCreateTime(persistedAt);
        order.setEditTime(persistedAt);
        order.setProductCode(snapshot.selectedProductId());
        order.setParentId(snapshot.productId());
        order.setProductName(snapshot.productName());
        order.setProductPrice(snapshot.orderPrice());
        order.setProductValue(snapshot.productValue());
        order.setProductType(snapshot.productType());
        order.setNum(snapshot.num());
        order.setOrderId(snapshot.orderId());
        order.setOutOrderId(snapshot.outOrderSn());
        order.setOutOrderNo(snapshot.outOrderSn());
        order.setOrderPrice(snapshot.orderPrice());
        order.setRefundOrderPrice(0L);
        order.setOrderStatus(orderStatus(snapshot));
        order.setDataStatus(DATA_STATUS_PROCESSED);
        order.setCallbackUrl(snapshot.notifyUrl());
        order.setSupplierChannel(snapshot.supplierChannel());
        order.setResData(resData(aggregate));
        order.setAccount(snapshot.account());
        order.setCouponType(snapshot.couponType());
        order.setOrderType(0);
        order.setFailCode(snapshot.failCode());
        order.setFailReason(snapshot.failReason());
        return order;
    }

    private RechargeOrderExtDO toExt(OrderPersistAggregate aggregate) {
        PersistOrderSnapshot snapshot = aggregate.snapshot();
        LocalDateTime persistedAt = persistedAt(aggregate);
        RechargeOrderExtDO ext = new RechargeOrderExtDO();
        ext.setOrderId(snapshot.orderId());
        ext.setOrgCode(snapshot.orgCode());
        ext.setOutOrderSn(snapshot.outOrderSn());
        ext.setRequestHash(snapshot.requestHash());
        ext.setRequestHashVersion(snapshot.requestHashVersion());
        ext.setRequestHashAlg(snapshot.requestHashAlg());
        ext.setRedisOrderId(snapshot.orderId());
        ext.setPersistMsgId(aggregate.persistMessageId());
        ext.setPersistStatus(PersistStatus.DB_PERSISTED.name());
        ext.setPersistSource(aggregate.persistSource().name());
        ext.setRedisOrderVersion(snapshot.orderVersion());
        // persisted_version 是“已成功写入 MySQL 的 Redis 订单版本”，必须随 order_version 单调前进。
        ext.setPersistedVersion(snapshot.orderVersion());
        ext.setValidationStatus(snapshot.validationStatus());
        ext.setDispatchAllowed(snapshot.dispatchAllowed());
        ext.setBalanceReserved(snapshot.balanceReserved());
        ext.setMqPublishCount(snapshot.mqPublishCount());
        ext.setPersistRetryCount(0);
        ext.setLastError(snapshot.failReason());
        ext.setCreatedAt(persistedAt);
        ext.setUpdatedAt(persistedAt);
        return ext;
    }

    private McDepartRecordDO toPreDeductRecord(OrderPersistAggregate aggregate) {
        PersistOrderSnapshot snapshot = aggregate.snapshot();
        McDepartRecordDO record = new McDepartRecordDO();
        record.setOrgCode(snapshot.orgCode());
        record.setBeforeBalance(null);
        record.setBalance(null);
        record.setCreateTime(persistedAt(aggregate));
        record.setType(PRE_DEDUCT_RECORD_TYPE);
        record.setOrderId(snapshot.orderId());
        /*
         * 测试库旧数据表明扣款流水使用 `type=1` 搭配正数 value，余额差额由
         * before_balance/balance 体现。D7/D8 首期没有旧 MySQL 实时余额，所以只记录发生金额。
         */
        record.setValue(snapshot.orderPrice());
        return record;
    }

    private RechargeOutboxDO toDispatchOutbox(OrderPersistAggregate aggregate) {
        PersistOrderSnapshot snapshot = aggregate.snapshot();
        LocalDateTime persistedAt = persistedAt(aggregate);
        String eventId = "dispatch-" + snapshot.orderId();
        RechargeOutboxDO outbox = new RechargeOutboxDO();
        outbox.setEventId(eventId);
        outbox.setEventType(OutboxEventType.SUPPLIER_DISPATCH.name());
        outbox.setOrgCode(snapshot.orgCode());
        outbox.setOrderId(snapshot.orderId());
        outbox.setOutOrderSn(snapshot.outOrderSn());
        outbox.setPayload(dispatchPayload(aggregate));
        outbox.setStatus(TaskStatus.INIT.name());
        outbox.setRetryCount(0);
        outbox.setMaxRetryCount(DEFAULT_MAX_RETRY_COUNT);
        outbox.setNextRetryTime(persistedAt);
        outbox.setLockedBy(null);
        outbox.setLockedUntil(null);
        outbox.setVersion(0L);
        outbox.setLastError("");
        outbox.setShardKey(eventId);
        outbox.setCreatedAt(persistedAt);
        outbox.setUpdatedAt(persistedAt);
        return outbox;
    }

    private static boolean preDeductRecordRequired(OrderPersistAggregate aggregate) {
        PersistOrderSnapshot snapshot = aggregate.snapshot();
        return aggregate.dispatchOutboxRequired() && snapshot.balanceReserved();
    }

    private String dispatchPayload(OrderPersistAggregate aggregate) {
        PersistOrderSnapshot snapshot = aggregate.snapshot();
        try {
            return objectMapper.writeValueAsString(Map.ofEntries(
                    Map.entry("order_id", snapshot.orderId()),
                    Map.entry("org_code", snapshot.orgCode()),
                    Map.entry("out_order_sn", snapshot.outOrderSn()),
                    Map.entry("account", snapshot.account()),
                    Map.entry("product_id", snapshot.productId()),
                    Map.entry("selected_product_id", snapshot.selectedProductId()),
                    Map.entry("supplier_channel", snapshot.supplierChannel()),
                    Map.entry("order_price", snapshot.orderPrice()),
                    Map.entry("persist_msg_id", aggregate.persistMessageId()),
                    Map.entry("persist_source", aggregate.persistSource().name())));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("serialize supplier dispatch outbox payload failed", ex);
        }
    }

    private String resData(OrderPersistAggregate aggregate) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "persist_msg_id", aggregate.persistMessageId(),
                    "persist_source", aggregate.persistSource().name()));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("serialize mc_order res_data failed", ex);
        }
    }

    private static int orderStatus(PersistOrderSnapshot snapshot) {
        return "PASSED".equals(snapshot.validationStatus()) ? ORDER_STATUS_PROCESSING : ORDER_STATUS_FAILED;
    }

    private static LocalDateTime persistedAt(OrderPersistAggregate aggregate) {
        /*
         * docs/交行直充业务系统详细设计.md「20.13.8 异步落库 Consumer」
         * mc_order/recharge_order_ext/recharge_outbox 使用 MySQL datetime 字面值，旧卡管库按 Asia/Shanghai
         * 读写和展示时间。这里将 Instant 转成本地业务时区的 LocalDateTime，避免补偿任务按 next_retry_time
         * 扫描时比真实时间偏移 8 小时。
         */
        return LocalDateTime.ofInstant(aggregate.persistedAt(), LEGACY_CARDSHOP_DB_ZONE);
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
