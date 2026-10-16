package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.FulfillmentDispatchStatus;
import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.RechargeDispatchEventIds;
import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.PersistStatus;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartRecordDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeBalanceDeltaOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeDispatchOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

/**
 * Redis/MQ 订单落库行对象转换器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。单条 adapter 和批写 adapter
 * 共享同一套 DO 字段映射，避免批写为了“独立事务控制流”复制字段契约；事务边界、写入顺序和是否依赖 row count
 * 仍由各自 adapter 单独决定。
 *
 * <p>拆表后派单事件映射到 {@link RechargeDispatchOutboxDO}（详细设计「9.1 派单 Outbox」），
 * 余额刷账事件映射到 {@link RechargeBalanceDeltaOutboxDO}（详细设计「9.2 余额 Delta 刷账 Outbox」）；
 * 不再写入旧混合表 {@code recharge_outbox}。
 */
final class RechargeOrderPersistenceRowConverter {

    private static final String DATA_STATUS_PROCESSED = "PROCESSED";
    private static final String PRE_DEDUCT_RECORD_TYPE = "1";
    private static final String BALANCE_DELTA_TYPE_PRE_DEDUCT = "PRE_DEDUCT";
    private static final int DISPATCH_MAX_RETRY_COUNT = 3;
    private static final int BALANCE_DELTA_MAX_RETRY_COUNT = 10;
    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");

    private final ObjectMapper objectMapper;
    private final Duration dispatchInitialDelay;

    RechargeOrderPersistenceRowConverter(ObjectMapper objectMapper) {
        this(objectMapper, Duration.ofSeconds(1));
    }

    /**
     * @param dispatchInitialDelay 落库后首次派单延迟，对应
     *     {@code trade.engine.fulfillment.dispatch-initial-delay-seconds}；与 timer 热唤醒共用。
     */
    RechargeOrderPersistenceRowConverter(ObjectMapper objectMapper, Duration dispatchInitialDelay) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        Objects.requireNonNull(dispatchInitialDelay, "dispatchInitialDelay must not be null");
        if (dispatchInitialDelay.isZero() || dispatchInitialDelay.isNegative()) {
            throw new IllegalArgumentException("dispatchInitialDelay must be positive");
        }
        this.dispatchInitialDelay = dispatchInitialDelay;
    }

    McOrderDO toMcOrder(OrderPersistAggregate aggregate) {
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
        /*
         * 旧卡管列表主要读取 `mc_order.remark` 展示运营侧可读失败原因。
         * 详细设计「6.2 `mc_order` 字段说明」约定：校验失败订单要保留失败说明，
         * 因此这里仅在校验失败时把 `fail_reason` 同步到 `remark`，避免覆盖后续履约链路写入的备注。
         */
        order.setRemark(validationFailed(snapshot) ? snapshot.failReason() : null);
        return order;
    }

    RechargeOrderExtDO toExt(OrderPersistAggregate aggregate) {
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
        ext.setDispatchStatus(snapshot.dispatchAllowed()
                ? FulfillmentDispatchStatus.WAIT_DISPATCH.name()
                : FulfillmentDispatchStatus.FAILED.name());
        ext.setMqPublishCount(snapshot.mqPublishCount());
        ext.setPersistRetryCount(0);
        ext.setLastError(snapshot.failReason());
        ext.setCreatedAt(persistedAt);
        ext.setUpdatedAt(persistedAt);
        return ext;
    }

    McDepartRecordDO toPreDeductRecord(OrderPersistAggregate aggregate) {
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

    /**
     * 构造供应商派单 outbox 行。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」「9.1 派单 Outbox」。
     * {@code event_id=dispatch-{orderId}}，{@code shard_key} 固定等于 {@code event_id}，
     * {@code event_type=SUPPLIER_DISPATCH}；payload 仅承载派单所需非密钥字段。
     */
    RechargeDispatchOutboxDO toDispatchOutbox(OrderPersistAggregate aggregate) {
        PersistOrderSnapshot snapshot = aggregate.snapshot();
        LocalDateTime persistedAt = persistedAt(aggregate);
        String eventId = RechargeDispatchEventIds.dispatchEventId(snapshot.orderId());
        RechargeDispatchOutboxDO outbox = new RechargeDispatchOutboxDO();
        outbox.setEventId(eventId);
        outbox.setEventType(OutboxEventType.SUPPLIER_DISPATCH.name());
        outbox.setOrgCode(snapshot.orgCode());
        outbox.setOrderId(snapshot.orderId());
        outbox.setOutOrderSn(snapshot.outOrderSn());
        outbox.setPayload(dispatchPayload(aggregate));
        outbox.setStatus(TaskStatus.INIT.name());
        outbox.setRetryCount(0);
        outbox.setMaxRetryCount(DISPATCH_MAX_RETRY_COUNT);
        // 详细设计「9.1 派单 Outbox」：next_retry_time 与 timer 热唤醒共用
        // trade.engine.fulfillment.dispatch-initial-delay-seconds（默认 1 秒）。
        outbox.setNextRetryTime(persistedAt.plus(dispatchInitialDelay));
        outbox.setLockedBy(null);
        outbox.setLockedUntil(null);
        outbox.setVersion(0L);
        outbox.setLastError("");
        outbox.setShardKey(eventId);
        outbox.setCreatedAt(persistedAt);
        outbox.setUpdatedAt(persistedAt);
        return outbox;
    }

    /**
     * 构造余额预扣刷账 outbox 行。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」
     * 「9.2 余额 Delta 刷账 Outbox」。{@code event_id=balance-flush-{orderId}}，
     * {@code delta_type=PRE_DEDUCT}，{@code delta_amount=-orderPrice}（预扣为负，单位分）；
     * 本表使用结构化字段，不写 JSON payload / event_type / shard_key。
     */
    RechargeBalanceDeltaOutboxDO toBalanceDeltaFlushOutbox(OrderPersistAggregate aggregate) {
        PersistOrderSnapshot snapshot = aggregate.snapshot();
        LocalDateTime persistedAt = persistedAt(aggregate);
        String eventId = "balance-flush-" + snapshot.orderId();
        RechargeBalanceDeltaOutboxDO outbox = new RechargeBalanceDeltaOutboxDO();
        outbox.setEventId(eventId);
        outbox.setOrgCode(snapshot.orgCode());
        outbox.setOrderId(snapshot.orderId());
        outbox.setOutOrderSn(snapshot.outOrderSn());
        outbox.setDeltaType(BALANCE_DELTA_TYPE_PRE_DEDUCT);
        // 与旧 payload 中 delta_amount=-orderPrice 符号一致；PRE_DEDUCT 为负，单位分。
        outbox.setDeltaAmount(-snapshot.orderPrice());
        outbox.setStatus(TaskStatus.INIT.name());
        outbox.setRetryCount(0);
        outbox.setMaxRetryCount(BALANCE_DELTA_MAX_RETRY_COUNT);
        outbox.setNextRetryTime(persistedAt);
        outbox.setLockedBy(null);
        outbox.setLockedUntil(null);
        outbox.setVersion(0L);
        outbox.setLastError("");
        outbox.setCreatedAt(persistedAt);
        outbox.setUpdatedAt(persistedAt);
        return outbox;
    }

    static boolean preDeductRecordRequired(OrderPersistAggregate aggregate) {
        PersistOrderSnapshot snapshot = aggregate.snapshot();
        return aggregate.dispatchOutboxRequired() && snapshot.balanceReserved();
    }

    private static boolean validationFailed(PersistOrderSnapshot snapshot) {
        return ValidationStatus.FAILED.name().equals(snapshot.validationStatus());
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
        return ValidationStatus.PASSED.name().equals(snapshot.validationStatus())
                ? LegacyOrderStatusCode.ACCEPTED.code()
                : LegacyOrderStatusCode.FAILED.code();
    }

    private static LocalDateTime persistedAt(OrderPersistAggregate aggregate) {
        /*
         * docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」
         * mc_order/recharge_order_ext 与拆表后的 outbox 使用 MySQL datetime 字面值，旧卡管库按 Asia/Shanghai
         * 读写和展示时间。这里将 Instant 转成本地业务时区的 LocalDateTime，避免补偿任务按 next_retry_time
         * 扫描时比真实时间偏移 8 小时。
         */
        return LocalDateTime.ofInstant(aggregate.persistedAt(), LEGACY_CARDSHOP_DB_ZONE);
    }
}
