package com.cardshop.trade.engine.infrastructure.persistence;

import static com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason.*;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cardshop.trade.engine.application.recharge.MinisoBalanceReleaseAuthorization;
import com.cardshop.trade.engine.application.recharge.MinisoWriteOffException;
import com.cardshop.trade.engine.application.recharge.OrderFulfillmentStatusUpdate;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrder;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.SupplierFlowSnapshot;
import com.cardshop.trade.engine.application.recharge.SupplierFlowStartCommand;
import com.cardshop.trade.engine.application.recharge.SupplierFlowUpdateCommand;
import com.cardshop.trade.engine.application.recharge.FulfillmentDispatchStatus;
import com.cardshop.trade.engine.domain.order.VoucherUseState;
import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartRecordDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeDispatchOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderSupplierFlowDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

/**
 * 基于 MyBatis 的供应商履约订单写库适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。该适配器负责
 * {@code mc_order}、{@code recharge_order_ext} 和 {@code mc_order_supplier_flow} 的状态同步。
 */
public class MybatisRechargeFulfillmentOrderAdapter implements RechargeFulfillmentOrderPort {

    private static final Logger log = LoggerFactory.getLogger(MybatisRechargeFulfillmentOrderAdapter.class);
    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");
    private static final int MINISO_COUPON_UNUSED = 0;
    private static final int MINISO_COUPON_USED = 1;
    private static final String MINISO_CHANNEL = "MINISO";
    private static final String DB_PERSISTED = "DB_PERSISTED";
    private static final String PRE_DEDUCT_RECORD_TYPE = "1";

    private final McOrderMapper orderMapper;
    private final RechargeOrderExtMapper extMapper;
    private final McOrderSupplierFlowMapper flowMapper;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final Supplier<MinisoReceiptStore> minisoReceipts;
    private final McDepartRecordMapper departRecords;
    private final RechargeDispatchOutboxMapper dispatchEvents;

    public MybatisRechargeFulfillmentOrderAdapter(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            McOrderSupplierFlowMapper flowMapper,
            ObjectMapper objectMapper,
            Clock clock) {
        this(orderMapper, extMapper, flowMapper, objectMapper, clock, null);
    }

    public MybatisRechargeFulfillmentOrderAdapter(McOrderMapper orderMapper, RechargeOrderExtMapper extMapper,
            McOrderSupplierFlowMapper flowMapper, ObjectMapper objectMapper, Clock clock,
            MinisoReceiptStore minisoReceipts) {
        this(orderMapper, extMapper, flowMapper, objectMapper, clock, () -> minisoReceipts, null, null);
    }

    /**
     * MINISO许可只用主SqlSession的Mapper；凭据Store延迟解析以适配晚装配，不因Job关闭而缺保护。
     * 原非MINISO写入语义不变，参见docs/交行直充业务系统详细设计.md「9.8」。
     */
    public MybatisRechargeFulfillmentOrderAdapter(McOrderMapper orderMapper, RechargeOrderExtMapper extMapper,
            McOrderSupplierFlowMapper flowMapper, ObjectMapper objectMapper, Clock clock,
            Supplier<MinisoReceiptStore> minisoReceipts, McDepartRecordMapper departRecords,
            RechargeDispatchOutboxMapper dispatchEvents) {
        this.minisoReceipts = Objects.requireNonNull(minisoReceipts);
        this.departRecords = departRecords;
        this.dispatchEvents = dispatchEvents;
        this.orderMapper = Objects.requireNonNull(orderMapper, "orderMapper must not be null");
        this.extMapper = Objects.requireNonNull(extMapper, "extMapper must not be null");
        this.flowMapper = Objects.requireNonNull(flowMapper, "flowMapper must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public Optional<RechargeFulfillmentOrder> findByOrderId(String orderId) {
        McOrderDO order = orderMapper.selectByOrderId(orderId);
        RechargeOrderExtDO ext = extMapper.selectByOrderId(orderId);
        if (order == null || ext == null) {
            return Optional.empty();
        }
        return Optional.of(new RechargeFulfillmentOrder(
                order.getOrderId(),
                order.getOutOrderId(),
                order.getOrgCode(),
                order.getAccount(),
                order.getParentId(),
                order.getProductCode(),
                order.getSupplierChannel(),
                order.getOrderPrice() == null ? 0L : order.getOrderPrice(),
                order.getCallbackUrl(),
                order.getOrderStatus() == null ? 0 : order.getOrderStatus(),
                ext.getPersistStatus(),
                ext.getValidationStatus(),
                Boolean.TRUE.equals(ext.getDispatchAllowed()),
                Boolean.TRUE.equals(ext.getBalanceReserved()),
                ext.getDispatchStatus(),
                ext.getVoucherInvalidTime() == null ? null
                        : ext.getVoucherInvalidTime().atZone(LEGACY_CARDSHOP_DB_ZONE).toInstant()));
    }

    @Override
    public Optional<SupplierFlowSnapshot> findLatestSupplierFlowByOrderId(String orderId) {
        McOrderSupplierFlowDO flow = flowMapper.selectOne(new LambdaQueryWrapper<McOrderSupplierFlowDO>()
                .eq(McOrderSupplierFlowDO::getOrderId, orderId)
                .orderByDesc(McOrderSupplierFlowDO::getId)
                .last("LIMIT 1"));
        if (flow == null || flow.getId() == null) {
            return Optional.empty();
        }
        return Optional.of(new SupplierFlowSnapshot(flow.getId(), flow.getStatus()));
    }

    @Override
    @Transactional
    public long insertSupplierFlowStart(SupplierFlowStartCommand command) {
        McOrderSupplierFlowDO flow = new McOrderSupplierFlowDO();
        flow.setOrderId(command.orderId());
        flow.setOutOrderNo(command.outOrderSn());
        flow.setChannel(command.channel());
        flow.setStatus(command.status());
        flow.setRemark(command.remark());
        flow.setRequestParam(json(command.requestPayload()));
        flow.setResponseResult("");
        flow.setCreateTime(now());
        flow.setUpdateTime(now());
        flowMapper.insert(flow);
        return flow.getId() == null ? 0L : flow.getId();
    }

    @Override
    @Transactional
    public void updateSupplierFlow(SupplierFlowUpdateCommand command) {
        McOrderSupplierFlowDO flow = new McOrderSupplierFlowDO();
        flow.setId(command.flowId());
        flow.setStatus(command.status());
        flow.setRemark(command.remark());
        flow.setResponseResult(json(Map.of(
                "response_code", value(command.responseCode()),
                "response_message", value(command.responseMessage()),
                "supplier_order_id", value(command.supplierOrderId()),
                "raw_response", value(command.rawResponse()))));
        flow.setUpdateTime(now());
        flowMapper.updateById(flow);
    }

    @Override
    @Transactional
    public void updateFulfillmentStatus(OrderFulfillmentStatusUpdate update) {
        validateInvalidTime(update);
        // 所有状态写前先锁主单；MINISO未携带receipt也必须经过相同最终门禁，参见详细设计9.8。
        var order = orderMapper.lockFulfillmentOrder(update.orderId());
        if (order != null && MINISO_CHANNEL.equals(order.getSupplierChannel())) {
            var ext = extMapper.lockFulfillmentOrder(update.orderId());
            validateMinisoOwnership(order, ext);
            if (isReleaseTerminal(update.dispatchStatus().name())) {
                authorizeMinisoTerminalRelease(update, null, order, ext);
                return;
            }
            if (authorization(order, ext).isPresent() || isReleaseTerminal(ext.getDispatchStatus())
                    || Objects.equals(order.getOrderStatus(), LegacyOrderStatusCode.FAILED.code())
                    || Objects.equals(order.getOrderStatus(), LegacyOrderStatusCode.REFUNDED.code())
                    || update.orderStatus() == LegacyOrderStatusCode.FAILED.code()
                    || update.orderStatus() == LegacyOrderStatusCode.REFUNDED.code()) {
                throw new MinisoWriteOffException(RELEASE_TERMINAL_CONFLICT);
            }
        }
        saveReceipt(update);
        writeFulfillmentStatus(update);
    }

    /** 只有当前可信MINISO订单可读取许可；部分字段损坏不能当作未授权。参见详细设计9.8。 */
    @Override
    @Transactional
    public Optional<MinisoBalanceReleaseAuthorization> findMinisoReleaseAuthorization(String orderId) {
        var order = orderMapper.lockFulfillmentOrder(orderId);
        var ext = extMapper.lockFulfillmentOrder(orderId);
        validateMinisoOwnership(order, ext);
        return authorization(order, ext);
    }

    /** 首次裁决锁内提交终态与授权，HTTP/Redis由后续Worker在锁外执行；参见详细设计9.8。 */
    @Override
    @Transactional
    public Optional<MinisoBalanceReleaseAuthorization> authorizeMinisoTerminalRelease(
            OrderFulfillmentStatusUpdate update, String sourceEventId) {
        validateInvalidTime(update);
        var order = orderMapper.lockFulfillmentOrder(update.orderId());
        var ext = extMapper.lockFulfillmentOrder(update.orderId());
        validateMinisoOwnership(order, ext);
        return authorizeMinisoTerminalRelease(update, sourceEventId, order, ext);
    }

    private Optional<MinisoBalanceReleaseAuthorization> authorizeMinisoTerminalRelease(
            OrderFulfillmentStatusUpdate update, String sourceEventId, McOrderDO order, RechargeOrderExtDO ext) {
        if (!matchesReleaseTerminal(update.dispatchStatus().name(), update.orderStatus())) {
            throw new MinisoWriteOffException(RELEASE_TERMINAL_CONFLICT);
        }
        var existing = authorization(order, ext);
        if (existing.isPresent()) {
            // 已提交授权优先；迟到核销不触发第二次裁决，也不重写eventId/amount/time。
            if (!update.dispatchStatus().name().equals(ext.getDispatchStatus())
                    || !Objects.equals(order.getOrderStatus(), update.orderStatus())) {
                throw new MinisoWriteOffException(RELEASE_TERMINAL_CONFLICT);
            }
            return existing;
        }
        if (isReleaseTerminal(ext.getDispatchStatus())
                || Objects.equals(order.getOrderStatus(), LegacyOrderStatusCode.FAILED.code())
                || Objects.equals(order.getOrderStatus(), LegacyOrderStatusCode.REFUNDED.code())) {
            // 历史终态无授权不能自动补认；发布前由人工核验，不能重放Redis回补。
            throw new MinisoWriteOffException(RELEASE_AUTHORIZATION_INCONSISTENT);
        }
        saveReceipt(update);
        if (VoucherUseState.REDEEMED.code().equals(ext.getVoucherUseState()) || ext.getVoucherUseTime() != null) {
            throw new MinisoWriteOffException(REDEEM_VOID_CONFLICT);
        }
        Objects.requireNonNull(minisoReceipts.get(), "MINISO receipt store required")
                .assertTerminalReleaseAllowed(update.orderId());
        if (!Boolean.TRUE.equals(ext.getBalanceReserved())) {
            // 原本未预扣才允许empty；已有扣款流水却丢失预扣标记不是无需退款的订单。
            if (!preDeductRecords(order.getOrderId()).isEmpty()) {
                throw new MinisoWriteOffException(RELEASE_AUTHORIZATION_INCONSISTENT);
            }
            validateSourceEvent(order, sourceEventId);
            writeFulfillmentStatus(update);
            return Optional.empty();
        }
        long amount = preDeductAmount(order);
        validateSourceEvent(order, sourceEventId);
        writeFulfillmentStatus(update);
        var authorizedAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        int rows = extMapper.update(null, new LambdaUpdateWrapper<RechargeOrderExtDO>()
                .eq(RechargeOrderExtDO::getOrderId, order.getOrderId())
                .isNull(RechargeOrderExtDO::getBalanceReleaseAuthorizedAt)
                .isNull(RechargeOrderExtDO::getBalanceReleaseEventId)
                .isNull(RechargeOrderExtDO::getBalanceReleaseAmount)
                .set(RechargeOrderExtDO::getBalanceReleaseAuthorizedAt,
                        LocalDateTime.ofInstant(authorizedAt, LEGACY_CARDSHOP_DB_ZONE))
                .set(RechargeOrderExtDO::getBalanceReleaseEventId, sourceEventId)
                .set(RechargeOrderExtDO::getBalanceReleaseAmount, amount));
        if (rows != 1) throw new MinisoWriteOffException(RELEASE_AUTHORIZATION_INCONSISTENT);
        log.info("MINISO terminal release authorized in transaction: orderId={}, orgCode={}, eventId={}, amount={}",
                order.getOrderId(), order.getOrgCode(), sourceEventId, amount);
        return Optional.of(new MinisoBalanceReleaseAuthorization(order.getOrderId(), order.getOrgCode(),
                order.getOutOrderId(), sourceEventId, amount, authorizedAt));
    }

    private Optional<MinisoBalanceReleaseAuthorization> authorization(McOrderDO order, RechargeOrderExtDO ext) {
        var time = ext.getBalanceReleaseAuthorizedAt();
        var event = ext.getBalanceReleaseEventId();
        var amount = ext.getBalanceReleaseAmount();
        if (time == null && event == null && amount == null) return Optional.empty();
        if (time == null || event == null || event.isBlank() || amount == null || amount <= 0
                || !matchesReleaseTerminal(ext.getDispatchStatus(), order.getOrderStatus())
                || amount != preDeductAmount(order)) {
            throw new MinisoWriteOffException(RELEASE_AUTHORIZATION_INCONSISTENT);
        }
        validateSourceEvent(order, event);
        return Optional.of(new MinisoBalanceReleaseAuthorization(order.getOrderId(), order.getOrgCode(),
                order.getOutOrderId(), event, amount, time.atZone(LEGACY_CARDSHOP_DB_ZONE).toInstant()));
    }

    private java.util.List<McDepartRecordDO> preDeductRecords(String orderId) {
        return Objects.requireNonNull(departRecords, "MINISO pre-deduction mapper required")
                .lockPreDeductRecords(orderId, PRE_DEDUCT_RECORD_TYPE);
    }

    private long preDeductAmount(McOrderDO order) {
        var records = preDeductRecords(order.getOrderId());
        if (records.size() != 1 || order.getOrderPrice() == null || order.getOrderPrice() <= 0
                || !order.getOrgCode().equals(records.getFirst().getOrgCode())
                || !order.getOrderPrice().equals(records.getFirst().getValue())) {
            throw new MinisoWriteOffException(RELEASE_AUTHORIZATION_INCONSISTENT);
        }
        return order.getOrderPrice();
    }

    private void validateSourceEvent(McOrderDO order, String eventId) {
        if (eventId == null || eventId.isBlank()) throw new MinisoWriteOffException(RELEASE_EVIDENCE_MISSING);
        var event = Objects.requireNonNull(dispatchEvents, "MINISO dispatch mapper required")
                .selectOne(new LambdaQueryWrapper<RechargeDispatchOutboxDO>()
                        .select(RechargeDispatchOutboxDO::getEventId, RechargeDispatchOutboxDO::getEventType,
                                RechargeDispatchOutboxDO::getOrderId, RechargeDispatchOutboxDO::getOrgCode,
                                RechargeDispatchOutboxDO::getOutOrderSn)
                        .eq(RechargeDispatchOutboxDO::getEventId, eventId).last("for update"));
        if (event == null || !order.getOrderId().equals(event.getOrderId())
                || !order.getOrgCode().equals(event.getOrgCode()) || !order.getOutOrderId().equals(event.getOutOrderSn())
                || !(OutboxEventType.INVALID_ORDER.name().equals(event.getEventType())
                        || OutboxEventType.SUPPLIER_DISPATCH.name().equals(event.getEventType())
                        || OutboxEventType.SUPPLIER_QUERY.name().equals(event.getEventType()))) {
            throw new MinisoWriteOffException(RELEASE_EVIDENCE_MISSING);
        }
    }

    private static void validateMinisoOwnership(McOrderDO order, RechargeOrderExtDO ext) {
        if (order == null || ext == null || !MINISO_CHANNEL.equals(order.getSupplierChannel())
                || !DB_PERSISTED.equals(ext.getPersistStatus()) || order.getOrgCode() == null
                || order.getOrgCode().isBlank() || order.getOutOrderId() == null || order.getOutOrderId().isBlank()
                || !order.getOrgCode().equals(ext.getOrgCode()) || !order.getOutOrderId().equals(ext.getOutOrderSn())
                || !order.getOrderId().equals(ext.getOrderId()) || ext.getBalanceReserved() == null) {
            throw new MinisoWriteOffException(OWNERSHIP_CONFLICT);
        }
    }

    private static boolean isReleaseTerminal(String status) {
        return FulfillmentDispatchStatus.CANCELLED.name().equals(status)
                || FulfillmentDispatchStatus.FAILED.name().equals(status);
    }

    /** 作废保留充值成功12，失败对应6，不能混写两套状态；参见详细设计9.6、9.8。 */
    private static boolean matchesReleaseTerminal(String status, Integer orderStatus) {
        return FulfillmentDispatchStatus.CANCELLED.name().equals(status)
                        && Objects.equals(orderStatus, LegacyOrderStatusCode.SUCCESS.code())
                || FulfillmentDispatchStatus.FAILED.name().equals(status)
                        && Objects.equals(orderStatus, LegacyOrderStatusCode.FAILED.code());
    }

    private static void validateInvalidTime(OrderFulfillmentStatusUpdate update) {
        if (update.dispatchStatus() == FulfillmentDispatchStatus.CANCELLED && update.invalidTime() == null) {
            throw new IllegalArgumentException("cancelled order requires trusted invalid time");
        }
    }

    private void saveReceipt(OrderFulfillmentStatusUpdate update) {
        // 先保存并锁定可信订单凭据；本方法Spring事务保证凭据/主单/扩展状态整体提交或回滚。
        if (update.minisoReceipt() != null) {
            var store = minisoReceipts.get();
            if (store == null || !update.orderId().equals(update.minisoReceipt().orderId())) {
                throw new IllegalStateException("MINISO receipt store or order mismatch");
            }
            store.save(update.minisoReceipt());
        }
    }

    private void writeFulfillmentStatus(OrderFulfillmentStatusUpdate update) {
        /*
         * supplierFlowId 对应旧表 mc_order.flow_id，用于运营后台按订单快速定位最后一次供应商流水。
         * 只有真实插入流水后才写入；频控等待、未找到订单等没有流水的状态更新保持 null，避免制造假关联。
         * 参见高可用方案「6.2 `mc_order` 字段说明」和详细设计「9.1 派单 Outbox」。
         */
        int orderRows = orderMapper.update(null, new LambdaUpdateWrapper<McOrderDO>()
                .eq(McOrderDO::getOrderId, update.orderId())
                .set(McOrderDO::getOrderStatus, update.orderStatus())
                .set(McOrderDO::getDataStatus, update.dataStatus())
                .set(update.supplierFlowId() != null, McOrderDO::getFlowId, update.supplierFlowId())
                .set(McOrderDO::getFailCode, update.failCode())
                .set(McOrderDO::getFailReason, update.failReason())
                .set(McOrderDO::getRemark, update.remark())
                .set(McOrderDO::getEditTime, now()));
        var extUpdate = new LambdaUpdateWrapper<RechargeOrderExtDO>()
                .eq(RechargeOrderExtDO::getOrderId, update.orderId())
                .set(RechargeOrderExtDO::getDispatchStatus, update.dispatchStatus().name())
                .set(RechargeOrderExtDO::getLastError, update.failReason())
                .set(RechargeOrderExtDO::getUpdatedAt, now());
        // 订单充值成功不回退已核销/已作废券；作废时间与通知使用同一事实，见详细设计9.6。
        if (update.dispatchStatus() == FulfillmentDispatchStatus.SUCCESS) {
            String initialState = update.minisoReceipt() == null ? VoucherUseState.UNUSED.code()
                    : switch (update.minisoReceipt().couponStatus()) {
                        case MINISO_COUPON_UNUSED -> VoucherUseState.UNUSED.code();
                        case MINISO_COUPON_USED -> VoucherUseState.REDEEMED.code();
                        // TODO: 过期等状态待单独确认外部码表，不能冒充未使用或作废完成；参见详细设计9.6。
                        default -> "";
                    };
            extUpdate.setSql("voucher_use_state = CASE WHEN voucher_use_state IN ({0},{1}) "
                            + "THEN voucher_use_state ELSE {2} END",
                    VoucherUseState.REDEEMED.code(), VoucherUseState.CANCELLED.code(), initialState);
        } else if (update.dispatchStatus() == FulfillmentDispatchStatus.CANCELLED) {
            extUpdate.set(RechargeOrderExtDO::getVoucherUseState, VoucherUseState.CANCELLED.code())
                    .setSql("voucher_invalid_time = COALESCE(voucher_invalid_time, {0})",
                            LocalDateTime.ofInstant(update.invalidTime(), LEGACY_CARDSHOP_DB_ZONE)
                                    .withNano(0));
        }
        int extRows = extMapper.update(null, extUpdate);
        if ((update.minisoReceipt() != null || update.dispatchStatus() == FulfillmentDispatchStatus.CANCELLED)
                && (orderRows != 1 || extRows != 1)) {
            throw new IllegalStateException("order terminal update did not affect one owned row");
        }
    }

    @Override
    @Transactional
    public void markBalanceReleased(String orderId) {
        extMapper.update(null, new LambdaUpdateWrapper<RechargeOrderExtDO>()
                .eq(RechargeOrderExtDO::getOrderId, orderId)
                .set(RechargeOrderExtDO::getBalanceReserved, false)
                .set(RechargeOrderExtDO::getUpdatedAt, now()));
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("serialize supplier fulfillment payload failed", ex);
        }
    }

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(clock.instant(), LEGACY_CARDSHOP_DB_ZONE);
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }
}
