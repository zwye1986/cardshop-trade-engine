package com.cardshop.trade.engine.infrastructure.persistence;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cardshop.trade.engine.application.recharge.OrderFulfillmentStatusUpdate;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrder;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.SupplierFlowSnapshot;
import com.cardshop.trade.engine.application.recharge.SupplierFlowStartCommand;
import com.cardshop.trade.engine.application.recharge.SupplierFlowUpdateCommand;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderSupplierFlowDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.springframework.transaction.annotation.Transactional;

/**
 * 基于 MyBatis 的供应商履约订单写库适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。该适配器负责
 * {@code mc_order}、{@code recharge_order_ext} 和 {@code mc_order_supplier_flow} 的状态同步。
 */
public class MybatisRechargeFulfillmentOrderAdapter implements RechargeFulfillmentOrderPort {

    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");

    private final McOrderMapper orderMapper;
    private final RechargeOrderExtMapper extMapper;
    private final McOrderSupplierFlowMapper flowMapper;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final MinisoReceiptStore minisoReceipts;

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
        this.minisoReceipts = minisoReceipts;
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
                Boolean.TRUE.equals(ext.getBalanceReserved())));
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
        // 先保存并锁定可信订单凭据；本方法Spring事务保证凭据/主单/扩展状态整体提交或回滚。
        if (update.minisoReceipt() != null) {
            if (minisoReceipts == null || !update.orderId().equals(update.minisoReceipt().orderId())) {
                throw new IllegalStateException("MINISO receipt store or order mismatch");
            }
            minisoReceipts.save(update.minisoReceipt());
        }
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
        int extRows = extMapper.update(null, new LambdaUpdateWrapper<RechargeOrderExtDO>()
                .eq(RechargeOrderExtDO::getOrderId, update.orderId())
                .set(RechargeOrderExtDO::getDispatchStatus, update.dispatchStatus().name())
                .set(RechargeOrderExtDO::getLastError, update.failReason())
                .set(RechargeOrderExtDO::getUpdatedAt, now()));
        if (update.minisoReceipt() != null && (orderRows != 1 || extRows != 1)) {
            throw new IllegalStateException("MINISO order terminal update did not affect one owned row");
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
