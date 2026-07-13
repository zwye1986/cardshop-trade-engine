package com.cardshop.trade.engine.infrastructure.persistence;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.cardshop.trade.engine.application.recharge.OrderFulfillmentStatusUpdate;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentCandidate;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentCandidateSwitchCommand;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrder;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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
    /*
     * 参见详细设计「9.1 派单 Outbox」：失败渠道只用于当前订单的候选排除，不参与资金或终态判定。
     * 设置查询上限是为了避免异常订单被人工/定时器反复补流水后，在热派单路径全表扫描同一订单流水。
     */
    private static final int FAILED_FLOW_SCAN_LIMIT = 64;
    /*
     * 旧系统正常写入小写 fail；这里显式列出常见大小写和历史 FAILED 变体，降低测试库/人工补数在
     * 区分大小写排序规则下漏过滤失败渠道的概率。参见详细设计「9.1 派单 Outbox」。
     */
    private static final Set<String> FAILED_FLOW_STATUSES = Set.of(
            "fail",
            "Fail",
            "FAIL",
            "failed",
            "Failed",
            "FAILED");

    private final McOrderMapper orderMapper;
    private final RechargeOrderExtMapper extMapper;
    private final McOrderSupplierFlowMapper flowMapper;
    private final LegacyRechargeFulfillmentCandidateSelector candidateSelector;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public MybatisRechargeFulfillmentOrderAdapter(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            McOrderSupplierFlowMapper flowMapper,
            ObjectMapper objectMapper,
            Clock clock) {
        this(orderMapper, extMapper, flowMapper, null, null, objectMapper, clock);
    }

    public MybatisRechargeFulfillmentOrderAdapter(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            McOrderSupplierFlowMapper flowMapper,
            V1DepartProductReader departProductReader,
            V1ProductReader productReader,
            ObjectMapper objectMapper,
            Clock clock) {
        this.orderMapper = Objects.requireNonNull(orderMapper, "orderMapper must not be null");
        this.extMapper = Objects.requireNonNull(extMapper, "extMapper must not be null");
        this.flowMapper = Objects.requireNonNull(flowMapper, "flowMapper must not be null");
        this.candidateSelector = departProductReader == null || productReader == null
                ? null
                : new LegacyRechargeFulfillmentCandidateSelector(departProductReader, productReader);
        if (this.candidateSelector == null) {
            /*
             * 兼容构造器只供旧单元测试或非 MyBatis 替身使用。真实 MyBatis 自动配置已经强依赖商品 Mapper，
             * 因此生产不应走到这里；若局部上下文缺少商品 Reader，必须给出 warn，避免 failover 静默退化。
             */
            log.warn("供应商候选 failover 未启用：V1DepartProductReader 或 V1ProductReader 未装配，orderId 仅会尝试当前渠道");
        }
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
    public List<RechargeFulfillmentCandidate> findDispatchCandidates(RechargeFulfillmentOrder order) {
        if (candidateSelector == null) {
            return RechargeFulfillmentOrderPort.super.findDispatchCandidates(order);
        }
        /*
         * 先读取当前订单已失败渠道，再交给 LegacyRechargeFulfillmentCandidateSelector 统一执行：
         * 原商品/stock/sort 排序、健康直充过滤和失败渠道排除。这样 outbox 重试和同一次 worker 内 failover
         * 使用同一套候选规则，避免某个路径绕过受理侧校验。参见详细设计「9.1 派单 Outbox」。
         */
        Set<String> failedChannels = flowMapper.selectList(new LambdaQueryWrapper<McOrderSupplierFlowDO>()
                        .eq(McOrderSupplierFlowDO::getOrderId, order.orderId())
                        .in(McOrderSupplierFlowDO::getStatus, FAILED_FLOW_STATUSES)
                        .last("limit " + FAILED_FLOW_SCAN_LIMIT))
                .stream()
                .map(McOrderSupplierFlowDO::getChannel)
                .filter(channel -> channel != null && !channel.isBlank())
                .collect(Collectors.toSet());
        return candidateSelector.select(
                order.orgCode(),
                order.productId(),
                order.selectedProductId(),
                failedChannels);
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
    public void switchFulfillmentCandidate(RechargeFulfillmentCandidateSwitchCommand command) {
        /*
         * 切换候选时三个旧表快照字段必须同时覆盖。即使新候选 product_name 为空，也要写空值，
         * 否则会留下“produt_code/supplier_channel 已切换、product_name 仍是旧候选”的脏快照。
         * 参见详细设计「9.1 派单 Outbox」。
         */
        orderMapper.update(null, new LambdaUpdateWrapper<McOrderDO>()
                .eq(McOrderDO::getOrderId, command.orderId())
                .set(McOrderDO::getProductCode, command.selectedProductId())
                .set(McOrderDO::getSupplierChannel, command.supplierChannel())
                .set(McOrderDO::getProductName, value(command.productName()))
                .set(McOrderDO::getRemark, command.reason())
                .set(McOrderDO::getEditTime, now()));
    }

    @Override
    @Transactional
    public void updateFulfillmentStatus(OrderFulfillmentStatusUpdate update) {
        /*
         * supplierFlowId 对应旧表 mc_order.flow_id，用于运营后台按订单快速定位最后一次供应商流水。
         * 只有真实插入流水后才写入；频控等待、未找到订单等没有流水的状态更新保持 null，避免制造假关联。
         * 参见高可用方案「6.2 `mc_order` 字段说明」和详细设计「9.1 派单 Outbox」。
         */
        orderMapper.update(null, new LambdaUpdateWrapper<McOrderDO>()
                .eq(McOrderDO::getOrderId, update.orderId())
                .set(McOrderDO::getOrderStatus, update.orderStatus())
                .set(McOrderDO::getDataStatus, update.dataStatus())
                .set(update.supplierFlowId() != null, McOrderDO::getFlowId, update.supplierFlowId())
                .set(McOrderDO::getFailCode, update.failCode())
                .set(McOrderDO::getFailReason, update.failReason())
                .set(McOrderDO::getRemark, update.remark())
                .set(McOrderDO::getEditTime, now()));
        extMapper.update(null, new LambdaUpdateWrapper<RechargeOrderExtDO>()
                .eq(RechargeOrderExtDO::getOrderId, update.orderId())
                .set(RechargeOrderExtDO::getDispatchStatus, update.dispatchStatus().name())
                .set(RechargeOrderExtDO::getLastError, update.failReason())
                .set(RechargeOrderExtDO::getUpdatedAt, now()));
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
