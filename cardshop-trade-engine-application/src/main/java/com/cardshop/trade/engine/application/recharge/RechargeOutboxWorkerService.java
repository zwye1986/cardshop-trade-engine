package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL outbox 履约 Worker。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」、
 * 「20.13.11 作废设计」和「20.13.12 回调设计」。本服务只负责编排端口：扫描并抢占 outbox、
 * 调用供应商 Gateway、回写订单/流水、生成回调任务；具体 MySQL、Redis 和 HTTP 实现都在基础设施层。
 */
public class RechargeOutboxWorkerService {

    private static final Logger log = LoggerFactory.getLogger(RechargeOutboxWorkerService.class);

    private static final int ORDER_STATUS_PROCESSING = 2;
    private static final int ORDER_STATUS_FAILED = 6;
    private static final int ORDER_STATUS_SUCCESS = 12;
    private static final int ORDER_STATUS_RECHARGING = 13;
    private static final String DATA_STATUS_PROCESSING = "PROCESSING";
    private static final String DATA_STATUS_PROCESSED = "PROCESSED";
    private static final String DATA_STATUS_FREQUENCY_WAIT = "FREQUENCY_WAIT";
    private static final int DEFAULT_NOTIFY_MAX_RETRY_COUNT = 10;

    private final RechargeOutboxPort outboxPort;
    private final RechargeFulfillmentOrderPort orderPort;
    private final RechargeSupplierGatewayPort supplierGatewayPort;
    private final DirectRechargeFrequencyPort frequencyPort;
    private final RechargeNotifyTaskPort notifyTaskPort;
    private final MerchantNotifyPayloadSigner notifyPayloadSigner;
    private final Clock clock;
    private final RechargeOutboxWorkerSettings settings;

    public RechargeOutboxWorkerService(
            RechargeOutboxPort outboxPort,
            RechargeFulfillmentOrderPort orderPort,
            RechargeSupplierGatewayPort supplierGatewayPort,
            DirectRechargeFrequencyPort frequencyPort,
            RechargeNotifyTaskPort notifyTaskPort,
            MerchantNotifyPayloadSigner notifyPayloadSigner,
            Clock clock,
            RechargeOutboxWorkerSettings settings) {
        this.outboxPort = Objects.requireNonNull(outboxPort, "outboxPort must not be null");
        this.orderPort = Objects.requireNonNull(orderPort, "orderPort must not be null");
        this.supplierGatewayPort = Objects.requireNonNull(supplierGatewayPort, "supplierGatewayPort must not be null");
        this.frequencyPort = Objects.requireNonNull(frequencyPort, "frequencyPort must not be null");
        this.notifyTaskPort = Objects.requireNonNull(notifyTaskPort, "notifyTaskPort must not be null");
        this.notifyPayloadSigner = Objects.requireNonNull(notifyPayloadSigner, "notifyPayloadSigner must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
    }

    /**
     * 扫描并处理一个 outbox 分片。
     *
     * <p>任何降级、重试或跳过都必须显式回写 outbox，避免事件长期停留在 PROCESSING。
     */
    public RechargeOutboxBatchResult processDue(int shardTotal, int shardIndex, int limit, String workerId) {
        var events = outboxPort.selectDueForShard(shardTotal, shardIndex, limit);
        RechargeOutboxBatchResult result = RechargeOutboxBatchResult.empty().scanned(events.size());
        for (RechargeOutboxEvent event : events) {
            var claimed = outboxPort.claim(event, workerId, settings.claimLease());
            if (claimed.isEmpty()) {
                result = result.add(RechargeOutboxBatchResult.Outcome.SKIPPED);
                continue;
            }
            result = result.claimedOne();
            result = result.add(handleClaimed(claimed.get()));
        }
        return result;
    }

    private RechargeOutboxBatchResult.Outcome handleClaimed(RechargeOutboxEvent event) {
        try {
            return switch (event.eventType()) {
                case SUPPLIER_DISPATCH -> handleSupplierDispatch(event);
                case INVALID_ORDER -> handleInvalidOrder(event);
                default -> {
                    String reason = "unsupported outbox event type: " + event.eventType();
                    outboxPort.markFailed(event, reason, settings.retryBackoff());
                    yield RechargeOutboxBatchResult.Outcome.FAILED;
                }
            };
        } catch (Exception ex) {
            log.error("outbox事件处理异常 eventId={}, eventType={}, orderId={}",
                    event.eventId(), event.eventType(), event.orderId(), ex);
            outboxPort.markFailed(event, ex.getMessage(), settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
    }

    private RechargeOutboxBatchResult.Outcome handleSupplierDispatch(RechargeOutboxEvent event) {
        RechargeFulfillmentOrder order = orderPort.findByOrderId(event.orderId()).orElse(null);
        if (order == null) {
            outboxPort.markFailed(event, "order not found for dispatch", settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        if (!order.dispatchable()) {
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        if (!frequencyPort.acquire(order.orgCode(), order.account(), order.orderId(), settings.frequencyLockTtl())) {
            orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                    order.orderId(),
                    FulfillmentDispatchStatus.FREQUENCY_WAIT,
                    ORDER_STATUS_PROCESSING,
                    DATA_STATUS_FREQUENCY_WAIT,
                    null,
                    null,
                    null,
                    "同账号存在直充处理中订单，请稍后重试"));
            outboxPort.markFailed(event, "direct recharge frequency wait", settings.frequencyRetryDelay());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }

        orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                order.orderId(),
                FulfillmentDispatchStatus.DISPATCHING,
                ORDER_STATUS_PROCESSING,
                DATA_STATUS_PROCESSING,
                null,
                null,
                null,
                "供应商派单中"));
        SupplierDispatchGatewayRequest request = dispatchRequest(event, order);
        long flowId = orderPort.insertSupplierFlowStart(new SupplierFlowStartCommand(
                order.orderId(),
                order.outOrderSn(),
                order.supplierChannel(),
                "new",
                "准备下单",
                event.eventId(),
                requestPayload(request)));
        SupplierGatewayResponse response = supplierGatewayPort.dispatch(request);
        orderPort.updateSupplierFlow(flowUpdate(flowId, response));
        return finishDispatch(event, order, flowId, response);
    }

    private RechargeOutboxBatchResult.Outcome finishDispatch(
            RechargeOutboxEvent event,
            RechargeFulfillmentOrder order,
            long flowId,
            SupplierGatewayResponse response) {
        switch (response.status()) {
            case SUCCESS -> {
                orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                        order.orderId(),
                        FulfillmentDispatchStatus.SUCCESS,
                        ORDER_STATUS_SUCCESS,
                        DATA_STATUS_PROCESSED,
                        flowId,
                        null,
                        null,
                        "供应商充值成功"));
                createOrderStatusNotifyTask(order, "12");
                frequencyPort.releaseIfOwned(
                        order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
                outboxPort.markSuccess(event);
                return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
            }
            case PROCESSING -> {
                orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                        order.orderId(),
                        FulfillmentDispatchStatus.SUPPLIER_ACCEPTED,
                        ORDER_STATUS_RECHARGING,
                        DATA_STATUS_PROCESSING,
                        flowId,
                        null,
                        null,
                        "供应商已受理，等待最终结果"));
                outboxPort.markSuccess(event);
                return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
            }
            case FAILED -> {
                orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                        order.orderId(),
                        FulfillmentDispatchStatus.FAILED,
                        ORDER_STATUS_FAILED,
                        DATA_STATUS_PROCESSED,
                        flowId,
                        response.responseCode(),
                        response.responseMessage(),
                        response.responseMessage()));
                createOrderStatusNotifyTask(order, "6");
                frequencyPort.releaseIfOwned(
                        order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
                outboxPort.markSuccess(event);
                return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
            }
            default -> throw new IllegalStateException("unsupported supplier status: " + response.status());
        }
    }

    private RechargeOutboxBatchResult.Outcome handleInvalidOrder(RechargeOutboxEvent event) {
        RechargeFulfillmentOrder order = orderPort.findByOrderId(event.orderId()).orElse(null);
        if (order == null) {
            outboxPort.markFailed(event, "order not found for invalid", settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                order.orderId(),
                FulfillmentDispatchStatus.CANCEL_REQUESTED,
                ORDER_STATUS_RECHARGING,
                DATA_STATUS_PROCESSING,
                null,
                null,
                null,
                "供应商作废中"));
        SupplierGatewayResponse response = supplierGatewayPort.invalid(invalidRequest(event, order));
        if (response.status() == SupplierGatewayStatus.SUCCESS) {
            orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                    order.orderId(),
                    FulfillmentDispatchStatus.CANCELLED,
                    ORDER_STATUS_FAILED,
                    DATA_STATUS_PROCESSED,
                    null,
                    response.responseCode(),
                    response.responseMessage(),
                    "供应商作废成功"));
            createWriteOffNotifyTask(order);
            frequencyPort.releaseIfOwned(
                    order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        if (response.status() == SupplierGatewayStatus.PROCESSING) {
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        outboxPort.markFailed(event, response.responseMessage(), settings.retryBackoff());
        return RechargeOutboxBatchResult.Outcome.FAILED;
    }

    private void createOrderStatusNotifyTask(RechargeFulfillmentOrder order, String orderStatus) {
        Map<String, String> payload = notifyPayloadSigner.signOrderStatus(order, orderStatus, clock.instant());
        notifyTaskPort.insertIfAbsent(new RechargeNotifyTaskDraft(
                "notify-order-" + order.orderId() + "-" + orderStatus,
                "ORDER_STATUS",
                order.orgCode(),
                order.orderId(),
                order.outOrderSn(),
                order.notifyUrl(),
                orderStatus,
                payload,
                DEFAULT_NOTIFY_MAX_RETRY_COUNT,
                LocalDateTime.now(clock)));
    }

    private void createWriteOffNotifyTask(RechargeFulfillmentOrder order) {
        Map<String, String> payload = notifyPayloadSigner.signWriteOff(order, clock.instant());
        notifyTaskPort.insertIfAbsent(new RechargeNotifyTaskDraft(
                "notify-writeoff-" + order.orderId() + "-cancelled",
                "VOUCHER_STATE",
                order.orgCode(),
                order.orderId(),
                order.outOrderSn(),
                order.notifyUrl(),
                "CANCELLED",
                payload,
                DEFAULT_NOTIFY_MAX_RETRY_COUNT,
                LocalDateTime.now(clock)));
    }

    private static SupplierDispatchGatewayRequest dispatchRequest(RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        return new SupplierDispatchGatewayRequest(
                event.eventId(),
                order.orderId(),
                order.outOrderSn(),
                order.orgCode(),
                order.account(),
                order.productId(),
                order.selectedProductId(),
                order.supplierChannel(),
                order.orderPrice(),
                Map.of("outbox_payload", event.payload()));
    }

    private static SupplierInvalidGatewayRequest invalidRequest(RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        return new SupplierInvalidGatewayRequest(
                event.eventId(),
                order.orderId(),
                order.outOrderSn(),
                order.orgCode(),
                order.account(),
                order.supplierChannel(),
                Map.of("outbox_payload", event.payload()));
    }

    private static Map<String, Object> requestPayload(SupplierDispatchGatewayRequest request) {
        return Map.ofEntries(
                Map.entry("supplier_request_id", request.supplierRequestId()),
                Map.entry("order_id", request.orderId()),
                Map.entry("out_order_sn", request.outOrderSn()),
                Map.entry("org_code", request.orgCode()),
                Map.entry("account", request.account()),
                Map.entry("product_id", request.productId()),
                Map.entry("selected_product_id", request.selectedProductId()),
                Map.entry("supplier_channel", request.supplierChannel()),
                Map.entry("order_price", request.orderPrice()));
    }

    private static SupplierFlowUpdateCommand flowUpdate(long flowId, SupplierGatewayResponse response) {
        String flowStatus = switch (response.status()) {
            case SUCCESS -> "success";
            case PROCESSING -> "processing";
            case FAILED -> "fail";
        };
        return new SupplierFlowUpdateCommand(
                flowId,
                flowStatus,
                response.responseMessage(),
                response.responseCode(),
                response.responseMessage(),
                response.supplierOrderId(),
                response.rawResponse());
    }
}
