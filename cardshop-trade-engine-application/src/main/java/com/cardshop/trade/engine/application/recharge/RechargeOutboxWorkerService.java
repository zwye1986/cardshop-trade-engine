package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
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
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」、
 * 「9.2 作废设计」和「9.3 回调设计」。本服务只负责编排端口：扫描并抢占 outbox、
 * 调用供应商 Gateway、回写订单/流水、生成回调任务；具体 MySQL、Redis 和 HTTP 实现都在基础设施层。
 */
public class RechargeOutboxWorkerService {

    private static final Logger log = LoggerFactory.getLogger(RechargeOutboxWorkerService.class);

    private static final int ORDER_STATUS_ACCEPTED = LegacyOrderStatusCode.ACCEPTED.code();
    private static final int ORDER_STATUS_FAILED = LegacyOrderStatusCode.FAILED.code();
    private static final int ORDER_STATUS_SUCCESS = LegacyOrderStatusCode.SUCCESS.code();
    private static final int ORDER_STATUS_SUPPLIER_ACCEPTED = LegacyOrderStatusCode.SUPPLIER_ACCEPTED.code();
    private static final String DATA_STATUS_PROCESSING = "PROCESSING";
    private static final String DATA_STATUS_PROCESSED = "PROCESSED";
    private static final String DATA_STATUS_FREQUENCY_WAIT = "FREQUENCY_WAIT";
    private static final int DEFAULT_NOTIFY_MAX_RETRY_COUNT = 10;

    private final RechargeOutboxPort outboxPort;
    private final RechargeFulfillmentOrderPort orderPort;
    private final RechargeSupplierGatewayPort supplierGatewayPort;
    private final SupplierQueryOutboxPort supplierQueryOutboxPort;
    private final DirectRechargeFrequencyPort frequencyPort;
    private final BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort;
    private final BalanceReservationReleasePort balanceReservationReleasePort;
    private final RechargeNotifyTaskPort notifyTaskPort;
    private final MerchantNotifyPayloadSigner notifyPayloadSigner;
    private final Clock clock;
    private final RechargeOutboxWorkerSettings settings;

    /**
     * 保留旧构造签名供已有 slice/unit test 使用；真实履约 Bean 必须注入查单 outbox 端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」：如果该兼容构造器被用于真实
     * 运行时，供应商处理中会显式失败并进入派单事件重试，不会静默丢失查单事件。
     */
    public RechargeOutboxWorkerService(
            RechargeOutboxPort outboxPort,
            RechargeFulfillmentOrderPort orderPort,
            RechargeSupplierGatewayPort supplierGatewayPort,
            DirectRechargeFrequencyPort frequencyPort,
            BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort,
            BalanceReservationReleasePort balanceReservationReleasePort,
            RechargeNotifyTaskPort notifyTaskPort,
            MerchantNotifyPayloadSigner notifyPayloadSigner,
            Clock clock,
            RechargeOutboxWorkerSettings settings) {
        this(
                outboxPort,
                orderPort,
                supplierGatewayPort,
                command -> {
                    throw new IllegalStateException("SupplierQueryOutboxPort is required for processing orders");
                },
                frequencyPort,
                balanceDeltaFlushOutboxPort,
                balanceReservationReleasePort,
                notifyTaskPort,
                notifyPayloadSigner,
                clock,
                settings);
    }

    public RechargeOutboxWorkerService(
            RechargeOutboxPort outboxPort,
            RechargeFulfillmentOrderPort orderPort,
            RechargeSupplierGatewayPort supplierGatewayPort,
            SupplierQueryOutboxPort supplierQueryOutboxPort,
            DirectRechargeFrequencyPort frequencyPort,
            BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort,
            BalanceReservationReleasePort balanceReservationReleasePort,
            RechargeNotifyTaskPort notifyTaskPort,
            MerchantNotifyPayloadSigner notifyPayloadSigner,
            Clock clock,
            RechargeOutboxWorkerSettings settings) {
        this.outboxPort = Objects.requireNonNull(outboxPort, "outboxPort must not be null");
        this.orderPort = Objects.requireNonNull(orderPort, "orderPort must not be null");
        this.supplierGatewayPort = Objects.requireNonNull(supplierGatewayPort, "supplierGatewayPort must not be null");
        this.supplierQueryOutboxPort = Objects.requireNonNull(
                supplierQueryOutboxPort, "supplierQueryOutboxPort must not be null");
        this.frequencyPort = Objects.requireNonNull(frequencyPort, "frequencyPort must not be null");
        this.balanceDeltaFlushOutboxPort = Objects.requireNonNull(
                balanceDeltaFlushOutboxPort, "balanceDeltaFlushOutboxPort must not be null");
        this.balanceReservationReleasePort = Objects.requireNonNull(
                balanceReservationReleasePort, "balanceReservationReleasePort must not be null");
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
            if (event.retryExhausted()) {
                outboxPort.markManualRequired(event, retryExhaustedMessage(event));
                result = result.add(RechargeOutboxBatchResult.Outcome.FAILED);
                continue;
            }
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
                case SUPPLIER_QUERY -> handleSupplierQuery(event);
                case INVALID_ORDER -> handleInvalidOrder(event);
                default -> {
                    String reason = "unsupported outbox event type: " + event.eventType();
                    markFailedOrManualRequired(event, reason, settings.retryBackoff());
                    yield RechargeOutboxBatchResult.Outcome.FAILED;
                }
            };
        } catch (Exception ex) {
            log.error("outbox事件处理异常 eventId={}, eventType={}, orderId={}",
                    event.eventId(), event.eventType(), event.orderId(), ex);
            markFailedOrManualRequired(event, ex.getMessage(), settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
    }

    private RechargeOutboxBatchResult.Outcome handleSupplierDispatch(RechargeOutboxEvent event) {
        RechargeFulfillmentOrder order = orderPort.findByOrderId(event.orderId()).orElse(null);
        if (order == null) {
            markFailedOrManualRequired(event, "order not found for dispatch", settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        if (!order.dispatchable()) {
            if (order.terminalFailureAwaitingBalanceRelease()) {
                return completeTerminalBalanceRelease(event, order, "供应商失败终态补偿释放预扣");
            }
            if (order.orderStatus() == ORDER_STATUS_SUPPLIER_ACCEPTED) {
                /*
                 * 派单返回处理中后，如果应用在“查单事件插入”和“派单事件成功回写”之间宕机，
                 * 恢复时订单已是供应商受理态。这里再次按稳定 event_id 补写查单事件，避免该窗口丢失后续终态推进。
                 */
                enqueueSupplierQuery(event, order);
            }
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        if (!frequencyPort.acquire(order.orgCode(), order.account(), order.orderId(), settings.frequencyLockTtl())) {
            orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                    order.orderId(),
                    FulfillmentDispatchStatus.FREQUENCY_WAIT,
                    ORDER_STATUS_ACCEPTED,
                    DATA_STATUS_FREQUENCY_WAIT,
                    null,
                    null,
                    null,
                    "同账号存在直充处理中订单，请稍后重试"));
            markFailedOrManualRequired(event, "direct recharge frequency wait", settings.frequencyRetryDelay());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }

        orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                order.orderId(),
                FulfillmentDispatchStatus.DISPATCHING,
                ORDER_STATUS_ACCEPTED,
                DATA_STATUS_PROCESSING,
                null,
                null,
                null,
                "供应商派单中"));
        /*
         * 参见详细设计「9.1 派单 Outbox」：新链路严格使用受理阶段按接口 product_id 精确匹配出的
         * `selected_product_id/supplier_channel`。供应商业务失败不能切到同父商品的其它供应商，否则会让
         * 订单金额、退款金额、商品类型校验和实际履约商品再次错位。
         */
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
                createOrderStatusNotifyTask(order, LegacyOrderStatusCode.SUCCESS);
                frequencyPort.releaseIfOwned(
                        order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
                outboxPort.markSuccess(event);
                return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
            }
            case PROCESSING -> {
                orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                        order.orderId(),
                        FulfillmentDispatchStatus.SUPPLIER_ACCEPTED,
                        ORDER_STATUS_SUPPLIER_ACCEPTED,
                        DATA_STATUS_PROCESSING,
                        flowId,
                        null,
                        null,
                        "供应商已受理，等待最终结果"));
                enqueueSupplierQuery(event, order);
                outboxPort.markSuccess(event);
                return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
            }
            case FAILED -> {
                return finishDispatchFailure(
                        event,
                        order,
                        flowId,
                        response.responseCode(),
                        response.responseMessage());
            }
            case RETRYABLE_FAILED -> {
                /*
                 * docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」要求供应商派单具备退避重试。
                 * HTTP 5xx、超时、连接异常属于“本次调用不可确认”，不是供应商明确拒绝；这里不能释放
                 * Redis reservation、不能生成失败回调，也不能把 outbox 标成 SUCCESS。保持订单处于
                 * DISPATCHING/PROCESSING，由 outbox 下次重试用同一 event_id 幂等号继续推进。
                 */
                markFailedOrManualRequired(event, response.responseMessage(), settings.retryBackoff());
                return RechargeOutboxBatchResult.Outcome.FAILED;
            }
            default -> throw new IllegalStateException("unsupported supplier status: " + response.status());
        }
    }

    private RechargeOutboxBatchResult.Outcome finishDispatchFailure(
            RechargeOutboxEvent event,
            RechargeFulfillmentOrder order,
            Long flowId,
            String failCode,
            String failReason) {
        orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                order.orderId(),
                FulfillmentDispatchStatus.FAILED,
                ORDER_STATUS_FAILED,
                DATA_STATUS_PROCESSED,
                flowId,
                failCode,
                failReason,
                failReason));
        createOrderStatusNotifyTask(order, LegacyOrderStatusCode.FAILED);
        if (!releaseBalanceReservationAndEnqueueDelta(event, order, failReason)) {
            frequencyPort.releaseIfOwned(
                    order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        frequencyPort.releaseIfOwned(
                order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
        outboxPort.markSuccess(event);
        return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
    }

    /**
     * 处理供应商处理中订单的异步查单事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和
     * {@code docs/交行直充业务系统高可用方案设计.md}「17.1 任务清单」。查单仍返回处理中或传输层不可确认
     * 结果时只退避当前查单事件；只有供应商明确给出成功/失败，才复用派单终态副作用，包括回调、频控释放和余额释放。
     */
    private RechargeOutboxBatchResult.Outcome handleSupplierQuery(RechargeOutboxEvent event) {
        RechargeFulfillmentOrder order = orderPort.findByOrderId(event.orderId()).orElse(null);
        if (order == null) {
            markFailedOrManualRequired(event, "order not found for supplier query", settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        if (order.terminalFailureAwaitingBalanceRelease()) {
            createOrderStatusNotifyTask(order, LegacyOrderStatusCode.FAILED);
            return completeTerminalBalanceRelease(event, order, "供应商查单终态补偿释放预扣");
        }
        if (order.orderStatus() == ORDER_STATUS_FAILED) {
            createOrderStatusNotifyTask(order, LegacyOrderStatusCode.FAILED);
            frequencyPort.releaseIfOwned(
                    order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        if (order.orderStatus() == ORDER_STATUS_SUCCESS) {
            // 终态已写入但回调或 outbox 成功回写可能尚未完成，依赖 notify task/event_id 幂等重放副作用。
            createOrderStatusNotifyTask(order, LegacyOrderStatusCode.SUCCESS);
            frequencyPort.releaseIfOwned(
                    order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        if (order.orderStatus() != ORDER_STATUS_SUPPLIER_ACCEPTED) {
            // 订单已经由其他重试或人工流程推进终态，当前查单事件只需关闭，不能重复调用供应商。
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }

        SupplierQueryGatewayRequest request = queryRequest(event, order);
        long flowId = orderPort.insertSupplierFlowStart(new SupplierFlowStartCommand(
                order.orderId(),
                order.outOrderSn(),
                order.supplierChannel(),
                "new",
                "供应商查单中",
                request.supplierRequestId(),
                queryRequestPayload(request)));
        SupplierGatewayResponse response = supplierGatewayPort.query(request);
        orderPort.updateSupplierFlow(flowUpdate(flowId, response));
        return finishSupplierQuery(event, order, flowId, response);
    }

    private RechargeOutboxBatchResult.Outcome finishSupplierQuery(
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
                        response.responseCode(),
                        null,
                        "供应商查单确认成功"));
                createOrderStatusNotifyTask(order, LegacyOrderStatusCode.SUCCESS);
                frequencyPort.releaseIfOwned(
                        order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
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
                createOrderStatusNotifyTask(order, LegacyOrderStatusCode.FAILED);
                if (!releaseBalanceReservationAndEnqueueDelta(event, order, response.responseMessage())) {
                    frequencyPort.releaseIfOwned(
                            order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
                    return RechargeOutboxBatchResult.Outcome.FAILED;
                }
                frequencyPort.releaseIfOwned(
                        order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
                outboxPort.markSuccess(event);
                return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
            }
            case PROCESSING -> {
                orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                        order.orderId(),
                        FulfillmentDispatchStatus.SUPPLIER_ACCEPTED,
                        ORDER_STATUS_SUPPLIER_ACCEPTED,
                        DATA_STATUS_PROCESSING,
                        flowId,
                        response.responseCode(),
                        response.responseMessage(),
                        "供应商仍在处理中"));
                markFailedOrManualRequired(event, response.responseMessage(), settings.retryBackoff());
                return RechargeOutboxBatchResult.Outcome.FAILED;
            }
            case RETRYABLE_FAILED -> {
                markFailedOrManualRequired(event, response.responseMessage(), settings.retryBackoff());
                return RechargeOutboxBatchResult.Outcome.FAILED;
            }
            default -> throw new IllegalStateException("unsupported supplier query status: " + response.status());
        }
    }

    private RechargeOutboxBatchResult.Outcome handleInvalidOrder(RechargeOutboxEvent event) {
        RechargeFulfillmentOrder order = orderPort.findByOrderId(event.orderId()).orElse(null);
        if (order == null) {
            markFailedOrManualRequired(event, "order not found for invalid", settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        if (order.terminalFailureAwaitingBalanceRelease()) {
            return completeTerminalBalanceRelease(event, order, "供应商作废终态补偿释放预扣");
        }
        orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                order.orderId(),
                FulfillmentDispatchStatus.CANCEL_REQUESTED,
                ORDER_STATUS_SUPPLIER_ACCEPTED,
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
            if (!releaseBalanceReservationAndEnqueueDelta(event, order, "供应商作废成功")) {
                frequencyPort.releaseIfOwned(
                        order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
                return RechargeOutboxBatchResult.Outcome.FAILED;
            }
            frequencyPort.releaseIfOwned(
                    order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        if (response.status() == SupplierGatewayStatus.PROCESSING) {
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        markFailedOrManualRequired(event, response.responseMessage(), settings.retryBackoff());
        return RechargeOutboxBatchResult.Outcome.FAILED;
    }

    private RechargeOutboxBatchResult.Outcome completeTerminalBalanceRelease(
            RechargeOutboxEvent event,
            RechargeFulfillmentOrder order,
            String reason) {
        if (!releaseBalanceReservationAndEnqueueDelta(event, order, reason)) {
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        frequencyPort.releaseIfOwned(
                order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
        outboxPort.markSuccess(event);
        return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
    }

    private boolean releaseBalanceReservationAndEnqueueDelta(
            RechargeOutboxEvent event,
            RechargeFulfillmentOrder order,
            String reason) {
        /*
         * docs/交行直充业务系统高可用方案设计.md「14.6 订单最终失败/作废退款协议」
         * 要求终态失败后释放 Redis reservation，并生成正向 BALANCE_DELTA_FLUSH 沉淀回旧表余额。
         * releaseEventId 使用当前 outbox 事件 ID 保证 Redis Lua 幂等；若 Redis 回补失败，不能先写
         * MySQL 余额反向事件，否则 Redis 与旧表会继续扩大偏差，因此保留当前 outbox 重试入口。
         */
        BalanceReservationReleaseResult releaseResult = balanceReservationReleasePort.release(
                new BalanceReservationReleaseCommand(
                        order.orgCode(),
                        order.orderId(),
                        order.outOrderSn(),
                        order.orderPrice(),
                        "RELEASE",
                        event.eventId(),
                        reason == null ? "" : reason));
        if (!releaseResult.succeeded()) {
            String failReason = releaseResult.failReason().isBlank()
                    ? releaseResult.failCode()
                    : releaseResult.failReason();
            markFailedOrManualRequired(event, "release balance reservation failed: " + failReason, settings.retryBackoff());
            return false;
        }
        enqueueReleaseBalanceDelta(order, reason);
        orderPort.markBalanceReleased(order.orderId());
        return true;
    }

    private void markFailedOrManualRequired(
            RechargeOutboxEvent event,
            String errorMessage,
            java.time.Duration nextRetryDelay) {
        if (event.nextFailureReachesMaxRetry()) {
            outboxPort.markManualRequired(event, errorMessage);
            return;
        }
        outboxPort.markFailed(event, errorMessage, nextRetryDelay);
    }

    private static String retryExhaustedMessage(RechargeOutboxEvent event) {
        if (event.lastError().isBlank()) {
            return "outbox retry exhausted";
        }
        return "outbox retry exhausted: " + event.lastError();
    }

    private void enqueueReleaseBalanceDelta(RechargeFulfillmentOrder order, String reason) {
        /*
         * docs/交行直充业务系统详细设计.md「9.2 作废设计」要求作废/失败完成后生成反向
         * BALANCE_DELTA_FLUSH。这里仅写可靠事件，不在 Worker 线程直接改 mc_ext_depart.balance，
         * 保持 D12 余额刷账的 CAS 抢占和重试语义。
         */
        balanceDeltaFlushOutboxPort.enqueueBalanceDeltaFlush(new BalanceDeltaFlushOutboxCommand(
                order.orgCode(),
                order.orderId(),
                order.outOrderSn(),
                order.orderPrice(),
                "RELEASE",
                reason == null ? "" : reason));
    }

    private void createOrderStatusNotifyTask(RechargeFulfillmentOrder order, LegacyOrderStatusCode orderStatus) {
        String orderStatusCode = Integer.toString(orderStatus.code());
        Map<String, String> payload = notifyPayloadSigner.signOrderStatus(order, orderStatusCode, clock.instant());
        notifyTaskPort.insertIfAbsent(new RechargeNotifyTaskDraft(
                "notify-order-" + order.orderId() + "-" + orderStatusCode,
                "ORDER_STATUS",
                order.orgCode(),
                order.orderId(),
                order.outOrderSn(),
                order.notifyUrl(),
                orderStatusCode,
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

    private static SupplierQueryGatewayRequest queryRequest(RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        return new SupplierQueryGatewayRequest(
                event.eventId(),
                order.orderId(),
                order.outOrderSn(),
                order.orgCode(),
                order.account(),
                order.supplierChannel(),
                Map.of("outbox_payload", event.payload()));
    }

    private void enqueueSupplierQuery(RechargeOutboxEvent dispatchEvent, RechargeFulfillmentOrder order) {
        supplierQueryOutboxPort.enqueueSupplierQuery(new SupplierQueryOutboxCommand(
                supplierQueryEventId(order.orderId()),
                order.orgCode(),
                order.orderId(),
                order.outOrderSn(),
                order.account(),
                order.supplierChannel(),
                dispatchEvent.payload()));
    }

    private static String supplierQueryEventId(String orderId) {
        return "supplier-query-" + orderId;
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

    private static Map<String, Object> queryRequestPayload(SupplierQueryGatewayRequest request) {
        return Map.ofEntries(
                Map.entry("supplier_request_id", request.supplierRequestId()),
                Map.entry("order_id", request.orderId()),
                Map.entry("out_order_sn", request.outOrderSn()),
                Map.entry("org_code", request.orgCode()),
                Map.entry("account", request.account()),
                Map.entry("supplier_channel", request.supplierChannel()));
    }

    private static SupplierFlowUpdateCommand flowUpdate(long flowId, SupplierGatewayResponse response) {
        String flowStatus = switch (response.status()) {
            case SUCCESS -> "success";
            case PROCESSING -> "processing";
            case FAILED -> "fail";
            /*
             * mc_order_supplier_flow.status 是旧卡管 varchar(10)，DDL 注释只约定 new/success/fail/processing。
             * docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」中的可重试失败表示供应商结果未确认，
             * 不是业务终态失败；写旧表时保持 processing，详细错误原因放到 response_result/remark 和 outbox
             * last_error 中，避免因状态值超长导致 SUP-002 的退避重试被数据库兼容问题打断。
             */
            case RETRYABLE_FAILED -> "processing";
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
