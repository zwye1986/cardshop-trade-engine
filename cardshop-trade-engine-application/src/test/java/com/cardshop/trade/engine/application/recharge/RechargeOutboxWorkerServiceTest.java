package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * D9/D10 outbox 履约 Worker 测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」、
 * 「20.13.11 作废设计」和「20.13.12 回调设计」。Worker 必须先抢占 outbox，再执行供应商动作；
 * 同账号限频、供应商流水、订单状态和回调任务都在 application 层用端口串联，避免 Controller 或
 * Persist Consumer 直接调用供应商。
 */
class RechargeOutboxWorkerServiceTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC);

    @Test
    void dispatchSuccessWritesSupplierFlowUpdatesOrderCreatesNotifyTaskAndCompletesOutbox() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("SUP-1001", "0", "success", "{\"status\":\"SUCCESS\"}"));
        FakeFrequencyPort frequencyPort = new FakeFrequencyPort(true);
        FakeNotifyTaskPort notifyTaskPort = new FakeNotifyTaskPort();
        FakeBalanceDeltaOutboxPort balanceDeltaOutboxPort = new FakeBalanceDeltaOutboxPort();
        FakeBalanceReleasePort balanceReleasePort = new FakeBalanceReleasePort(BalanceReservationReleaseResult.releasedSuccess());

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                frequencyPort,
                balanceDeltaOutboxPort,
                balanceReleasePort,
                notifyTaskPort,
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(outboxPort.successes).containsExactly("dispatch-ORDER-1");
        assertThat(orderPort.flowStarts).hasSize(1);
        assertThat(orderPort.flowStarts.getFirst().supplierRequestId()).isEqualTo("dispatch-ORDER-1");
        assertThat(orderPort.flowUpdates.getFirst().status()).isEqualTo("success");
        assertThat(orderPort.statusUpdates)
                .extracting(OrderFulfillmentStatusUpdate::dispatchStatus)
                .containsExactly(FulfillmentDispatchStatus.DISPATCHING, FulfillmentDispatchStatus.SUCCESS);
        assertThat(orderPort.statusUpdates.getLast().orderStatus()).isEqualTo(12);
        assertThat(frequencyPort.releasedOrderIds).containsExactly("ORDER-1");
        assertThat(notifyTaskPort.createdTasks).hasSize(1);
        assertThat(notifyTaskPort.createdTasks.getFirst().taskId()).isEqualTo("notify-order-ORDER-1-12");
        assertThat(notifyTaskPort.createdTasks.getFirst().payload())
                .containsEntry("orderStatus", "12")
                .containsEntry("sign", "signed-12");
        assertThat(balanceDeltaOutboxPort.commands).isEmpty();
    }

    @Test
    void dispatchIsDelayedWhenSameAccountFrequencyLockAlreadyExists() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "success", "{}"));
        FakeFrequencyPort frequencyPort = new FakeFrequencyPort(false);

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                frequencyPort,
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.failed()).isEqualTo(1);
        assertThat(supplierGateway.dispatchRequests).isEmpty();
        assertThat(orderPort.flowStarts).isEmpty();
        assertThat(orderPort.statusUpdates).singleElement().satisfies(update -> {
            assertThat(update.dispatchStatus()).isEqualTo(FulfillmentDispatchStatus.FREQUENCY_WAIT);
            assertThat(update.orderStatus()).isEqualTo(2);
            assertThat(update.dataStatus()).isEqualTo("FREQUENCY_WAIT");
        });
        assertThat(outboxPort.failures.getFirst().nextRetryDelay()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void dispatchProcessingKeepsOrderRechargingAndDoesNotCreateFinalNotifyTask() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.processing("SUP-1002", "PENDING", "accepted", "{\"status\":\"PROCESSING\"}"));
        FakeNotifyTaskPort notifyTaskPort = new FakeNotifyTaskPort();

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeFrequencyPort(true),
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                notifyTaskPort,
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(orderPort.statusUpdates.getLast().dispatchStatus()).isEqualTo(FulfillmentDispatchStatus.SUPPLIER_ACCEPTED);
        assertThat(orderPort.statusUpdates.getLast().orderStatus()).isEqualTo(13);
        assertThat(notifyTaskPort.createdTasks).isEmpty();
    }

    @Test
    void invalidOrderSuccessUpdatesLocalStateCreatesWriteOffNotifyAndCompletesOutbox() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.INVALID_ORDER));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("SUP-INVALID-1", "0", "invalid success", "{\"status\":\"SUCCESS\"}"));
        FakeNotifyTaskPort notifyTaskPort = new FakeNotifyTaskPort();
        FakeBalanceDeltaOutboxPort balanceDeltaOutboxPort = new FakeBalanceDeltaOutboxPort();
        FakeBalanceReleasePort balanceReleasePort = new FakeBalanceReleasePort(BalanceReservationReleaseResult.releasedSuccess());

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeFrequencyPort(true),
                balanceDeltaOutboxPort,
                balanceReleasePort,
                notifyTaskPort,
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(supplierGateway.invalidRequests).hasSize(1);
        assertThat(orderPort.statusUpdates)
                .extracting(OrderFulfillmentStatusUpdate::dispatchStatus)
                .containsExactly(FulfillmentDispatchStatus.CANCEL_REQUESTED, FulfillmentDispatchStatus.CANCELLED);
        assertThat(orderPort.statusUpdates.getLast().orderStatus()).isEqualTo(6);
        assertThat(notifyTaskPort.createdTasks.getFirst().notifyType()).isEqualTo("VOUCHER_STATE");
        assertThat(notifyTaskPort.createdTasks.getFirst().payload())
                .containsEntry("useState", "2")
                .containsEntry("sign", "signed-write-off");
        assertThat(outboxPort.successes).containsExactly("dispatch-ORDER-1");
        assertThat(balanceReleasePort.commands).singleElement().satisfies(command -> {
            assertThat(command.orderId()).isEqualTo("ORDER-1");
            assertThat(command.outOrderSn()).isEqualTo("OUT-1");
            assertThat(command.amount()).isEqualTo(5200L);
            assertThat(command.releaseType()).isEqualTo("RELEASE");
            assertThat(command.releaseEventId()).isEqualTo("dispatch-ORDER-1");
        });
        assertThat(balanceDeltaOutboxPort.commands).singleElement().satisfies(command -> {
            assertThat(command.orderId()).isEqualTo("ORDER-1");
            assertThat(command.orgCode()).isEqualTo("A36");
            assertThat(command.deltaAmount()).isEqualTo(5200L);
            assertThat(command.deltaType()).isEqualTo("RELEASE");
        });
    }

    @Test
    void dispatchFailureGeneratesReleaseBalanceDeltaFlushOutboxBeforeCompletingDispatchEvent() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.failed("SUP-FAIL-1", "E_SUP", "supplier rejected", "{\"status\":\"FAILED\"}"));
        FakeFrequencyPort frequencyPort = new FakeFrequencyPort(true);
        FakeBalanceDeltaOutboxPort balanceDeltaOutboxPort = new FakeBalanceDeltaOutboxPort();
        FakeBalanceReleasePort balanceReleasePort = new FakeBalanceReleasePort(BalanceReservationReleaseResult.releasedSuccess());

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                frequencyPort,
                balanceDeltaOutboxPort,
                balanceReleasePort,
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(orderPort.statusUpdates.getLast().dispatchStatus()).isEqualTo(FulfillmentDispatchStatus.FAILED);
        assertThat(outboxPort.successes).containsExactly("dispatch-ORDER-1");
        assertThat(frequencyPort.releasedOrderIds).containsExactly("ORDER-1");
        assertThat(orderPort.balanceReleasedOrderIds).containsExactly("ORDER-1");
        assertThat(balanceReleasePort.commands).singleElement().satisfies(command -> {
            assertThat(command.orderId()).isEqualTo("ORDER-1");
            assertThat(command.outOrderSn()).isEqualTo("OUT-1");
            assertThat(command.amount()).isEqualTo(5200L);
            assertThat(command.releaseType()).isEqualTo("RELEASE");
        });
        assertThat(balanceDeltaOutboxPort.commands).singleElement().satisfies(command -> {
            assertThat(command.orderId()).isEqualTo("ORDER-1");
            assertThat(command.outOrderSn()).isEqualTo("OUT-1");
            assertThat(command.deltaAmount()).isEqualTo(5200L);
            assertThat(command.deltaType()).isEqualTo("RELEASE");
            assertThat(command.reason()).contains("supplier rejected");
        });
    }

    @Test
    void dispatchRetryableSupplierFailureKeepsOutboxRetriableWithoutTerminalSideEffects() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.retryableFailed("", "500", "supplier http status not 2xx", "server error"));
        FakeFrequencyPort frequencyPort = new FakeFrequencyPort(true);
        FakeNotifyTaskPort notifyTaskPort = new FakeNotifyTaskPort();
        FakeBalanceDeltaOutboxPort balanceDeltaOutboxPort = new FakeBalanceDeltaOutboxPort();
        FakeBalanceReleasePort balanceReleasePort = new FakeBalanceReleasePort(BalanceReservationReleaseResult.releasedSuccess());

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                frequencyPort,
                balanceDeltaOutboxPort,
                balanceReleasePort,
                notifyTaskPort,
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.failed()).isEqualTo(1);
        assertThat(outboxPort.successes).isEmpty();
        assertThat(outboxPort.failures).singleElement().satisfies(failure -> {
            assertThat(failure.errorMessage()).contains("supplier http status not 2xx");
            assertThat(failure.nextRetryDelay()).isEqualTo(Duration.ofSeconds(5));
        });
        assertThat(orderPort.statusUpdates)
                .extracting(OrderFulfillmentStatusUpdate::dispatchStatus)
                .containsExactly(FulfillmentDispatchStatus.DISPATCHING);
        assertThat(orderPort.balanceReleasedOrderIds).isEmpty();
        assertThat(balanceReleasePort.commands).isEmpty();
        assertThat(balanceDeltaOutboxPort.commands).isEmpty();
        assertThat(notifyTaskPort.createdTasks).isEmpty();
        assertThat(frequencyPort.releasedOrderIds).isEmpty();
    }

    @Test
    void dispatchFailureRetriesOutboxWithoutBalanceDeltaWhenRedisReleaseFails() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.failed("SUP-FAIL-1", "E_SUP", "supplier rejected", "{\"status\":\"FAILED\"}"));
        FakeBalanceDeltaOutboxPort balanceDeltaOutboxPort = new FakeBalanceDeltaOutboxPort();
        FakeBalanceReleasePort balanceReleasePort = new FakeBalanceReleasePort(
                BalanceReservationReleaseResult.failed("REDIS_DOWN", "redis unavailable"));

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeFrequencyPort(true),
                balanceDeltaOutboxPort,
                balanceReleasePort,
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.failed()).isEqualTo(1);
        assertThat(outboxPort.successes).isEmpty();
        assertThat(outboxPort.failures).singleElement()
                .satisfies(failure -> assertThat(failure.errorMessage()).contains("redis unavailable"));
        assertThat(balanceReleasePort.commands).hasSize(1);
        assertThat(orderPort.balanceReleasedOrderIds).isEmpty();
        assertThat(balanceDeltaOutboxPort.commands).isEmpty();
    }

    @Test
    void dispatchOutboxRetryStillFinalizesBalanceReleaseWhenOrderAlreadyFailed() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(failedOrderAwaitingRelease());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "unused", "{}"));
        FakeBalanceDeltaOutboxPort balanceDeltaOutboxPort = new FakeBalanceDeltaOutboxPort();
        FakeBalanceReleasePort balanceReleasePort = new FakeBalanceReleasePort(BalanceReservationReleaseResult.alreadyReleasedSuccess());

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeFrequencyPort(true),
                balanceDeltaOutboxPort,
                balanceReleasePort,
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(supplierGateway.dispatchRequests).isEmpty();
        assertThat(outboxPort.successes).containsExactly("dispatch-ORDER-1");
        assertThat(balanceReleasePort.commands).hasSize(1);
        assertThat(orderPort.balanceReleasedOrderIds).containsExactly("ORDER-1");
        assertThat(balanceDeltaOutboxPort.commands).singleElement()
                .satisfies(command -> assertThat(command.deltaAmount()).isEqualTo(5200L));
    }

    @Test
    void retryExhaustedOutboxIsMovedToManualRequiredWithoutClaimingBusinessAction() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(exhaustedOutbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "unused", "{}"));

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeFrequencyPort(true),
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.failed()).isEqualTo(1);
        assertThat(outboxPort.claimedOrderIds).isEmpty();
        assertThat(supplierGateway.dispatchRequests).isEmpty();
        assertThat(outboxPort.manualRequired).singleElement()
                .satisfies(failure -> assertThat(failure.errorMessage()).contains("last permanent error"));
    }

    @Test
    void finalAutomaticFailureMovesOutboxToManualRequiredInsteadOfSchedulingRetry() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(lastRetryOutbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "unused", "{}"));

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeFrequencyPort(false),
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.failed()).isEqualTo(1);
        assertThat(outboxPort.failures).isEmpty();
        assertThat(outboxPort.manualRequired).singleElement()
                .satisfies(failure -> assertThat(failure.errorMessage()).contains("direct recharge frequency wait"));
    }

    private static RechargeOutboxWorkerSettings settings() {
        return new RechargeOutboxWorkerSettings(
                Duration.ofMinutes(2),
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                Duration.ofMinutes(30),
                Duration.ofSeconds(5));
    }

    private static RechargeOutboxEvent outbox(OutboxEventType eventType) {
        return new RechargeOutboxEvent(
                "dispatch-ORDER-1",
                eventType,
                "A36",
                "ORDER-1",
                "OUT-1",
                "{\"order_id\":\"ORDER-1\"}",
                0,
                10,
                7L,
                LocalDateTime.of(2026, 7, 8, 11, 20),
                "");
    }

    private static RechargeOutboxEvent exhaustedOutbox(OutboxEventType eventType) {
        return new RechargeOutboxEvent(
                "dispatch-ORDER-1",
                eventType,
                "A36",
                "ORDER-1",
                "OUT-1",
                "{\"order_id\":\"ORDER-1\"}",
                10,
                10,
                7L,
                LocalDateTime.of(2026, 7, 8, 11, 20),
                "last permanent error");
    }

    private static RechargeOutboxEvent lastRetryOutbox(OutboxEventType eventType) {
        return new RechargeOutboxEvent(
                "dispatch-ORDER-1",
                eventType,
                "A36",
                "ORDER-1",
                "OUT-1",
                "{\"order_id\":\"ORDER-1\"}",
                9,
                10,
                7L,
                LocalDateTime.of(2026, 7, 8, 11, 20),
                "");
    }

    private static RechargeFulfillmentOrder dispatchableOrder() {
        return new RechargeFulfillmentOrder(
                "ORDER-1",
                "OUT-1",
                "A36",
                "13800138000",
                "PARENT-1",
                "SKU-1",
                "FNK_CGS_BX",
                5200L,
                "https://merchant.example/notify",
                2,
                "DB_PERSISTED",
                "PASSED",
                true,
                true);
    }

    private static RechargeFulfillmentOrder failedOrderAwaitingRelease() {
        return new RechargeFulfillmentOrder(
                "ORDER-1",
                "OUT-1",
                "A36",
                "13800138000",
                "PARENT-1",
                "SKU-1",
                "FNK_CGS_BX",
                5200L,
                "https://merchant.example/notify",
                6,
                "DB_PERSISTED",
                "PASSED",
                true,
                true);
    }

    private static final class FakeOutboxPort implements RechargeOutboxPort {

        private final RechargeOutboxEvent event;
        private final List<String> successes = new ArrayList<>();
        private final List<OutboxFailure> failures = new ArrayList<>();
        private final List<OutboxFailure> manualRequired = new ArrayList<>();
        private final List<String> claimedOrderIds = new ArrayList<>();

        private FakeOutboxPort(RechargeOutboxEvent event) {
            this.event = event;
        }

        @Override
        public List<RechargeOutboxEvent> selectDueForShard(int shardTotal, int shardIndex, int limit) {
            return List.of(event);
        }

        @Override
        public Optional<RechargeOutboxEvent> claim(RechargeOutboxEvent event, String workerId, Duration lease) {
            claimedOrderIds.add(event.orderId());
            return Optional.of(event.claimedCopy());
        }

        @Override
        public void markSuccess(RechargeOutboxEvent event) {
            successes.add(event.eventId());
        }

        @Override
        public void markFailed(RechargeOutboxEvent event, String errorMessage, Duration nextRetryDelay) {
            failures.add(new OutboxFailure(event.eventId(), errorMessage, nextRetryDelay));
        }

        @Override
        public void markManualRequired(RechargeOutboxEvent event, String errorMessage) {
            manualRequired.add(new OutboxFailure(event.eventId(), errorMessage, Duration.ZERO));
        }
    }

    private static final class FakeFulfillmentOrderPort implements RechargeFulfillmentOrderPort {

        private final RechargeFulfillmentOrder order;
        private final List<SupplierFlowStartCommand> flowStarts = new ArrayList<>();
        private final List<SupplierFlowUpdateCommand> flowUpdates = new ArrayList<>();
        private final List<OrderFulfillmentStatusUpdate> statusUpdates = new ArrayList<>();
        private final List<String> balanceReleasedOrderIds = new ArrayList<>();

        private FakeFulfillmentOrderPort(RechargeFulfillmentOrder order) {
            this.order = order;
        }

        @Override
        public Optional<RechargeFulfillmentOrder> findByOrderId(String orderId) {
            return Optional.of(order);
        }

        @Override
        public long insertSupplierFlowStart(SupplierFlowStartCommand command) {
            flowStarts.add(command);
            return 99L;
        }

        @Override
        public void updateSupplierFlow(SupplierFlowUpdateCommand command) {
            flowUpdates.add(command);
        }

        @Override
        public void updateFulfillmentStatus(OrderFulfillmentStatusUpdate update) {
            statusUpdates.add(update);
        }

        @Override
        public void markBalanceReleased(String orderId) {
            balanceReleasedOrderIds.add(orderId);
        }
    }

    private static final class FakeSupplierGatewayPort implements RechargeSupplierGatewayPort {

        private final SupplierGatewayResponse response;
        private final List<SupplierDispatchGatewayRequest> dispatchRequests = new ArrayList<>();
        private final List<SupplierInvalidGatewayRequest> invalidRequests = new ArrayList<>();

        private FakeSupplierGatewayPort(SupplierGatewayResponse response) {
            this.response = response;
        }

        @Override
        public SupplierGatewayResponse dispatch(SupplierDispatchGatewayRequest request) {
            dispatchRequests.add(request);
            return response;
        }

        @Override
        public SupplierGatewayResponse invalid(SupplierInvalidGatewayRequest request) {
            invalidRequests.add(request);
            return response;
        }
    }

    private static final class FakeFrequencyPort implements DirectRechargeFrequencyPort {

        private final boolean acquired;
        private final List<String> releasedOrderIds = new ArrayList<>();

        private FakeFrequencyPort(boolean acquired) {
            this.acquired = acquired;
        }

        @Override
        public boolean acquire(String orgCode, String account, String orderId, Duration ttl) {
            return acquired;
        }

        @Override
        public void releaseIfOwned(String orgCode, String account, String orderId, Duration minimumHold) {
            releasedOrderIds.add(orderId);
        }
    }

    private static final class FakeNotifyTaskPort implements RechargeNotifyTaskPort {

        private final List<RechargeNotifyTaskDraft> createdTasks = new ArrayList<>();

        @Override
        public void insertIfAbsent(RechargeNotifyTaskDraft task) {
            createdTasks.add(task);
        }
    }

    private static final class FakeBalanceDeltaOutboxPort implements BalanceDeltaFlushOutboxPort {

        private final List<BalanceDeltaFlushOutboxCommand> commands = new ArrayList<>();

        @Override
        public void enqueueBalanceDeltaFlush(BalanceDeltaFlushOutboxCommand command) {
            commands.add(command);
        }
    }

    private static final class FakeBalanceReleasePort implements BalanceReservationReleasePort {

        private final BalanceReservationReleaseResult result;
        private final List<BalanceReservationReleaseCommand> commands = new ArrayList<>();

        private FakeBalanceReleasePort(BalanceReservationReleaseResult result) {
            this.result = result;
        }

        @Override
        public BalanceReservationReleaseResult release(BalanceReservationReleaseCommand command) {
            commands.add(command);
            return result;
        }
    }

    private static final class FixedNotifyPayloadSigner implements MerchantNotifyPayloadSigner {

        @Override
        public Map<String, String> signOrderStatus(RechargeFulfillmentOrder order, String orderStatus, Instant now) {
            return Map.of(
                    "orderId", order.orderId(),
                    "outOrderSn", order.outOrderSn(),
                    "orderStatus", orderStatus,
                    "sign", "signed-" + orderStatus);
        }

        @Override
        public Map<String, String> signWriteOff(RechargeFulfillmentOrder order, Instant invalidTime) {
            return Map.of(
                    "orderId", order.orderId(),
                    "outOrderSn", order.outOrderSn(),
                    "useState", "2",
                    "sign", "signed-write-off");
        }
    }

    private record OutboxFailure(String eventId, String errorMessage, Duration nextRetryDelay) {
    }
}
