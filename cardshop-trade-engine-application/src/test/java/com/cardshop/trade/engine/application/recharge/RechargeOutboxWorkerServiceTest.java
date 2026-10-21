package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;

/**
 * D9/D10 outbox 履约 Worker 测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」、
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」、
 * 「9.3 作废设计」和「9.4 回调设计」。Worker 必须先抢占 outbox，再执行供应商动作；
 * 余额释放通过 BalanceDeltaFlushOutboxPort 入队（「9.2 余额 Delta 刷账 Outbox」）。
 * 同账号限频、供应商流水、订单状态和回调任务都在 application 层用端口串联，避免 Controller 或
 * Persist Consumer 直接调用供应商。
 */
class RechargeOutboxWorkerServiceTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC);

    @ParameterizedTest
    @EnumSource(value = OutboxEventType.class, names = {"BALANCE_DELTA_FLUSH", "BALANCE_RELEASE", "CACHE_REPAIR"})
    void minisoAuthorizationDoesNotMakeUnrelatedEventTypesFulfillmentTasks(OutboxEventType type) {
        var orders = new FakeFulfillmentOrderPort(minisoOrder(6, true, "FAILED"));
        orders.authorization = minisoAuthorization();
        var outbox = new FakeOutboxPort(outbox(type));
        var release = new IdempotentBalanceReleasePort();
        var notifications = new FakeNotifyTaskPort();
        var service = minisoWorker(outbox, orders,
                new FakeSupplierGatewayPort(SupplierGatewayResponse.success("unused", "200", "", "{}")),
                release, notifications, new FakeBalanceDeltaOutboxPort(), true);

        assertThat(service.processDue(1, 0, 10, "worker", List.of(type), 1).failed()).isEqualTo(1);
        assertThat(outbox.successes).isEmpty();
        assertThat(release.balanceIncrease).isZero();
        assertThat(orders.completedReleaseDeltas).isEmpty();
        assertThat(notifications.createdTasks).isEmpty();
    }

    @Test
    void authorizationCommittedAfterOrderSnapshotStillCreatesCancelledNotificationNotFailure() {
        var orders = new FakeFulfillmentOrderPort(minisoOrder(12, true, "SUCCESS"));
        orders.onAuthorizationRead = () -> {
            orders.authorization = minisoAuthorization();
            orders.ordersById.put("ORDER-1", minisoOrder(12, true, "CANCELLED"));
        };
        var outbox = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_QUERY));
        var notifications = new FakeNotifyTaskPort();
        var service = minisoWorker(outbox, orders,
                new FakeSupplierGatewayPort(SupplierGatewayResponse.success("unused", "200", "", "{}")),
                new IdempotentBalanceReleasePort(), notifications, new FakeBalanceDeltaOutboxPort(), true);

        assertThat(service.processDue(1, 0, 10, "worker", List.of(OutboxEventType.SUPPLIER_QUERY), 1).succeeded()).isEqualTo(1);
        assertThat(notifications.createdTasks).singleElement().satisfies(task -> {
            assertThat(task.bizStatus()).isEqualTo("CANCELLED");
            assertThat(task.payload()).containsEntry("invalidTime", CLOCK.instant().minusSeconds(120).toString());
        });
    }

    @ParameterizedTest
    @EnumSource(value = OutboxEventType.class,
            names = {"SUPPLIER_DISPATCH", "SUPPLIER_QUERY", "INVALID_ORDER"})
    void authorizedMinisoReleaseSurvivesLateRedemptionAndNotificationFailure(OutboxEventType type) {
        for (boolean cancelled : List.of(true, false)) {
            var orders = new FakeFulfillmentOrderPort(minisoOrder(cancelled ? 12 : 6, true,
                    cancelled ? "CANCELLED" : "FAILED"));
            orders.authorization = minisoAuthorization();
            var outbox = new FakeOutboxPort(outbox(type));
            var release = new IdempotentBalanceReleasePort();
            var notifications = new FakeNotifyTaskPort();
            notifications.failuresRemaining = 1;
            var gateway = new FakeSupplierGatewayPort(SupplierGatewayResponse.success("unused", "200", "", "{}"));
            var deltas = new FakeBalanceDeltaOutboxPort();
            var service = minisoWorker(outbox, orders, gateway, release, notifications, deltas, true);

            assertThat(service.processDue(1, 0, 10, "worker", List.of(type), 1).failed()).isEqualTo(1);
            assertThat(release.balanceIncrease).isEqualTo(137);
            assertThat(release.firstCommand.releaseEventId()).isEqualTo("event-A");
            assertThat(release.firstCommand.orgCode()).isEqualTo("A36");
            assertThat(release.firstCommand.orderId()).isEqualTo("ORDER-1");
            assertThat(release.firstCommand.outOrderSn()).isEqualTo("OUT-1");
            assertThat(release.firstCommand.releaseType()).isEqualTo("RELEASE");
            assertThat(orders.completedReleaseDeltas).containsOnlyKeys("balance-flush-release-ORDER-1");
            assertThat(outbox.successes).isEmpty();
            assertThat(outbox.manualRequired).isEmpty();

            assertThat(service.processDue(1, 0, 10, "worker", List.of(type), 1).succeeded()).isEqualTo(1);
            assertThat(release.balanceIncrease).isEqualTo(137);
            assertThat(orders.completedReleaseDeltas.get("balance-flush-release-ORDER-1"))
                    .isEqualTo(new BalanceDeltaFlushOutboxCommand("A36", "ORDER-1", "OUT-1", 137, "RELEASE", ""));
            assertThat(orders.authorization).isEqualTo(minisoAuthorization());
            assertThat(orders.findByOrderId("ORDER-1").orElseThrow().balanceReserved()).isFalse();
            assertThat(notifications.createdTasks).singleElement().satisfies(task -> {
                assertThat(task.bizStatus()).isEqualTo(cancelled ? "CANCELLED" : "6");
                if (cancelled) assertThat(task.payload()).containsEntry("invalidTime", CLOCK.instant().minusSeconds(120).toString());
            });
            assertThat(outbox.successes).containsExactly("dispatch-ORDER-1");
            assertThat(gateway.dispatchRequests).isEmpty();
            assertThat(gateway.queryRequests).isEmpty();
            assertThat(gateway.invalidRequests).isEmpty();
            assertThat(deltas.commands).isEmpty();
            assertThat(orders.balanceReleasedOrderIds).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(value = OutboxEventType.class,
            names = {"SUPPLIER_DISPATCH", "SUPPLIER_QUERY", "INVALID_ORDER"})
    void minisoTerminalWithoutAuthorizationCannotReleaseOrSilentlyComplete(OutboxEventType type) {
        for (boolean reserved : List.of(true, false)) {
            for (boolean cancelled : List.of(true, false)) {
                var orders = new FakeFulfillmentOrderPort(minisoOrder(cancelled ? 12 : 6, reserved,
                        cancelled ? "CANCELLED" : "FAILED"));
                var outbox = new FakeOutboxPort(outbox(type));
                var release = new IdempotentBalanceReleasePort();
                var notifications = new FakeNotifyTaskPort();
                var service = minisoWorker(outbox, orders,
                        new FakeSupplierGatewayPort(SupplierGatewayResponse.success("unused", "200", "", "{}")),
                        release, notifications, new FakeBalanceDeltaOutboxPort(), false);

                assertThat(service.processDue(1, 0, 10, "worker", List.of(type), 1).failed()).isEqualTo(1);
                assertThat(outbox.successes).isEmpty();
                assertThat(outbox.manualRequired).hasSize(1);
                assertThat(release.balanceIncrease).isZero();
                assertThat(orders.completedReleaseDeltas).isEmpty();
                assertThat(orders.authorization).isNull();
                assertThat(notifications.createdTasks).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = OutboxEventType.class,
            names = {"SUPPLIER_DISPATCH", "SUPPLIER_QUERY", "INVALID_ORDER"})
    void minisoNewTerminalUsesAtomicAuthorizationAndRecoversEveryFailureWindow(OutboxEventType type) {
        // 端口故障只模拟其合同；真实MySQL事务及Redis Lua断点由Task17验证（详细设计9.8）。
        for (String fault : List.of("before-authorization", "after-authorization", "redis-unavailable",
                "redis-response-lost", "completion", "outbox-success")) {
            var orders = new FakeFulfillmentOrderPort(minisoOrder(type == OutboxEventType.INVALID_ORDER ? 12
                    : type == OutboxEventType.SUPPLIER_QUERY ? 13 : 2, true, ""));
            orders.authorizationFault = fault;
            orders.completionFailures = fault.equals("completion") ? 1 : 0;
            var outbox = new FakeOutboxPort(outbox(type));
            outbox.successFailures = fault.equals("outbox-success") ? 1 : 0;
            var release = new IdempotentBalanceReleasePort();
            release.fault = fault;
            var response = (type == OutboxEventType.INVALID_ORDER
                    ? SupplierGatewayResponse.success("ORDER-1", "200", "ok", "{}")
                    : SupplierGatewayResponse.failed("ORDER-1", "UNVERIFIED", "failed with void receipt", "{}"))
                    .withMinisoReceipt(new MinisoFulfillmentReceipt("ORDER-1", "A36", 123, "TEST-C1",
                            "13800138000", "2026-07-01 00:00:00", "2026-12-01 00:00:00", 3));
            var gateway = new FakeSupplierGatewayPort(response);
            var service = minisoWorker(outbox, orders, gateway, release, new FakeNotifyTaskPort(),
                    new FakeBalanceDeltaOutboxPort(), false);

            assertThat(service.processDue(1, 0, 10, "worker", List.of(type), 1).failed()).isEqualTo(1);
            assertThat(outbox.successes).isEmpty();
            if (fault.equals("before-authorization")) {
                assertThat(orders.authorization).isNull();
                assertThat(release.balanceIncrease).isZero();
                assertThat(orders.completedReleaseDeltas).isEmpty();
            } else {
                assertThat(orders.authorization.sourceEventId()).isEqualTo("dispatch-ORDER-1");
                assertThat(orders.authorization.amount()).isEqualTo(137);
            }
            // 授权已提交的恢复中加入迟到核销；授权前故障则仍允许本次首次裁决。
            service = minisoWorker(outbox, orders, gateway, release, new FakeNotifyTaskPort(),
                    new FakeBalanceDeltaOutboxPort(), orders.authorization != null);
            assertThat(service.processDue(1, 0, 10, "worker", List.of(type), 1).succeeded()).isEqualTo(1);
            String sourceEventId = "dispatch-ORDER-1";
            if (type == OutboxEventType.SUPPLIER_DISPATCH && fault.equals("before-authorization")) {
                // 没有提交许可但已有调用流水，只能交接查单，不能重派；查单以自己的事件首次授权。
                assertThat(orders.authorization).isNull();
                assertThat(release.balanceIncrease).isZero();
                var queryEvent = new RechargeOutboxEvent("query-ORDER-1", OutboxEventType.SUPPLIER_QUERY,
                        "A36", "ORDER-1", "OUT-1", "{}", 0, 10, 0, LocalDateTime.now(CLOCK), "");
                service = minisoWorker(new FakeOutboxPort(queryEvent), orders, gateway, release,
                        new FakeNotifyTaskPort(), new FakeBalanceDeltaOutboxPort(), false);
                assertThat(service.processDue(1, 0, 10, "worker", List.of(queryEvent.eventType()), 1).succeeded()).isEqualTo(1);
                sourceEventId = "query-ORDER-1";
            }
            assertThat(release.balanceIncrease).isEqualTo(137);
            assertThat(orders.completedReleaseDeltas).hasSize(1);
            assertThat(orders.authorization.amount()).isEqualTo(137);
            assertThat(orders.authorization.sourceEventId()).isEqualTo(sourceEventId);
            assertThat(orders.findByOrderId("ORDER-1").orElseThrow().balanceReserved()).isFalse();
            assertThat(outbox.manualRequired).isEmpty();
        }
    }

    @Test
    void unprovenMinisoDispatchFailureQueriesOriginalOrderAndUnknownQueriesNeverAuthorizeEvenAtExhaustion() {
        for (SupplierGatewayResponse unknown : java.util.Arrays.asList(null,
                SupplierGatewayResponse.retryableFailed("ORDER-1", "UNKNOWN", "null body", "null"),
                SupplierGatewayResponse.retryableFailed("ORDER-1", "UNKNOWN", "empty data", "{\"data\":[]}"),
                SupplierGatewayResponse.retryableFailed("ORDER-1", "NETWORK", "network failed", ""),
                SupplierGatewayResponse.failed("ORDER-1", "UNVERIFIED", "unverified failure", "{}"))) {
            var orders = new FakeFulfillmentOrderPort(minisoOrder(2, true, ""));
            var outbox = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
            var gateway = new FakeSupplierGatewayPort(
                    SupplierGatewayResponse.failed("ORDER-1", "UNVERIFIED", "not proof of no issuance", "{}"), unknown);
            var queries = new FakeSupplierQueryOutboxPort();
            var release = new IdempotentBalanceReleasePort();
            var notifications = new FakeNotifyTaskPort();
            var service = new RechargeOutboxWorkerService(outbox, orders, gateway, queries, new FakeFrequencyPort(true),
                    new FakeBalanceDeltaOutboxPort(), release, notifications, new FixedNotifyPayloadSigner(), CLOCK, settings());

            assertThat(service.processDue(1, 0, 10, "worker").succeeded()).isEqualTo(1);
            assertThat(queries.commands).singleElement().satisfies(query -> assertThat(query.orderId()).isEqualTo("ORDER-1"));
            assertThat(orders.statusUpdates).noneMatch(update -> update.dispatchStatus() == FulfillmentDispatchStatus.FAILED);

            orders.ordersById.put("ORDER-1", minisoOrder(13, true, "SUPPLIER_ACCEPTED"));
            for (var event : List.of(outbox(OutboxEventType.SUPPLIER_QUERY), lastRetryOutbox(OutboxEventType.SUPPLIER_QUERY))) {
                var queryOutbox = new FakeOutboxPort(event);
                var queryWorker = minisoWorker(queryOutbox, orders, gateway, release, notifications,
                        new FakeBalanceDeltaOutboxPort(), false);
                assertThat(queryWorker.processDue(1, 0, 10, "worker", List.of(event.eventType()), 1).failed()).isEqualTo(1);
                assertThat(queryOutbox.successes).isEmpty();
                assertThat(queryOutbox.manualRequired).hasSize(event.nextFailureReachesMaxRetry() ? 1 : 0);
            }
            assertThat(gateway.queryRequests).allSatisfy(request -> assertThat(request.orderId()).isEqualTo("ORDER-1"));
            assertThat(orders.authorization).isNull();
            assertThat(orders.completedReleaseDeltas).isEmpty();
            assertThat(release.balanceIncrease).isZero();
            assertThat(notifications.createdTasks).isEmpty();
        }
    }

    @Test
    void invalidationWithoutMinisoReceiptQueriesOriginalOrderAndRetainsItsRecoveryTask() {
        for (int status : List.of(2, 12)) {
            for (String raw : List.of("null", "{\"code\":200,\"data\":[]}", "network-unavailable")) {
                var orders = new FakeFulfillmentOrderPort(minisoOrder(status, true, ""));
                var outbox = new FakeOutboxPort(lastRetryOutbox(OutboxEventType.INVALID_ORDER));
                var unknown = SupplierGatewayResponse.retryableFailed("ORDER-1", "UNKNOWN", "unconfirmed", raw);
                var gateway = new FakeSupplierGatewayPort(unknown);
                var release = new IdempotentBalanceReleasePort();
                var notifications = new FakeNotifyTaskPort();
                // 真实保护器在本地缺凭据时返回归属冲突，Worker仍只能查单补证据，不能绕过门禁作废/退款。
                var service = new RechargeOutboxWorkerService(outbox, orders, gateway, new FakeSupplierQueryOutboxPort(),
                        new FakeFrequencyPort(true), new FakeBalanceDeltaOutboxPort(), release, notifications,
                        new FixedNotifyPayloadSigner(), RechargeDispatchTimerPort.noop(), CLOCK, settings(),
                        () -> new MinisoWriteOffProtection() {
                            public boolean blockInvalidation(String eventId, String orderId) {
                                throw new MinisoWriteOffException(MinisoWriteOffException.Reason.OWNERSHIP_CONFLICT);
                            }
                            public void assertVoidAllowed(String orderId) { }
                        });

                assertThat(service.processDue(1, 0, 10, "worker", List.of(OutboxEventType.INVALID_ORDER), 1).failed()).isEqualTo(1);
                assertThat(gateway.queryRequests).singleElement().satisfies(query -> assertThat(query.orderId()).isEqualTo("ORDER-1"));
                assertThat(outbox.successes).isEmpty();
                assertThat(outbox.manualRequired).hasSize(1);
                assertThat(orders.authorization).isNull();
                assertThat(orders.completedReleaseDeltas).isEmpty();
                assertThat(release.balanceIncrease).isZero();
                assertThat(notifications.createdTasks).isEmpty();
            }
        }
    }

    @Test
    void invalidationCanRecoverMissingMinisoReceiptButQueryConfirmationAloneDoesNotRelease() {
        var orders = new FakeFulfillmentOrderPort(minisoOrder(12, true, "SUCCESS"));
        var outbox = new FakeOutboxPort(outbox(OutboxEventType.INVALID_ORDER));
        var receipt = new MinisoFulfillmentReceipt("ORDER-1", "A36", 123, "TEST-C1", "13800138000",
                "2026-07-01 00:00:00", "2026-12-01 00:00:00", 0);
        var gateway = new FakeSupplierGatewayPort(SupplierGatewayResponse.retryableFailed("ORDER-1", "UNKNOWN", "no receipt", ""),
                SupplierGatewayResponse.success("ORDER-1", "200", "ok", "{}").withMinisoReceipt(receipt));
        var release = new IdempotentBalanceReleasePort();
        var notifications = new FakeNotifyTaskPort();
        var service = minisoWorker(outbox, orders, gateway, release, notifications, new FakeBalanceDeltaOutboxPort(), false);

        assertThat(service.processDue(1, 0, 10, "worker", List.of(OutboxEventType.INVALID_ORDER), 1).failed()).isEqualTo(1);
        assertThat(orders.statusUpdates.getLast().minisoReceipt()).isEqualTo(receipt);
        assertThat(orders.authorization).isNull();
        assertThat(release.balanceIncrease).isZero();
        assertThat(notifications.createdTasks).isEmpty();
        assertThat(outbox.successes).isEmpty();

        var voidReceipt = new MinisoFulfillmentReceipt(receipt.orderId(), receipt.orgCode(), receipt.batchId(),
                receipt.couponCode(), receipt.phone(), receipt.beginTime(), receipt.endTime(), 3);
        service = minisoWorker(outbox, orders, new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("ORDER-1", "200", "void confirmed", "{}").withMinisoReceipt(voidReceipt)),
                release, notifications, new FakeBalanceDeltaOutboxPort(), false);
        assertThat(service.processDue(1, 0, 10, "worker", List.of(OutboxEventType.INVALID_ORDER), 1).succeeded()).isEqualTo(1);
        assertThat(release.balanceIncrease).isEqualTo(137);
        assertThat(orders.completedReleaseDeltas).hasSize(1);
        assertThat(notifications.createdTasks).singleElement().satisfies(task -> assertThat(task.bizStatus()).isEqualTo("CANCELLED"));
    }

    @ParameterizedTest
    @EnumSource(value = OutboxEventType.class,
            names = {"SUPPLIER_DISPATCH", "SUPPLIER_QUERY", "INVALID_ORDER"})
    void completedMinisoAuthorizationVerifiesCompletionWithoutDependingOnRedisAgain(OutboxEventType type) {
        var orders = new FakeFulfillmentOrderPort(minisoOrder(6, false, "FAILED"));
        orders.authorization = minisoAuthorization();
        orders.completedReleaseDeltas.put("balance-flush-release-ORDER-1",
                new BalanceDeltaFlushOutboxCommand("A36", "ORDER-1", "OUT-1", 137, "RELEASE", ""));
        orders.completionFailures = 1;
        var outbox = new FakeOutboxPort(outbox(type));
        var release = new FakeBalanceReleasePort(BalanceReservationReleaseResult.failed("DOWN", "unavailable"));
        var notifications = new FakeNotifyTaskPort();
        var service = minisoWorker(outbox, orders,
                new FakeSupplierGatewayPort(SupplierGatewayResponse.success("unused", "200", "", "{}")),
                release, notifications, new FakeBalanceDeltaOutboxPort(), true);

        assertThat(service.processDue(1, 0, 10, "worker", List.of(type), 1).failed()).isEqualTo(1);
        assertThat(outbox.successes).isEmpty();
        assertThat(notifications.createdTasks).isEmpty();
        assertThat(service.processDue(1, 0, 10, "worker", List.of(type), 1).succeeded()).isEqualTo(1);
        assertThat(release.commands).isEmpty();
        assertThat(orders.completedReleaseDeltas).hasSize(1);
        assertThat(orders.authorization).isEqualTo(minisoAuthorization());
    }

    @ParameterizedTest
    @EnumSource(value = OutboxEventType.class,
            names = {"SUPPLIER_DISPATCH", "SUPPLIER_QUERY", "INVALID_ORDER"})
    void minisoTerminalReliesOnPersistentEvidenceAndNeverInventsReleaseForUnreservedOrder(OutboxEventType type) {
        for (boolean reserved : List.of(true, false)) {
            var orders = new FakeFulfillmentOrderPort(minisoOrder(type == OutboxEventType.INVALID_ORDER ? 12
                    : type == OutboxEventType.SUPPLIER_QUERY ? 13 : 2, reserved, ""));
            orders.hasTrustedVoidReceipt = true;
            var outbox = new FakeOutboxPort(outbox(type));
            var response = type == OutboxEventType.INVALID_ORDER
                    ? SupplierGatewayResponse.success("ORDER-1", "200", "ok", "{}")
                    : SupplierGatewayResponse.failed("ORDER-1", "UNVERIFIED", "failed", "{}");
            var release = new IdempotentBalanceReleasePort();
            var service = minisoWorker(outbox, orders, new FakeSupplierGatewayPort(response), release,
                    new FakeNotifyTaskPort(), new FakeBalanceDeltaOutboxPort(), false);

            assertThat(service.processDue(1, 0, 10, "worker", List.of(type), 1).succeeded()).isEqualTo(1);
            assertThat(release.balanceIncrease).isEqualTo(reserved ? 137 : 0);
            assertThat(orders.completedReleaseDeltas).hasSize(reserved ? 1 : 0);
            assertThat(orders.authorization != null).isEqualTo(reserved);
            assertThat(orders.statusUpdates.getLast().dispatchStatus()).isEqualTo(type == OutboxEventType.INVALID_ORDER
                    ? FulfillmentDispatchStatus.CANCELLED : FulfillmentDispatchStatus.FAILED);
        }
    }

    private static RechargeFulfillmentOrder minisoOrder(int status, boolean reserved, String dispatchStatus) {
        return new RechargeFulfillmentOrder("ORDER-1", "OUT-1", "A36", "13800138000", "P", "SKU", "MINISO",
                137, "https://merchant.example/notify", status, "DB_PERSISTED", "PASSED", true, reserved,
                dispatchStatus, dispatchStatus.equals("CANCELLED") ? CLOCK.instant().minusSeconds(120) : null);
    }

    private static MinisoBalanceReleaseAuthorization minisoAuthorization() {
        return new MinisoBalanceReleaseAuthorization("ORDER-1", "A36", "OUT-1", "event-A", 137,
                CLOCK.instant().minusSeconds(120));
    }

    private static RechargeOutboxWorkerService minisoWorker(FakeOutboxPort outbox, FakeFulfillmentOrderPort orders,
            RechargeSupplierGatewayPort gateway, BalanceReservationReleasePort release, RechargeNotifyTaskPort notifications,
            BalanceDeltaFlushOutboxPort deltas, boolean redeemed) {
        return new RechargeOutboxWorkerService(outbox, orders, gateway, new FakeSupplierQueryOutboxPort(),
                new FakeFrequencyPort(true), deltas, release, notifications, new FixedNotifyPayloadSigner(),
                RechargeDispatchTimerPort.noop(), CLOCK, settings(), () -> new MinisoWriteOffProtection() {
                    public boolean blockInvalidation(String eventId, String orderId) { return redeemed; }
                    public void assertVoidAllowed(String orderId) { }
                });
    }

    @Test
    void confirmedMinisoReceiptReachesTerminalWriteBeforeMerchantNotification() {
        var receipt = new MinisoFulfillmentReceipt("ORDER-1","A36",100023,"TEST-C1","13800138000",
                "2026-10-01 00:00:00","2026-12-01 00:00:00",0);
        var outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        var orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        var notifications = new FakeNotifyTaskPort();
        var gateway = new FakeSupplierGatewayPort(SupplierGatewayResponse.success("ORDER-1","200","ok","{}")
                .withMinisoReceipt(receipt));
        var service = new RechargeOutboxWorkerService(outboxPort,orderPort,gateway,new FakeFrequencyPort(true),
                new FakeBalanceDeltaOutboxPort(),new FakeBalanceReleasePort(BalanceReservationReleaseResult.releasedSuccess()),
                notifications,new FixedNotifyPayloadSigner(),CLOCK,settings());
        assertThat(service.processDue(1,0,10,"worker").succeeded()).isEqualTo(1);
        assertThat(orderPort.statusUpdates).anySatisfy(update -> {
            assertThat(update.dispatchStatus()).isEqualTo(FulfillmentDispatchStatus.SUCCESS);
            assertThat(update.minisoReceipt()).isEqualTo(receipt);
        });
    }

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
        FakeDispatchTimerPort dispatchTimerPort = new FakeDispatchTimerPort();

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
    void processEventClaimsSingleEventByIdWithoutShardScan() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("SUP-1001", "0", "success", "{\"status\":\"SUCCESS\"}"));

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

        RechargeOutboxBatchResult.Outcome outcome = service.processEvent(
                "dispatch-ORDER-1",
                OutboxEventType.SUPPLIER_DISPATCH,
                "timer-worker-A");

        assertThat(outcome).isEqualTo(RechargeOutboxBatchResult.Outcome.SUCCEEDED);
        assertThat(outboxPort.scanCount).isZero();
        assertThat(outboxPort.findByEventIds).containsExactly("dispatch-ORDER-1");
        assertThat(outboxPort.claimedOrderIds).containsExactly("ORDER-1");
        assertThat(outboxPort.successes).containsExactly("dispatch-ORDER-1");
    }

    @Test
    void processEventDefersDispatchWhenSameAccountOwnedByAnotherOrderWithoutConsumingRetry() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("SUP-1001", "0", "success", "{\"status\":\"SUCCESS\"}"));
        FakeFrequencyPort frequencyPort = new FakeFrequencyPort(true);
        frequencyPort.setCurrentOwner("A36", "13800138000", "ORDER-OTHER");
        FakeDispatchTimerPort dispatchTimerPort = new FakeDispatchTimerPort();

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeSupplierQueryOutboxPort(),
                frequencyPort,
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings(),
                dispatchTimerPort);

        RechargeOutboxBatchResult.Outcome outcome = service.processEvent(
                "dispatch-ORDER-1",
                OutboxEventType.SUPPLIER_DISPATCH,
                "timer-worker-A");

        assertThat(outcome).isEqualTo(RechargeOutboxBatchResult.Outcome.DEFERRED);
        assertThat(outboxPort.findByEventIds).containsExactly("dispatch-ORDER-1");
        assertThat(outboxPort.claimedOrderIds).isEmpty();
        assertThat(outboxPort.failures).isEmpty();
        assertThat(supplierGateway.dispatchRequests).isEmpty();
        assertThat(dispatchTimerPort.calls)
                .containsExactly(new TimerCall(
                        "dispatch-ORDER-1",
                        OutboxEventType.SUPPLIER_DISPATCH,
                        Duration.ofSeconds(30)));
    }

    @Test
    void processEventReloadsOrderAfterClaimBeforeDispatchDecision() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        SequencedFulfillmentOrderPort orderPort = new SequencedFulfillmentOrderPort(
                dispatchableOrder(),
                successfulOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("SUP-1001", "0", "success", "{\"status\":\"SUCCESS\"}"));

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeSupplierQueryOutboxPort(),
                new FakeFrequencyPort(true),
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings(),
                new FakeDispatchTimerPort());

        RechargeOutboxBatchResult.Outcome outcome = service.processEvent(
                "dispatch-ORDER-1",
                OutboxEventType.SUPPLIER_DISPATCH,
                "timer-worker-A");

        assertThat(outcome).isEqualTo(RechargeOutboxBatchResult.Outcome.SUCCEEDED);
        assertThat(orderPort.lookupCount()).isEqualTo(2);
        assertThat(outboxPort.claimedOrderIds).containsExactly("ORDER-1");
        assertThat(outboxPort.successes).containsExactly("dispatch-ORDER-1");
        assertThat(supplierGateway.dispatchRequests).isEmpty();
        assertThat(orderPort.flowStarts).isEmpty();
        assertThat(orderPort.statusUpdates).isEmpty();
    }

    @Test
    void dispatchSuccessSkipsNotifyTaskWhenOrderNotifyUrlIsBlank() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(orderWithNotifyUrl(""));
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("SUP-1001", "0", "success", "{\"status\":\"SUCCESS\"}"));
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
        assertThat(outboxPort.successes).containsExactly("dispatch-ORDER-1");
        assertThat(orderPort.statusUpdates.getLast().dispatchStatus()).isEqualTo(FulfillmentDispatchStatus.SUCCESS);
        assertThat(notifyTaskPort.createdTasks).isEmpty();
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
        FakeSupplierQueryOutboxPort queryOutboxPort = new FakeSupplierQueryOutboxPort();
        FakeDispatchTimerPort dispatchTimerPort = new FakeDispatchTimerPort();

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                queryOutboxPort,
                new FakeFrequencyPort(true),
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                notifyTaskPort,
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings(),
                dispatchTimerPort);

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(orderPort.statusUpdates.getLast().dispatchStatus()).isEqualTo(FulfillmentDispatchStatus.SUPPLIER_ACCEPTED);
        assertThat(orderPort.statusUpdates.getLast().orderStatus()).isEqualTo(13);
        assertThat(notifyTaskPort.createdTasks).isEmpty();
        assertThat(queryOutboxPort.commands).singleElement().satisfies(command -> {
            assertThat(command.eventId()).isEqualTo("supplier-query-ORDER-1");
            assertThat(command.orderId()).isEqualTo("ORDER-1");
            assertThat(command.supplierChannel()).isEqualTo("FNK_CGS_BX");
            assertThat(command.initialDelay()).isEqualTo(Duration.ofSeconds(2));
            assertThat(command.maxRetryCount()).isEqualTo(6);
        });
        assertThat(dispatchTimerPort.calls)
                .containsExactly(new TimerCall(
                        "supplier-query-ORDER-1",
                        OutboxEventType.SUPPLIER_QUERY,
                        Duration.ofSeconds(2)));
    }

    @Test
    void supplierQueryProcessingKeepsOrderInSupplierAcceptedAndRetriesQueryEvent() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(supplierQueryOutbox());
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(supplierAcceptedOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "unused", "{}"),
                SupplierGatewayResponse.processing("SUP-1002", "PENDING", "still processing", "{\"status\":\"PROCESSING\"}"));
        FakeNotifyTaskPort notifyTaskPort = new FakeNotifyTaskPort();
        FakeFrequencyPort frequencyPort = new FakeFrequencyPort(true);
        FakeDispatchTimerPort dispatchTimerPort = new FakeDispatchTimerPort();

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeSupplierQueryOutboxPort(),
                frequencyPort,
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                notifyTaskPort,
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings(),
                dispatchTimerPort);

        RechargeOutboxBatchResult result =
                service.processDue(1, 0, 10, "worker-A", List.of(OutboxEventType.SUPPLIER_QUERY), 1);

        assertThat(result.failed()).isEqualTo(1);
        assertThat(supplierGateway.queryRequests).singleElement().satisfies(request -> {
            assertThat(request.supplierRequestId()).isEqualTo("supplier-query-ORDER-1");
            assertThat(request.orderId()).isEqualTo("ORDER-1");
        });
        assertThat(outboxPort.successes).isEmpty();
        assertThat(outboxPort.failures).singleElement().satisfies(failure -> {
            assertThat(failure.eventId()).isEqualTo("supplier-query-ORDER-1");
            assertThat(failure.nextRetryDelay()).isEqualTo(Duration.ofSeconds(4));
        });
        assertThat(dispatchTimerPort.calls)
                .containsExactly(new TimerCall(
                        "supplier-query-ORDER-1",
                        OutboxEventType.SUPPLIER_QUERY,
                        Duration.ofSeconds(4)));
        assertThat(orderPort.statusUpdates).singleElement().satisfies(update -> {
            assertThat(update.dispatchStatus()).isEqualTo(FulfillmentDispatchStatus.SUPPLIER_ACCEPTED);
            assertThat(update.orderStatus()).isEqualTo(13);
        });
        assertThat(notifyTaskPort.createdTasks).isEmpty();
        assertThat(frequencyPort.releasedOrderIds).isEmpty();
    }

    @Test
    void supplierQueryRetryableFailureUsesNextQueryDelayAndSchedulesTimer() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(supplierQueryOutbox(1, 6));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(supplierAcceptedOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "unused", "{}"),
                SupplierGatewayResponse.retryableFailed("SUP-1002", "TIMEOUT", "query timeout", "{}"));
        FakeDispatchTimerPort dispatchTimerPort = new FakeDispatchTimerPort();

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeSupplierQueryOutboxPort(),
                new FakeFrequencyPort(true),
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings(),
                dispatchTimerPort);

        RechargeOutboxBatchResult result =
                service.processDue(1, 0, 10, "worker-A", List.of(OutboxEventType.SUPPLIER_QUERY), 1);

        assertThat(result.failed()).isEqualTo(1);
        assertThat(outboxPort.failures).singleElement().satisfies(failure -> {
            assertThat(failure.eventId()).isEqualTo("supplier-query-ORDER-1");
            assertThat(failure.nextRetryDelay()).isEqualTo(Duration.ofSeconds(6));
        });
        assertThat(dispatchTimerPort.calls)
                .containsExactly(new TimerCall(
                        "supplier-query-ORDER-1",
                        OutboxEventType.SUPPLIER_QUERY,
                        Duration.ofSeconds(6)));
    }

    @Test
    void supplierQuerySuccessAdvancesOrderToSuccessAndCreatesNotifyTask() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(supplierQueryOutbox());
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(supplierAcceptedOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "unused", "{}"),
                SupplierGatewayResponse.success("SUP-1002", "0", "success", "{\"status\":\"SUCCESS\"}"));
        FakeNotifyTaskPort notifyTaskPort = new FakeNotifyTaskPort();
        FakeFrequencyPort frequencyPort = new FakeFrequencyPort(true);

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeSupplierQueryOutboxPort(),
                frequencyPort,
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                notifyTaskPort,
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result =
                service.processDue(1, 0, 10, "worker-A", List.of(OutboxEventType.SUPPLIER_QUERY), 1);

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(outboxPort.successes).containsExactly("supplier-query-ORDER-1");
        assertThat(orderPort.statusUpdates).singleElement().satisfies(update -> {
            assertThat(update.dispatchStatus()).isEqualTo(FulfillmentDispatchStatus.SUCCESS);
            assertThat(update.orderStatus()).isEqualTo(12);
            assertThat(update.dataStatus()).isEqualTo("PROCESSED");
        });
        assertThat(orderPort.flowStarts).singleElement().satisfies(flow ->
                assertThat(flow.supplierRequestId()).isEqualTo("supplier-query-ORDER-1"));
        assertThat(orderPort.flowUpdates).singleElement().satisfies(update ->
                assertThat(update.status()).isEqualTo("success"));
        assertThat(notifyTaskPort.createdTasks).singleElement().satisfies(task -> {
            assertThat(task.taskId()).isEqualTo("notify-order-ORDER-1-12");
            assertThat(task.payload()).containsEntry("orderStatus", "12");
        });
        assertThat(frequencyPort.releasedOrderIds).containsExactly("ORDER-1");
    }

    @Test
    void supplierQueryRetryAfterOrderSuccessReplaysNotifyAndFrequencySideEffectsWithoutQueryingSupplier() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(supplierQueryOutbox());
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(successfulOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "unused", "{}"));
        FakeNotifyTaskPort notifyTaskPort = new FakeNotifyTaskPort();
        FakeFrequencyPort frequencyPort = new FakeFrequencyPort(true);

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeSupplierQueryOutboxPort(),
                frequencyPort,
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                notifyTaskPort,
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result =
                service.processDue(1, 0, 10, "worker-A", List.of(OutboxEventType.SUPPLIER_QUERY), 1);

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(supplierGateway.queryRequests).isEmpty();
        assertThat(outboxPort.successes).containsExactly("supplier-query-ORDER-1");
        assertThat(notifyTaskPort.createdTasks).singleElement().satisfies(task ->
                assertThat(task.taskId()).isEqualTo("notify-order-ORDER-1-12"));
        assertThat(frequencyPort.releasedOrderIds).containsExactly("ORDER-1");
    }

    @Test
    void supplierQueryFailureAdvancesOrderToFailedAndReleasesReservedBalance() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(supplierQueryOutbox());
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(supplierAcceptedOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "unused", "{}"),
                SupplierGatewayResponse.failed("SUP-1002", "E_SUP", "supplier rejected", "{\"status\":\"FAILED\"}"));
        FakeNotifyTaskPort notifyTaskPort = new FakeNotifyTaskPort();
        FakeFrequencyPort frequencyPort = new FakeFrequencyPort(true);
        FakeBalanceDeltaOutboxPort balanceDeltaOutboxPort = new FakeBalanceDeltaOutboxPort();
        FakeBalanceReleasePort balanceReleasePort = new FakeBalanceReleasePort(
                BalanceReservationReleaseResult.releasedSuccess());

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeSupplierQueryOutboxPort(),
                frequencyPort,
                balanceDeltaOutboxPort,
                balanceReleasePort,
                notifyTaskPort,
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result =
                service.processDue(1, 0, 10, "worker-A", List.of(OutboxEventType.SUPPLIER_QUERY), 1);

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(outboxPort.successes).containsExactly("supplier-query-ORDER-1");
        assertThat(orderPort.statusUpdates).singleElement().satisfies(update -> {
            assertThat(update.dispatchStatus()).isEqualTo(FulfillmentDispatchStatus.FAILED);
            assertThat(update.orderStatus()).isEqualTo(6);
            assertThat(update.dataStatus()).isEqualTo("PROCESSED");
        });
        assertThat(notifyTaskPort.createdTasks).singleElement().satisfies(task ->
                assertThat(task.taskId()).isEqualTo("notify-order-ORDER-1-6"));
        assertThat(balanceReleasePort.commands).singleElement().satisfies(command -> {
            assertThat(command.releaseEventId()).isEqualTo("supplier-query-ORDER-1");
            assertThat(command.amount()).isEqualTo(5200L);
        });
        assertThat(balanceDeltaOutboxPort.commands).singleElement().satisfies(command -> {
            assertThat(command.deltaAmount()).isEqualTo(5200L);
            assertThat(command.deltaType()).isEqualTo("RELEASE");
        });
        assertThat(orderPort.balanceReleasedOrderIds).containsExactly("ORDER-1");
        assertThat(frequencyPort.releasedOrderIds).containsExactly("ORDER-1");
    }

    @Test
    void minisoRedemptionBlocksBeforeAfterRemoteVoidAndTerminalReleaseCompensation() {
        for (int scenario = 0; scenario < 3; scenario++) {
            int stage = scenario;
            var order =
                    new RechargeFulfillmentOrder(
                            "ORDER-1",
                            "OUT-1",
                            "A36",
                            "13800138000",
                            "P",
                            "SKU",
                            "MINISO",
                            5200,
                            "https://merchant.example",
                            stage == 2 ? 6 : 12,
                            "DB_PERSISTED",
                            "PASSED",
                            true,
                            true);
            var outbox = new FakeOutboxPort(outbox(OutboxEventType.INVALID_ORDER));
            var orders = new FakeFulfillmentOrderPort(order);
            var gateway =
                    new FakeSupplierGatewayPort(
                            SupplierGatewayResponse.success("SUP", "200", "ok", "{}"));
            var release =
                    new FakeBalanceReleasePort(BalanceReservationReleaseResult.releasedSuccess());
            var deltas = new FakeBalanceDeltaOutboxPort();
            var notify = new FakeNotifyTaskPort();
            var checks = new AtomicInteger();
            var guard =
                    new MinisoWriteOffProtection() {
                        public boolean blockInvalidation(String eventId, String orderId) {
                            return stage != 1 || checks.incrementAndGet() > 1;
                        }

                        public void assertVoidAllowed(String orderId) {}
                    };
            var service =
                    new RechargeOutboxWorkerService(
                            outbox,
                            orders,
                            gateway,
                            command -> {},
                            new FakeFrequencyPort(true),
                            deltas,
                            release,
                            notify,
                            new FixedNotifyPayloadSigner(),
                            RechargeDispatchTimerPort.noop(),
                            CLOCK,
                            settings(),
                            () -> guard);
            service.processDue(1, 0, 10, "worker", List.of(OutboxEventType.INVALID_ORDER), 1);
            assertThat(release.commands).isEmpty();
            assertThat(deltas.commands).isEmpty();
            assertThat(notify.createdTasks).isEmpty();
            assertThat(outbox.manualRequired).hasSize(1);
            assertThat(gateway.invalidRequests).hasSize(stage == 1 ? 1 : 0);
            assertThat(orders.statusUpdates)
                    .noneMatch(
                            update ->
                                    update.dispatchStatus() == FulfillmentDispatchStatus.CANCELLED);
        }
    }

    @Test
    void invalidOrderSuccessUpdatesLocalStateCreatesWriteOffNotifyAndCompletesOutbox() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.INVALID_ORDER));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(successfulOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("SUP-INVALID-1", "0", "invalid success", "{\"status\":\"SUCCESS\"}"));
        FakeNotifyTaskPort notifyTaskPort = new FakeNotifyTaskPort();
        FakeBalanceDeltaOutboxPort balanceDeltaOutboxPort = new FakeBalanceDeltaOutboxPort();
        FakeBalanceReleasePort balanceReleasePort = new FakeBalanceReleasePort(BalanceReservationReleaseResult.releasedSuccess());
        FakeDispatchTimerPort dispatchTimerPort = new FakeDispatchTimerPort();

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
                settings(),
                dispatchTimerPort);

        RechargeOutboxBatchResult result =
                service.processDue(1, 0, 10, "worker-A", List.of(OutboxEventType.INVALID_ORDER), 1);

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(supplierGateway.invalidRequests).hasSize(1);
        assertThat(orderPort.statusUpdates)
                .extracting(OrderFulfillmentStatusUpdate::dispatchStatus)
                .containsExactly(FulfillmentDispatchStatus.CANCEL_REQUESTED, FulfillmentDispatchStatus.CANCELLED);
        assertThat(orderPort.statusUpdates.getLast().orderStatus()).isEqualTo(12);
        assertThat(orderPort.statusUpdates.getFirst().orderStatus()).isEqualTo(12);
        assertThat(orderPort.statusUpdates.getLast().invalidTime()).isEqualTo(CLOCK.instant());
        assertThat(notifyTaskPort.createdTasks.getFirst().notifyType()).isEqualTo("VOUCHER_STATE");
        assertThat(notifyTaskPort.createdTasks.getFirst().payload())
                .containsEntry("useState", "3")
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
    void invalidOrderSuccessSkipsWriteOffNotifyWhenOrderNotifyUrlIsBlank() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.INVALID_ORDER));
        var original = successfulOrder();
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(new RechargeFulfillmentOrder(
                original.orderId(), original.outOrderSn(), original.orgCode(), original.account(),
                original.productId(), original.selectedProductId(), original.supplierChannel(),
                original.orderPrice(), "", original.orderStatus(), original.persistStatus(),
                original.validationStatus(), original.dispatchAllowed(), original.balanceReserved()));
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

        RechargeOutboxBatchResult result =
                service.processDue(1, 0, 10, "worker-A", List.of(OutboxEventType.INVALID_ORDER), 1);

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(outboxPort.successes).containsExactly("dispatch-ORDER-1");
        assertThat(orderPort.statusUpdates.getLast().dispatchStatus()).isEqualTo(FulfillmentDispatchStatus.CANCELLED);
        assertThat(balanceReleasePort.commands).hasSize(1);
        assertThat(balanceDeltaOutboxPort.commands).hasSize(1);
        assertThat(notifyTaskPort.createdTasks).isEmpty();
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
        assertThat(orderPort.statusUpdates.getLast().supplierFlowId()).isEqualTo(99L);
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
    void dispatchRetryableSupplierFailureCreatesQueryAndClosesDispatchWithoutTerminalSideEffects() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.retryableFailed("", "500", "supplier http status not 2xx", "server error"));
        FakeFrequencyPort frequencyPort = new FakeFrequencyPort(true);
        FakeNotifyTaskPort notifyTaskPort = new FakeNotifyTaskPort();
        FakeBalanceDeltaOutboxPort balanceDeltaOutboxPort = new FakeBalanceDeltaOutboxPort();
        FakeBalanceReleasePort balanceReleasePort = new FakeBalanceReleasePort(BalanceReservationReleaseResult.releasedSuccess());
        FakeSupplierQueryOutboxPort queryOutboxPort = new FakeSupplierQueryOutboxPort();
        FakeDispatchTimerPort dispatchTimerPort = new FakeDispatchTimerPort();

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                queryOutboxPort,
                frequencyPort,
                balanceDeltaOutboxPort,
                balanceReleasePort,
                notifyTaskPort,
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings(),
                dispatchTimerPort);

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(orderPort.flowUpdates).singleElement().satisfies(update ->
                assertThat(update.status()).isEqualTo("processing"));
        assertThat(outboxPort.successes).containsExactly("dispatch-ORDER-1");
        assertThat(outboxPort.failures).isEmpty();
        assertThat(queryOutboxPort.commands).singleElement().satisfies(command -> {
            assertThat(command.eventId()).isEqualTo("supplier-query-ORDER-1");
            assertThat(command.orderId()).isEqualTo("ORDER-1");
            assertThat(command.initialDelay()).isEqualTo(Duration.ofSeconds(2));
            assertThat(command.maxRetryCount()).isEqualTo(6);
        });
        assertThat(dispatchTimerPort.calls)
                .containsExactly(new TimerCall(
                        "supplier-query-ORDER-1",
                        OutboxEventType.SUPPLIER_QUERY,
                        Duration.ofSeconds(2)));
        assertThat(orderPort.statusUpdates)
                .extracting(OrderFulfillmentStatusUpdate::dispatchStatus)
                .containsExactly(FulfillmentDispatchStatus.DISPATCHING, FulfillmentDispatchStatus.SUPPLIER_ACCEPTED);
        assertThat(orderPort.statusUpdates.getLast().orderStatus()).isEqualTo(13);
        assertThat(orderPort.balanceReleasedOrderIds).isEmpty();
        assertThat(balanceReleasePort.commands).isEmpty();
        assertThat(balanceDeltaOutboxPort.commands).isEmpty();
        assertThat(notifyTaskPort.createdTasks).isEmpty();
        assertThat(frequencyPort.releasedOrderIds).isEmpty();
    }

    @Test
    void dispatchRecoveryWithExistingSupplierFlowCreatesQueryWithoutDispatchingAgain() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        orderPort.latestSupplierFlowsByOrderId.put("ORDER-1", new SupplierFlowSnapshot(88L, "new"));
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "unused", "{}"));
        FakeSupplierQueryOutboxPort queryOutboxPort = new FakeSupplierQueryOutboxPort();
        FakeDispatchTimerPort dispatchTimerPort = new FakeDispatchTimerPort();

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                queryOutboxPort,
                new FakeFrequencyPort(true),
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings(),
                dispatchTimerPort);

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(supplierGateway.dispatchRequests).isEmpty();
        assertThat(orderPort.flowStarts).isEmpty();
        assertThat(outboxPort.successes).containsExactly("dispatch-ORDER-1");
        assertThat(queryOutboxPort.commands).singleElement().satisfies(command ->
                assertThat(command.eventId()).isEqualTo("supplier-query-ORDER-1"));
        assertThat(orderPort.statusUpdates).singleElement().satisfies(update -> {
            assertThat(update.dispatchStatus()).isEqualTo(FulfillmentDispatchStatus.SUPPLIER_ACCEPTED);
            assertThat(update.supplierFlowId()).isEqualTo(88L);
        });
        assertThat(dispatchTimerPort.calls).singleElement().satisfies(call ->
                assertThat(call.eventId()).isEqualTo("supplier-query-ORDER-1"));
    }

    @Test
    void dispatchExceptionAfterSupplierFlowStartCreatesQueryWithoutRetryingDispatch() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = FakeSupplierGatewayPort.throwing(new IllegalStateException("socket closed"));
        FakeSupplierQueryOutboxPort queryOutboxPort = new FakeSupplierQueryOutboxPort();
        FakeDispatchTimerPort dispatchTimerPort = new FakeDispatchTimerPort();

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                queryOutboxPort,
                new FakeFrequencyPort(true),
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings(),
                dispatchTimerPort);

        RechargeOutboxBatchResult result = service.processDue(1, 0, 10, "worker-A");

        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(outboxPort.failures).isEmpty();
        assertThat(outboxPort.successes).containsExactly("dispatch-ORDER-1");
        assertThat(orderPort.flowStarts).hasSize(1);
        assertThat(queryOutboxPort.commands).singleElement().satisfies(command ->
                assertThat(command.eventId()).isEqualTo("supplier-query-ORDER-1"));
        assertThat(orderPort.statusUpdates)
                .extracting(OrderFulfillmentStatusUpdate::dispatchStatus)
                .containsExactly(FulfillmentDispatchStatus.DISPATCHING, FulfillmentDispatchStatus.SUPPLIER_ACCEPTED);
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

    @Test
    void processDueDefersSecondDispatchForSameAccountInBatch() {
        RechargeOutboxEvent first = dispatchOutbox("ORDER-1");
        RechargeOutboxEvent second = dispatchOutbox("ORDER-2");
        FakeOutboxPort outboxPort = new FakeOutboxPort(first, second);
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(Map.of(
                "ORDER-1", dispatchableOrder("ORDER-1"),
                "ORDER-2", dispatchableOrder("ORDER-2")));
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("SUP-1001", "0", "success", "{\"status\":\"SUCCESS\"}"));

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

        RechargeOutboxBatchResult result = service.processDue(1, 0, 2, "worker-A");

        assertThat(result.deferred()).isGreaterThanOrEqualTo(1);
        assertThat(result.claimed()).isEqualTo(1);
        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(outboxPort.claimedOrderIds).containsExactly("ORDER-1");
        assertThat(supplierGateway.dispatchRequests).hasSize(1);
    }

    @Test
    void processDueDefersDispatchWhenFrequencyLockOwnedByOtherOrder() {
        FakeOutboxPort outboxPort = new FakeOutboxPort(outbox(OutboxEventType.SUPPLIER_DISPATCH));
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(dispatchableOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "success", "{}"));
        FakeFrequencyPort frequencyPort = new FakeFrequencyPort(true);
        frequencyPort.setCurrentOwner("A36", "13800138000", "OTHER-ORDER");

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

        assertThat(result.deferred()).isGreaterThanOrEqualTo(1);
        assertThat(result.failed()).isZero();
        assertThat(result.claimed()).isZero();
        assertThat(outboxPort.claimedOrderIds).isEmpty();
        assertThat(outboxPort.failures).isEmpty();
        assertThat(supplierGateway.dispatchRequests).isEmpty();
    }

    @Test
    void processDueWithQueryEventTypesOnlyProcessesSupplierQuery() {
        RechargeOutboxEvent dispatch = outbox(OutboxEventType.SUPPLIER_DISPATCH);
        RechargeOutboxEvent query = supplierQueryOutbox();
        FakeOutboxPort outboxPort = new FakeOutboxPort(dispatch, query);
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(supplierAcceptedOrder());
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("unused", "0", "unused", "{}"),
                SupplierGatewayResponse.processing("SUP-1002", "PENDING", "still processing", "{}"));

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeSupplierQueryOutboxPort(),
                new FakeFrequencyPort(true),
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result =
                service.processDue(1, 0, 10, "worker-A", List.of(OutboxEventType.SUPPLIER_QUERY), 1);

        assertThat(result.claimed()).isEqualTo(1);
        assertThat(result.failed()).isEqualTo(1);
        assertThat(supplierGateway.dispatchRequests).isEmpty();
        assertThat(supplierGateway.queryRequests).hasSize(1);
        assertThat(outboxPort.claimedOrderIds).containsExactly("ORDER-1");
    }

    @Test
    void processDueSerializesSameAccountDispatchAndQueryUnderParallelism() throws Exception {
        /*
         * spec §9「并行同账号串行」：同账号 (A36, 13800138000) 的一条 DISPATCH 与一条 QUERY。
         * fairSelect 只对 DISPATCH 去重，QUERY 不去重，故两者均被 claim 并提交到线程池；
         * 线程池内按 orgCode+'\0'+account 同锁串行执行，gateway 不应观测到并发重叠。
         */
        RechargeOutboxEvent dispatch = outbox(OutboxEventType.SUPPLIER_DISPATCH);
        RechargeOutboxEvent query = supplierQueryOutbox("ORDER-2");
        FakeOutboxPort outboxPort = new FakeOutboxPort(dispatch, query);
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(Map.of(
                "ORDER-1", dispatchableOrder("ORDER-1"),
                "ORDER-2", supplierAcceptedOrder("ORDER-2")));
        ConcurrencyDetectingSupplierGatewayPort supplierGateway = new ConcurrencyDetectingSupplierGatewayPort();

        RechargeOutboxWorkerService service = new RechargeOutboxWorkerService(
                outboxPort,
                orderPort,
                supplierGateway,
                new FakeSupplierQueryOutboxPort(),
                new FakeFrequencyPort(true),
                BalanceDeltaFlushOutboxPort.noop(),
                BalanceReservationReleasePort.noop(),
                new FakeNotifyTaskPort(),
                new FixedNotifyPayloadSigner(),
                CLOCK,
                settings());

        RechargeOutboxBatchResult result = service.processDue(
                1, 0, 10, "worker-A",
                List.of(OutboxEventType.SUPPLIER_DISPATCH, OutboxEventType.SUPPLIER_QUERY),
                4);

        assertThat(result.succeeded()).isEqualTo(2);
        assertThat(supplierGateway.overlap()).isFalse();
        assertThat(supplierGateway.dispatchRequests()).hasSize(1);
        assertThat(supplierGateway.queryRequests()).hasSize(1);
    }

    @Test
    void processDueOverfetchFillsLimitAfterAccountDedup() {
        /*
         * spec §4 overfetch：4 条 DISPATCH 中 D1/D2 同账号、D3/D4 不同账号。limit=2 + overfetch=3
         * → scanLimit=6 扫到全部 4 条；fairSelect 选 D1、D3（D2 同账号 deferred），claim 2 条。
         * 若 overfetch=1 则 scanLimit=2 只扫到 D1/D2，D2 同账号 deferred，仅 claim 1 条——
         * 故 claimed=2 直接证明 overfetch 是填满 limit 的必要手段。
         */
        RechargeOutboxEvent d1 = dispatchOutbox("ORDER-1");
        RechargeOutboxEvent d2 = dispatchOutbox("ORDER-2");
        RechargeOutboxEvent d3 = dispatchOutbox("ORDER-3");
        RechargeOutboxEvent d4 = dispatchOutbox("ORDER-4");
        FakeOutboxPort outboxPort = new FakeOutboxPort(d1, d2, d3, d4);
        FakeFulfillmentOrderPort orderPort = new FakeFulfillmentOrderPort(Map.of(
                "ORDER-1", dispatchableOrderWithAccount("ORDER-1", "13800138000"),
                "ORDER-2", dispatchableOrderWithAccount("ORDER-2", "13800138000"),
                "ORDER-3", dispatchableOrderWithAccount("ORDER-3", "13900139000"),
                "ORDER-4", dispatchableOrderWithAccount("ORDER-4", "14000140000")));
        FakeSupplierGatewayPort supplierGateway = new FakeSupplierGatewayPort(
                SupplierGatewayResponse.success("SUP-1001", "0", "success", "{\"status\":\"SUCCESS\"}"));

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
                settings(1, 3));

        RechargeOutboxBatchResult result = service.processDue(1, 0, 2, "worker-A");

        assertThat(result.claimed()).isEqualTo(2);
        assertThat(result.deferred()).isGreaterThanOrEqualTo(1);
        assertThat(result.succeeded()).isEqualTo(2);
        assertThat(outboxPort.claimedOrderIds).containsExactly("ORDER-1", "ORDER-3");
    }

    private static RechargeOutboxWorkerSettings settings() {
        return new RechargeOutboxWorkerSettings(
                Duration.ofMinutes(2),
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                Duration.ofMinutes(30),
                Duration.ofSeconds(5));
    }

    private static RechargeOutboxWorkerSettings settings(int parallelism, int overfetchFactor) {
        return new RechargeOutboxWorkerSettings(
                Duration.ofMinutes(2),
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                Duration.ofMinutes(30),
                Duration.ofSeconds(5),
                parallelism,
                overfetchFactor);
    }

    private static RechargeOutboxEvent dispatchOutbox(String orderId) {
        return new RechargeOutboxEvent(
                "dispatch-" + orderId,
                OutboxEventType.SUPPLIER_DISPATCH,
                "A36",
                orderId,
                "OUT-" + orderId.substring(orderId.length() - 1),
                "{\"order_id\":\"" + orderId + "\"}",
                0,
                10,
                7L,
                LocalDateTime.of(2026, 7, 8, 11, 20),
                "");
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

    private static RechargeOutboxEvent supplierQueryOutbox() {
        return supplierQueryOutbox(0, 10);
    }

    private static RechargeOutboxEvent supplierQueryOutbox(int retryCount, int maxRetryCount) {
        return new RechargeOutboxEvent(
                "supplier-query-ORDER-1",
                OutboxEventType.SUPPLIER_QUERY,
                "A36",
                "ORDER-1",
                "OUT-1",
                "{\"order_id\":\"ORDER-1\"}",
                retryCount,
                maxRetryCount,
                7L,
                LocalDateTime.of(2026, 7, 8, 11, 20),
                "");
    }

    private static RechargeOutboxEvent supplierQueryOutbox(String orderId) {
        return new RechargeOutboxEvent(
                "supplier-query-" + orderId,
                OutboxEventType.SUPPLIER_QUERY,
                "A36",
                orderId,
                "OUT-" + orderId.substring(orderId.length() - 1),
                "{\"order_id\":\"" + orderId + "\"}",
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

    @Test
    void cancelledSuccessResumesBalanceReleaseWithoutCallingSupplierOrChangingFactTime() {
        for (var eventType : List.of(OutboxEventType.INVALID_ORDER, OutboxEventType.SUPPLIER_QUERY,
                OutboxEventType.SUPPLIER_DISPATCH)) {
            var base = successfulOrder();
            Instant originalTime = CLOCK.instant().minusSeconds(120);
            var cancelled = new RechargeFulfillmentOrder(base.orderId(), base.outOrderSn(), base.orgCode(),
                    base.account(), base.productId(), base.selectedProductId(), base.supplierChannel(),
                    base.orderPrice(), base.notifyUrl(), 12, base.persistStatus(), base.validationStatus(),
                    true, true, FulfillmentDispatchStatus.CANCELLED.name(), originalTime);
            var outbox = new FakeOutboxPort(outbox(eventType));
            var orders = new FakeFulfillmentOrderPort(cancelled);
            var gateway = new FakeSupplierGatewayPort(SupplierGatewayResponse.success("unused", "200", "", "{}"));
            var release = new FakeBalanceReleasePort(BalanceReservationReleaseResult.releasedSuccess());
            var notify = new FakeNotifyTaskPort();
            var service = new RechargeOutboxWorkerService(outbox, orders, gateway, new FakeFrequencyPort(true),
                    new FakeBalanceDeltaOutboxPort(), release, notify, new FixedNotifyPayloadSigner(), CLOCK, settings());

            var result = service.processDue(1, 0, 10, "worker", List.of(eventType), 1);

            assertThat(result.succeeded()).isEqualTo(1);
            assertThat(cancelled.terminalBalanceReleasePending()).isTrue();
            assertThat(gateway.invalidRequests).isEmpty();
            assertThat(gateway.dispatchRequests).isEmpty();
            assertThat(gateway.queryRequests).isEmpty();
            assertThat(orders.statusUpdates).isEmpty();
            assertThat(release.commands).hasSize(1);
            assertThat(notify.createdTasks.getFirst().payload())
                    .containsEntry("invalidTime", originalTime.toString());
            assertThat(notify.createdTasks).noneMatch(task -> "6".equals(task.bizStatus()));
        }
    }

    @Test
    void pendingIssuanceCannotBecomeSuccessThroughInvalidation() {
        var outbox = new FakeOutboxPort(outbox(OutboxEventType.INVALID_ORDER));
        var orders = new FakeFulfillmentOrderPort(dispatchableOrder());
        var gateway = new FakeSupplierGatewayPort(SupplierGatewayResponse.success("unused", "200", "", "{}"));
        var notify = new FakeNotifyTaskPort();
        var service = new RechargeOutboxWorkerService(outbox, orders, gateway, new FakeFrequencyPort(true),
                new FakeBalanceDeltaOutboxPort(), new FakeBalanceReleasePort(BalanceReservationReleaseResult.releasedSuccess()),
                notify, new FixedNotifyPayloadSigner(), CLOCK, settings());
        service.processDue(1, 0, 10, "worker", List.of(OutboxEventType.INVALID_ORDER), 1);
        assertThat(gateway.invalidRequests).isEmpty();
        assertThat(orders.statusUpdates).isEmpty();
        assertThat(notify.createdTasks).isEmpty();
    }

    @Test
    void supplierProcessingInvalidationKeepsRecoveryTaskAndDoesNotReleaseBalance() {
        var outbox = new FakeOutboxPort(outbox(OutboxEventType.INVALID_ORDER));
        var orders = new FakeFulfillmentOrderPort(successfulOrder());
        var gateway = new FakeSupplierGatewayPort(SupplierGatewayResponse.processing("SUP", "202", "processing", "{}"));
        var release = new FakeBalanceReleasePort(BalanceReservationReleaseResult.releasedSuccess());
        var notify = new FakeNotifyTaskPort();
        var service = new RechargeOutboxWorkerService(outbox, orders, gateway, new FakeFrequencyPort(true),
                new FakeBalanceDeltaOutboxPort(), release, notify, new FixedNotifyPayloadSigner(), CLOCK, settings());
        service.processDue(1, 0, 10, "worker", List.of(OutboxEventType.INVALID_ORDER), 1);
        assertThat(outbox.successes).isEmpty();
        assertThat(outbox.failures).hasSize(1);
        assertThat(release.commands).isEmpty();
        assertThat(notify.createdTasks).isEmpty();
        assertThat(orders.statusUpdates.getLast().orderStatus()).isEqualTo(12);
    }

    private static RechargeFulfillmentOrder dispatchableOrder() {
        return dispatchableOrder("ORDER-1");
    }

    private static RechargeFulfillmentOrder dispatchableOrder(String orderId) {
        return orderWithNotifyUrl("https://merchant.example/notify", orderId);
    }

    private static RechargeFulfillmentOrder dispatchableOrderWithAccount(String orderId, String account) {
        return new RechargeFulfillmentOrder(
                orderId,
                "OUT-" + orderId.substring(orderId.length() - 1),
                "A36",
                account,
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

    private static RechargeFulfillmentOrder orderWithNotifyUrl(String notifyUrl) {
        return orderWithNotifyUrl(notifyUrl, "ORDER-1");
    }

    private static RechargeFulfillmentOrder orderWithNotifyUrl(String notifyUrl, String orderId) {
        return new RechargeFulfillmentOrder(
                orderId,
                "OUT-" + orderId.substring(orderId.length() - 1),
                "A36",
                "13800138000",
                "PARENT-1",
                "SKU-1",
                "FNK_CGS_BX",
                5200L,
                notifyUrl,
                2,
                "DB_PERSISTED",
                "PASSED",
                true,
                true);
    }

    private static RechargeFulfillmentOrder supplierAcceptedOrder() {
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
                13,
                "DB_PERSISTED",
                "PASSED",
                true,
                true);
    }

    private static RechargeFulfillmentOrder supplierAcceptedOrder(String orderId) {
        return new RechargeFulfillmentOrder(
                orderId,
                "OUT-" + orderId.substring(orderId.length() - 1),
                "A36",
                "13800138000",
                "PARENT-1",
                "SKU-1",
                "FNK_CGS_BX",
                5200L,
                "https://merchant.example/notify",
                13,
                "DB_PERSISTED",
                "PASSED",
                true,
                true);
    }

    private static RechargeFulfillmentOrder successfulOrder() {
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
                12,
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

        private final List<RechargeOutboxEvent> events;
        private final List<String> successes = new ArrayList<>();
        private final List<OutboxFailure> failures = new ArrayList<>();
        private final List<OutboxFailure> manualRequired = new ArrayList<>();
        private final List<String> claimedOrderIds = new ArrayList<>();
        private final List<String> findByEventIds = new ArrayList<>();
        private int scanCount;
        private int successFailures;

        private FakeOutboxPort(RechargeOutboxEvent... events) {
            this.events = List.of(events);
        }

        @Override
        public List<RechargeOutboxEvent> selectDueForShard(int shardTotal, int shardIndex, int limit) {
            return selectDueForShard(shardTotal, shardIndex, limit, List.of(OutboxEventType.SUPPLIER_DISPATCH));
        }

        @Override
        public List<RechargeOutboxEvent> selectDueForShard(
                int shardTotal, int shardIndex, int limit, List<OutboxEventType> eventTypes) {
            scanCount++;
            return events.stream()
                    .filter(event -> eventTypes.contains(event.eventType()))
                    .limit(limit)
                    .toList();
        }

        @Override
        public Optional<RechargeOutboxEvent> findDueByEventId(String eventId, OutboxEventType eventType) {
            findByEventIds.add(eventId);
            return events.stream()
                    .filter(event -> event.eventId().equals(eventId))
                    .filter(event -> event.eventType() == eventType)
                    .findFirst();
        }

        @Override
        public Optional<RechargeOutboxEvent> claim(RechargeOutboxEvent event, String workerId, Duration lease) {
            claimedOrderIds.add(event.orderId());
            return Optional.of(event.claimedCopy());
        }

        @Override
        public void markSuccess(RechargeOutboxEvent event) {
            if (successFailures-- > 0) throw new IllegalStateException("outbox completion unavailable");
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

        private final Map<String, RechargeFulfillmentOrder> ordersById;
        private final List<SupplierFlowStartCommand> flowStarts = new ArrayList<>();
        private final List<SupplierFlowUpdateCommand> flowUpdates = new ArrayList<>();
        private final List<OrderFulfillmentStatusUpdate> statusUpdates = new ArrayList<>();
        private final List<String> balanceReleasedOrderIds = new ArrayList<>();
        private final Map<String, SupplierFlowSnapshot> latestSupplierFlowsByOrderId = new HashMap<>();
        private MinisoBalanceReleaseAuthorization authorization;
        private Runnable onAuthorizationRead = () -> { };
        private boolean hasTrustedVoidReceipt;
        private String authorizationFault = "";
        private int completionFailures;
        private final Map<String, BalanceDeltaFlushOutboxCommand> completedReleaseDeltas = new HashMap<>();

        private FakeFulfillmentOrderPort(RechargeFulfillmentOrder order) {
            this(Map.of(order.orderId(), order));
        }

        private FakeFulfillmentOrderPort(Map<String, RechargeFulfillmentOrder> ordersById) {
            this.ordersById = new HashMap<>(ordersById);
        }

        @Override
        public Optional<RechargeFulfillmentOrder> findByOrderId(String orderId) {
            return Optional.ofNullable(ordersById.get(orderId));
        }

        @Override
        public Optional<SupplierFlowSnapshot> findLatestSupplierFlowByOrderId(String orderId) {
            return Optional.ofNullable(latestSupplierFlowsByOrderId.get(orderId));
        }

        @Override
        public long insertSupplierFlowStart(SupplierFlowStartCommand command) {
            flowStarts.add(command);
            latestSupplierFlowsByOrderId.put(command.orderId(), new SupplierFlowSnapshot(99L, command.status()));
            return 99L;
        }

        @Override
        public void updateSupplierFlow(SupplierFlowUpdateCommand command) {
            flowUpdates.add(command);
        }

        @Override
        public void updateFulfillmentStatus(OrderFulfillmentStatusUpdate update) {
            statusUpdates.add(update);
            var order = ordersById.get(update.orderId());
            if (order != null && "MINISO".equals(order.supplierChannel())) {
                ordersById.put(order.orderId(), new RechargeFulfillmentOrder(order.orderId(), order.outOrderSn(),
                        order.orgCode(), order.account(), order.productId(), order.selectedProductId(), order.supplierChannel(),
                        order.orderPrice(), order.notifyUrl(), update.orderStatus(), order.persistStatus(), order.validationStatus(),
                        order.dispatchAllowed(), order.balanceReserved(), update.dispatchStatus().name(), update.invalidTime()));
            }
        }

        @Override
        public Optional<MinisoBalanceReleaseAuthorization> findMinisoReleaseAuthorization(String orderId) {
            onAuthorizationRead.run();
            return Optional.ofNullable(authorization);
        }

        @Override
        public Optional<MinisoBalanceReleaseAuthorization> authorizeMinisoTerminalRelease(
                OrderFulfillmentStatusUpdate update, String sourceEventId) {
            String fault = authorizationFault;
            authorizationFault = "";
            if (fault.equals("before-authorization")) throw new IllegalStateException("authorization unavailable");
            if (update.minisoReceipt() == null && !hasTrustedVoidReceipt) {
                throw new MinisoWriteOffException(MinisoWriteOffException.Reason.RELEASE_EVIDENCE_MISSING);
            }
            var order = ordersById.get(update.orderId());
            updateFulfillmentStatus(update);
            if (!order.balanceReserved()) return Optional.empty();
            authorization = new MinisoBalanceReleaseAuthorization(order.orderId(), order.orgCode(), order.outOrderSn(),
                    sourceEventId, order.orderPrice(), CLOCK.instant());
            if (fault.equals("after-authorization")) throw new IllegalStateException("authorization response lost");
            return Optional.of(authorization);
        }

        @Override
        public void completeMinisoBalanceRelease(MinisoBalanceReleaseAuthorization supplied) {
            if (completionFailures-- > 0) throw new IllegalStateException("release completion unavailable");
            assertThat(supplied).isEqualTo(authorization);
            completedReleaseDeltas.putIfAbsent("balance-flush-release-" + supplied.orderId(),
                    new BalanceDeltaFlushOutboxCommand(supplied.orgCode(), supplied.orderId(), supplied.outOrderSn(),
                            supplied.amount(), "RELEASE", ""));
            var order = ordersById.get(supplied.orderId());
            ordersById.put(order.orderId(), new RechargeFulfillmentOrder(order.orderId(), order.outOrderSn(),
                    order.orgCode(), order.account(), order.productId(), order.selectedProductId(), order.supplierChannel(),
                    order.orderPrice(), order.notifyUrl(), order.orderStatus(), order.persistStatus(), order.validationStatus(),
                    order.dispatchAllowed(), false, order.dispatchStatus(), order.invalidTime()));
        }

        @Override
        public void markBalanceReleased(String orderId) {
            balanceReleasedOrderIds.add(orderId);
        }
    }

    private static final class SequencedFulfillmentOrderPort implements RechargeFulfillmentOrderPort {

        private final List<RechargeFulfillmentOrder> orders;
        private final List<SupplierFlowStartCommand> flowStarts = new ArrayList<>();
        private final List<SupplierFlowUpdateCommand> flowUpdates = new ArrayList<>();
        private final List<OrderFulfillmentStatusUpdate> statusUpdates = new ArrayList<>();
        private final AtomicInteger lookupCount = new AtomicInteger();

        private SequencedFulfillmentOrderPort(RechargeFulfillmentOrder... orders) {
            this.orders = List.of(orders);
        }

        @Override
        public Optional<RechargeFulfillmentOrder> findByOrderId(String orderId) {
            int index = Math.min(lookupCount.getAndIncrement(), orders.size() - 1);
            RechargeFulfillmentOrder order = orders.get(index);
            return order.orderId().equals(orderId) ? Optional.of(order) : Optional.empty();
        }

        @Override
        public Optional<SupplierFlowSnapshot> findLatestSupplierFlowByOrderId(String orderId) {
            return Optional.empty();
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
        }

        private int lookupCount() {
            return lookupCount.get();
        }
    }

    private static final class FakeSupplierGatewayPort implements RechargeSupplierGatewayPort {

        private final List<SupplierGatewayResponse> dispatchResponses;
        private final SupplierGatewayResponse queryResponse;
        private final RuntimeException dispatchException;
        private int dispatchResponseIndex;
        private final List<SupplierDispatchGatewayRequest> dispatchRequests = new ArrayList<>();
        private final List<SupplierQueryGatewayRequest> queryRequests = new ArrayList<>();
        private final List<SupplierInvalidGatewayRequest> invalidRequests = new ArrayList<>();

        private FakeSupplierGatewayPort(SupplierGatewayResponse response) {
            this(response, response);
        }

        private FakeSupplierGatewayPort(List<SupplierGatewayResponse> dispatchResponses) {
            this(dispatchResponses, dispatchResponses.getFirst());
        }

        private FakeSupplierGatewayPort(
                SupplierGatewayResponse response,
                SupplierGatewayResponse queryResponse) {
            this(List.of(response), queryResponse);
        }

        private FakeSupplierGatewayPort(
                List<SupplierGatewayResponse> dispatchResponses,
                SupplierGatewayResponse queryResponse) {
            this(dispatchResponses, queryResponse, null);
        }

        private FakeSupplierGatewayPort(
                List<SupplierGatewayResponse> dispatchResponses,
                SupplierGatewayResponse queryResponse,
                RuntimeException dispatchException) {
            this.dispatchResponses = dispatchResponses;
            this.queryResponse = queryResponse;
            this.dispatchException = dispatchException;
        }

        private static FakeSupplierGatewayPort throwing(RuntimeException dispatchException) {
            return new FakeSupplierGatewayPort(
                    List.of(SupplierGatewayResponse.retryableFailed("", "EX", dispatchException.getMessage(), "{}")),
                    SupplierGatewayResponse.processing("", "202", "processing", "{}"),
                    dispatchException);
        }

        @Override
        public SupplierGatewayResponse dispatch(SupplierDispatchGatewayRequest request) {
            dispatchRequests.add(request);
            if (dispatchException != null) {
                throw dispatchException;
            }
            SupplierGatewayResponse response = dispatchResponses.get(
                    Math.min(dispatchResponseIndex, dispatchResponses.size() - 1));
            dispatchResponseIndex++;
            return response;
        }

        @Override
        public SupplierGatewayResponse query(SupplierQueryGatewayRequest request) {
            queryRequests.add(request);
            return queryResponse;
        }

        @Override
        public SupplierGatewayResponse invalid(SupplierInvalidGatewayRequest request) {
            invalidRequests.add(request);
            return dispatchResponses.getFirst();
        }
    }

    /**
     * 检测 dispatch/query 是否并发重叠的假 gateway：进入时自增在途计数，>1 则置 overlap，
     * 休眠扩大并发窗口，退出时自减。配合 {@code executeClaimed} 的同账号锁，正确实现下 overlap 恒为 false。
     */
    private static final class ConcurrencyDetectingSupplierGatewayPort implements RechargeSupplierGatewayPort {

        private final AtomicInteger inFlight = new AtomicInteger(0);
        private final AtomicBoolean overlap = new AtomicBoolean(false);
        private final List<SupplierDispatchGatewayRequest> dispatchRequests = new ArrayList<>();
        private final List<SupplierQueryGatewayRequest> queryRequests = new ArrayList<>();

        @Override
        public SupplierGatewayResponse dispatch(SupplierDispatchGatewayRequest request) {
            dispatchRequests.add(request);
            return detectConcurrency(SupplierGatewayResponse.success("SUP-DISPATCH", "0", "success", "{}"));
        }

        @Override
        public SupplierGatewayResponse query(SupplierQueryGatewayRequest request) {
            queryRequests.add(request);
            return detectConcurrency(SupplierGatewayResponse.success("SUP-QUERY", "0", "success", "{}"));
        }

        @Override
        public SupplierGatewayResponse invalid(SupplierInvalidGatewayRequest request) {
            return SupplierGatewayResponse.success("SUP-INVALID", "0", "success", "{}");
        }

        boolean overlap() {
            return overlap.get();
        }

        List<SupplierDispatchGatewayRequest> dispatchRequests() {
            return dispatchRequests;
        }

        List<SupplierQueryGatewayRequest> queryRequests() {
            return queryRequests;
        }

        private SupplierGatewayResponse detectConcurrency(SupplierGatewayResponse response) {
            if (inFlight.incrementAndGet() > 1) {
                overlap.set(true);
            }
            try {
                Thread.sleep(80);
                return response;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return response;
            } finally {
                inFlight.decrementAndGet();
            }
        }
    }

    private static final class FakeSupplierQueryOutboxPort implements SupplierQueryOutboxPort {

        private final List<SupplierQueryOutboxCommand> commands = new ArrayList<>();

        @Override
        public void enqueueSupplierQuery(SupplierQueryOutboxCommand command) {
            commands.add(command);
        }
    }

    private record TimerCall(String eventId, OutboxEventType eventType, Duration delay) {}

    private static final class FakeDispatchTimerPort implements RechargeDispatchTimerPort {

        private final List<TimerCall> calls = new ArrayList<>();

        @Override
        public void schedule(String eventId, OutboxEventType eventType, Duration delay) {
            calls.add(new TimerCall(eventId, eventType, delay));
        }
    }

    private static final class FakeFrequencyPort implements DirectRechargeFrequencyPort {

        private final boolean acquired;
        private final Map<String, String> currentOwners = new HashMap<>();
        private final List<String> releasedOrderIds = new ArrayList<>();

        private FakeFrequencyPort(boolean acquired) {
            this.acquired = acquired;
        }

        void setCurrentOwner(String orgCode, String account, String orderId) {
            currentOwners.put(orgCode + '\0' + account, orderId);
        }

        @Override
        public boolean acquire(String orgCode, String account, String orderId, Duration ttl) {
            return acquired;
        }

        @Override
        public void releaseIfOwned(String orgCode, String account, String orderId, Duration minimumHold) {
            releasedOrderIds.add(orderId);
        }

        @Override
        public Optional<String> currentOwner(String orgCode, String account) {
            return Optional.ofNullable(currentOwners.get(orgCode + '\0' + account));
        }
    }

    private static final class FakeNotifyTaskPort implements RechargeNotifyTaskPort {

        private final List<RechargeNotifyTaskDraft> createdTasks = new ArrayList<>();
        private int failuresRemaining;

        @Override
        public void insertIfAbsent(RechargeNotifyTaskDraft task) {
            if (failuresRemaining-- > 0) throw new IllegalStateException("notify creation unavailable");
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

    /** 仅模拟释放端口的幂等合同，不代表执行或验证真实Redis Lua。 */
    private static final class IdempotentBalanceReleasePort implements BalanceReservationReleasePort {
        private long balanceIncrease;
        private BalanceReservationReleaseCommand firstCommand;
        private String fault = "";

        @Override
        public BalanceReservationReleaseResult release(BalanceReservationReleaseCommand command) {
            String currentFault = fault;
            fault = "";
            if (currentFault.equals("redis-unavailable")) return BalanceReservationReleaseResult.failed("DOWN", "unavailable");
            if (firstCommand != null) {
                assertThat(command.orgCode()).isEqualTo(firstCommand.orgCode());
                assertThat(command.orderId()).isEqualTo(firstCommand.orderId());
                assertThat(command.outOrderSn()).isEqualTo(firstCommand.outOrderSn());
                assertThat(command.releaseEventId()).isEqualTo(firstCommand.releaseEventId());
                assertThat(command.amount()).isEqualTo(firstCommand.amount());
                return BalanceReservationReleaseResult.alreadyReleasedSuccess();
            }
            firstCommand = command;
            balanceIncrease += command.amount();
            if (currentFault.equals("redis-response-lost")) throw new IllegalStateException("redis response lost");
            return BalanceReservationReleaseResult.releasedSuccess();
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
                    "useState", "3",
                    "invalidTime", invalidTime.toString(),
                    "sign", "signed-write-off");
        }
    }

    private record OutboxFailure(String eventId, String errorMessage, Duration nextRetryDelay) {
    }
}
