package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.cardshop.trade.engine.application.v1.*;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** 无后台扫描的请求内推进与资金收尾分离，参见详细设计9.3.3/9.3.5。 */
class RechargeOrderInvalidationServiceTest {
    @Test void committedCompletionWinsEvenWhenWorkerFailsDuringCleanup() {
        var pending = state(RechargeOrderInvalidationState.Status.PENDING);
        var complete = state(RechargeOrderInvalidationState.Status.COMPLETED);
        var port = mock(InvalidRechargeOrderOutboxPort.class);
        var worker = mock(RechargeOutboxWorkerService.class);
        var command = new InvalidRechargeOrderOutboxCommand("A", "O", "OUT", "");
        when(port.prepareInvalidOrder(command)).thenReturn(pending);
        when(port.findInvalidOrder("A", "OUT")).thenReturn(Optional.of(complete));
        when(worker.processEvent("invalid-O", OutboxEventType.INVALID_ORDER, "http-fixture"))
                .thenThrow(new IllegalStateException("cleanup failed"));
        var service = new RechargeOrderInvalidationService(port, worker, "http-fixture");
        assertThat(service.invalidate(command, prepared -> assertThat(prepared).isEqualTo(pending))).isEqualTo(complete);
    }

    @Test void existingManualOperationIsNotReactivated() {
        var port = mock(InvalidRechargeOrderOutboxPort.class);
        var worker = mock(RechargeOutboxWorkerService.class);
        var command = new InvalidRechargeOrderOutboxCommand("A", "O", "OUT", "");
        var manual = state(RechargeOrderInvalidationState.Status.MANUAL_REQUIRED);
        when(port.prepareInvalidOrder(command)).thenReturn(manual);
        assertThat(new RechargeOrderInvalidationService(port, worker, "http-fixture")
                .invalidate(command, prepared -> {})).isEqualTo(manual);
        verifyNoInteractions(worker);
    }

    private static RechargeOrderInvalidationState state(RechargeOrderInvalidationState.Status status) {
        return new RechargeOrderInvalidationState(new RechargeOrderSnapshot("O", "OUT", "SUCCESS",
                "DB_PERSISTED", "PASSED", "", "", true, "3", "3", "", "2026-11-05 11:00:00"),
                "invalid-O", status, "");
    }
}
