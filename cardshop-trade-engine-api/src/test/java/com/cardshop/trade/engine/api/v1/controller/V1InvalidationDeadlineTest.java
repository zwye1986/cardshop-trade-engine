package com.cardshop.trade.engine.api.v1.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.api.v1.dto.V1CardApiResponse;
import com.cardshop.trade.engine.api.v1.routing.V1RouteDecisionService;
import com.cardshop.trade.engine.api.v1.routing.V1RoutingSettings;
import com.cardshop.trade.engine.api.v1.security.*;
import com.cardshop.trade.engine.api.v1.V12TestCredentials;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderContext;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import reactor.core.publisher.Mono;

/** 完整body起算的真实调度边界；参见详细设计9.3.4，认证排队不得延长响应期限。 */
class V1InvalidationDeadlineTest {
    /** 纳秒时间源与生产共用剩余预算判断，不用Reactor虚拟调度替代System.nanoTime合同。 */
    @ParameterizedTest(name = "首次受理相对截止 {0}ns")
    @ValueSource(longs = {-1, 0, 1})
    void admissionRequiresStrictlyPositiveBudgetEvenBeforeDeadlineCallback(long offsetNanos) {
        var nowNanos = new AtomicLong(0);
        var timeout = InvalidRechargeOrderContext.DEFAULT_WAIT_TIMEOUT;
        var context = new InvalidRechargeOrderContext(timeout, nowNanos::get);
        var pending = new V1CardApiResult("202", "pending", null);
        nowNanos.set(timeout.toNanos() + offsetNanos);

        // 故意不先调用closeResponse，证明调度器迟到也不能让未开始的受理晚启动。
        assertThat(context.remainingNanos()).isEqualTo(Math.max(0, -offsetNanos));
        assertThat(context.preparing()).isEqualTo(offsetNanos < 0);
        assertThat(context.beginAdmission("A36", "EXISTING", pending)).isEqualTo(offsetNanos < 0);
        assertThat(context.view().phase()).isEqualTo(offsetNanos < 0
                ? InvalidRechargeOrderContext.Phase.ADMITTING : InvalidRechargeOrderContext.Phase.PREPARING);
        assertThat(context.closeResponse().code()).isEqualTo(offsetNanos < 0 ? "202" : "500");
        assertThat(context.beginAdmission("A36", "LATE", pending)).isFalse();
    }

    @ParameterizedTest(name = "完成发布相对截止 {0}ns")
    @ValueSource(longs = {-1, 0, 1})
    void completionMustBePublishedBeforeDeadlineToReplacePending(long offsetNanos) {
        var nowNanos = new AtomicLong(0);
        var timeout = InvalidRechargeOrderContext.DEFAULT_WAIT_TIMEOUT;
        var context = new InvalidRechargeOrderContext(timeout, nowNanos::get);
        var pending = new V1CardApiResult("202", "pending", null);
        var completed = new V1CardApiResult("200", "completed", null);
        assertThat(context.beginAdmission("A36", "EXISTING", pending)).isTrue();
        context.prepared(pending);
        nowNanos.set(timeout.toNanos() + offsetNanos);

        // 截止同刻remaining=0：即使截止回调尚未运行，也不能覆盖最后的安全202快照。
        assertThat(context.finish(completed)).isSameAs(offsetNanos < 0 ? completed : pending);
        nowNanos.set(timeout.toNanos() + 1);
        assertThat(context.closeResponse()).isSameAs(offsetNanos < 0 ? completed : pending);
        assertThat(context.finish(completed)).isSameAs(offsetNanos < 0 ? completed : pending);
    }

    @Test
    void closingBeforeAdmissionWinsEvenWithOneNanosecondLeft() {
        var nowNanos = new AtomicLong(0);
        var timeout = InvalidRechargeOrderContext.DEFAULT_WAIT_TIMEOUT;
        var context = new InvalidRechargeOrderContext(timeout, nowNanos::get);
        nowNanos.set(timeout.toNanos() - 1);
        assertThat(context.closeResponse().code()).isEqualTo("500");
        assertThat(context.beginAdmission("A36", "EXISTING", new V1CardApiResult("202", "pending", null))).isFalse();
        assertThat(context.view().orgCode()).isNull();
    }

    @Test
    void deadlineClosingBetweenAdmissionSnapshotAndCasPreventsAdmission() throws Exception {
        var timeout = InvalidRechargeOrderContext.DEFAULT_WAIT_TIMEOUT;
        var clockReads = new AtomicInteger();
        var budgetRead = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var context = new InvalidRechargeOrderContext(timeout, () -> {
            if (clockReads.getAndIncrement() == 0) return 0; // 构造时的单调起点。
            // 在beginAdmission已读取PREPARING后停住；保留截止前采样，让剩余预算为正，
            // 证明即使预算检查放行，截止关闭仍能通过同一状态CAS阻止受理。
            long sampledNanos = timeout.toNanos() - 1;
            budgetRead.countDown();
            await(release);
            return sampledNanos;
        });
        try (var executor = Executors.newSingleThreadExecutor()) {
            var admission = executor.submit(() -> context.beginAdmission(
                    "A36", "EXISTING", new V1CardApiResult("202", "pending", null)));
            try {
                assertThat(budgetRead.await(1, TimeUnit.SECONDS)).isTrue();
                assertThat(context.closeResponse().code()).isEqualTo("500");
            } finally {
                release.countDown();
            }
            assertThat(admission.get(1, TimeUnit.SECONDS)).isFalse();
            assertThat(context.view().phase()).isEqualTo(InvalidRechargeOrderContext.Phase.CLOSED);
            assertThat(context.view().orgCode()).isNull();
        }
    }

    @Test
    void admissionOneNanosecondBeforeDeadlineContinuesAfterResponseCloses() throws Exception {
        var nowNanos = new AtomicLong(0);
        var timeout = InvalidRechargeOrderContext.DEFAULT_WAIT_TIMEOUT;
        var context = new InvalidRechargeOrderContext(timeout, nowNanos::get);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var persisted = new AtomicInteger();
        var pending = new V1CardApiResult("202", "pending", null);
        nowNanos.set(timeout.toNanos() - 1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var execution = executor.submit(() -> {
                boolean admitted = context.beginAdmission("A36", "EXISTING", pending);
                entered.countDown();
                assertThat(admitted).isTrue();
                await(release);
                persisted.incrementAndGet();
                return context.finish(new V1CardApiResult("200", "completed", null));
            });
            try {
                assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
                nowNanos.set(timeout.toNanos());
                assertThat(context.closeResponse()).isSameAs(pending);
                assertThat(persisted.get()).isZero();
                nowNanos.incrementAndGet();
            } finally {
                release.countDown();
            }
            assertThat(execution.get(1, TimeUnit.SECONDS)).isSameAs(pending);
            assertThat(persisted.get()).isEqualTo(1);
            assertThat(context.view().phase()).isEqualTo(InvalidRechargeOrderContext.Phase.CLOSED);
        }
    }

    @Test
    void slowAuthenticationTimesOutWithoutLateAdmission() throws Exception {
        var release = new CountDownLatch(1);
        var finished = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var auth = new V1TokenAuthenticator(new V1TokenSettings("fixture", "fixture:", Duration.ofSeconds(30)),
                Clock.systemUTC(), Optional.empty()) {
            @Override public Optional<AuthenticatedMerchant> authenticate(String token) {
                try {
                    release.await(8, TimeUnit.SECONDS);
                    return Optional.of(new AuthenticatedMerchant("app", "A36", V12TestCredentials.CREDENTIAL));
                } catch (InterruptedException ex) {
                    throw new IllegalStateException(ex);
                } finally { finished.countDown(); }
            }
        };
        var controller = new V1RechargeOrderController(new V1RouteDecisionService(new V1RoutingSettings(
                true, "http://unused", Duration.ofSeconds(3), List.of("A36"), List.of("app"),
                List.of("/v1/cardapi/order/invalid"))), auth,
                (request, body) -> Mono.just(ResponseEntity.ok("unused")),
                command -> CompletableFuture.completedFuture(new V1CardApiResult("200", "unused", null)),
                command -> new V1CardApiResult("200", "unused", null),
                (command, context) -> { calls.incrementAndGet(); return new V1CardApiResult("200", "wrong", null); });
        var response = controller.invalidRechargeOrder("Bearer fixture", Mono.just("{}"),
                MockServerHttpRequest.post("/v1/cardapi/order/invalid").build()).toFuture();
        try {
            var result = response.get(5500, TimeUnit.MILLISECONDS);
            assertThat(result.getBody()).isInstanceOfSatisfying(V1CardApiResponse.class,
                    body -> { assertThat(body.code()).isEqualTo("500"); assertThat(body.data()).isNull(); });
        } finally { release.countDown(); }
        assertThat(finished.await(2, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(100);
        assertThat(calls.get()).isZero();
    }

    @Test
    void deadlineDuringAdmissionReturnsPendingAndDoesNotCancelExecution() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var persisted = new CountDownLatch(1);
        var useCase = (com.cardshop.trade.engine.application.v1.InvalidRechargeOrderUseCase) (command, context) -> {
            assertThat(context.beginAdmission("A36", "EXISTING", new V1CardApiResult("202", "pending", null))).isTrue();
            entered.countDown();
            await(release);
            persisted.countDown();
            return context.finish(new V1CardApiResult("200", "completed", null));
        };
        var response = controller(useCase, Duration.ofMillis(200)).invalidRechargeOrder("fixture", Mono.just("{}"),
                MockServerHttpRequest.post("/v1/cardapi/order/invalid").build()).toFuture();
        try {
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(((V1CardApiResponse<?>) response.get(2, TimeUnit.SECONDS).getBody()).code()).isEqualTo("202");
            assertThat(persisted.getCount()).isEqualTo(1);
        } finally { release.countDown(); }
        assertThat(persisted.await(1, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void independentObservationCanCompleteBeforeExecutionFinishesFundsRecovery() throws Exception {
        var release = new CountDownLatch(1);
        var persisted = new CountDownLatch(1);
        var finished = new CountDownLatch(1);
        var useCase = new com.cardshop.trade.engine.application.v1.InvalidRechargeOrderUseCase() {
            @Override public V1CardApiResult invalidRechargeOrder(
                    com.cardshop.trade.engine.application.v1.V1MerchantCommand command,
                    com.cardshop.trade.engine.application.v1.InvalidRechargeOrderContext context) {
                context.beginAdmission("A36", "EXISTING", new V1CardApiResult("202", "pending", null));
                context.prepared(new V1CardApiResult("202", "pending", null));
                persisted.countDown();
                await(release);
                finished.countDown();
                return context.response();
            }
            @Override public Optional<V1CardApiResult> observeInvalidRechargeOrder(
                    com.cardshop.trade.engine.application.v1.InvalidRechargeOrderContext context) {
                return persisted.getCount() == 0
                        ? Optional.of(context.finish(new V1CardApiResult("200", "durably completed", null)))
                        : Optional.empty();
            }
        };
        try {
            var response = controller(useCase, Duration.ofSeconds(2)).invalidRechargeOrder("fixture", Mono.just("{}"),
                    MockServerHttpRequest.post("/v1/cardapi/order/invalid").build()).block(Duration.ofSeconds(3));
            assertThat(((V1CardApiResponse<?>) response.getBody()).code()).isEqualTo("200");
            assertThat(finished.getCount()).isEqualTo(1);
        } finally { release.countDown(); }
        assertThat(finished.await(1, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void clientCancellationAfterAdmissionDoesNotPreventPersistence() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var persisted = new CountDownLatch(1);
        var useCase = (com.cardshop.trade.engine.application.v1.InvalidRechargeOrderUseCase) (command, context) -> {
            context.beginAdmission("A36", "EXISTING", new V1CardApiResult("202", "pending", null));
            entered.countDown();
            await(release);
            persisted.countDown();
            return context.finish(new V1CardApiResult("200", "completed", null));
        };
        var response = controller(useCase, Duration.ofSeconds(2)).invalidRechargeOrder("fixture", Mono.just("{}"),
                MockServerHttpRequest.post("/v1/cardapi/order/invalid").build()).toFuture();
        try {
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            response.cancel(true);
        } finally { release.countDown(); }
        assertThat(persisted.await(1, TimeUnit.SECONDS)).isTrue();
    }

    private static V1RechargeOrderController controller(
            com.cardshop.trade.engine.application.v1.InvalidRechargeOrderUseCase useCase, Duration timeout) {
        var auth = new V1TokenAuthenticator(new V1TokenSettings("fixture", "fixture:", Duration.ofSeconds(30)),
                Clock.systemUTC(), Optional.empty()) {
            @Override public Optional<AuthenticatedMerchant> authenticate(String token) {
                return Optional.of(new AuthenticatedMerchant("app", "A36", V12TestCredentials.CREDENTIAL));
            }
        };
        return new V1RechargeOrderController(new V1RouteDecisionService(new V1RoutingSettings(
                true, "http://unused", Duration.ofSeconds(3), List.of("A36"), List.of("app"),
                List.of("/v1/cardapi/order/invalid"))), auth,
                (request, body) -> Mono.just(ResponseEntity.ok("unused")),
                command -> CompletableFuture.completedFuture(new V1CardApiResult("200", "unused", null)),
                command -> new V1CardApiResult("200", "unused", null), useCase,
                new com.cardshop.trade.engine.api.v1.usecase.V1InvalidationSettings(timeout, (code, elapsed) -> {}));
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(3, TimeUnit.SECONDS)) throw new IllegalStateException("fixture gate timed out");
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("execution was cancelled", ex);
        }
    }
}
