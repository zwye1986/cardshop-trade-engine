package com.cardshop.trade.engine.simulator.supplier;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;
import java.util.function.LongUnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

class SupplierMockOrderStoreTest {

    private static final String ORDER_KEY = "order-001";

    @Test
    void registerDispatchFirstCallReturnsProcessingAndStoresReadyAtAndFinalStatus() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-20T10:00:00Z"));
        LongUnaryOperator delaySampler = delta -> 2_000L;
        DoubleSupplier successSampler = () -> 0.0;

        SupplierMockOrderStore store = new SupplierMockOrderStore(
                1_000L, 5_000L, 1.0, clock, delaySampler, successSampler);

        SupplierMockOrderStore.MockResponse dispatch = store.registerDispatch(ORDER_KEY);

        assertThat(dispatch.status()).isEqualTo("PROCESSING");
        assertThat(dispatch.code()).isEqualTo("202");
        assertThat(dispatch.supplierOrderId()).isNotBlank();

        clock.advanceMillis(2_999L);
        SupplierMockOrderStore.MockResponse beforeReady = store.query(ORDER_KEY);
        assertThat(beforeReady.status()).isEqualTo("PROCESSING");
        assertThat(beforeReady.code()).isEqualTo("202");
        assertThat(beforeReady.supplierOrderId()).isEqualTo(dispatch.supplierOrderId());

        clock.advanceMillis(1L);
        SupplierMockOrderStore.MockResponse afterReady = store.query(ORDER_KEY);
        assertThat(afterReady.status()).isEqualTo("SUCCESS");
        assertThat(afterReady.code()).isEqualTo("0");
        assertThat(afterReady.supplierOrderId()).isEqualTo(dispatch.supplierOrderId());
    }

    @Test
    void queryBeforeReadyAtReturnsProcessingAfterReadyAtReturnsPredeterminedTerminalStatus() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-20T10:00:00Z"));
        SupplierMockOrderStore store = new SupplierMockOrderStore(
                0L, 800L, 0.5, clock, delta -> 800L, () -> 0.9);

        store.registerDispatch(ORDER_KEY);

        clock.advanceMillis(799L);
        assertThat(store.query(ORDER_KEY).status()).isEqualTo("PROCESSING");

        clock.advanceMillis(1L);
        assertThat(store.query(ORDER_KEY).status()).isEqualTo("FAILED");
        assertThat(store.query(ORDER_KEY).code()).isEqualTo("E_MOCK_FAIL");
    }

    @Test
    void firstRegisterDispatchAlwaysReturnsProcessingEvenWhenReadyAtIsNow() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-20T10:00:00Z"));
        SupplierMockOrderStore store = new SupplierMockOrderStore(
                0L, 0L, 1.0, clock, delta -> 0L, () -> 0.0);

        SupplierMockOrderStore.MockResponse dispatch = store.registerDispatch(ORDER_KEY);

        assertThat(dispatch.status()).isEqualTo("PROCESSING");
        assertThat(dispatch.code()).isEqualTo("202");
        assertThat(store.query(ORDER_KEY).status()).isEqualTo("SUCCESS");
        assertThat(store.query(ORDER_KEY).code()).isEqualTo("0");
    }

    @Test
    void successRateOneAlwaysSucceeds() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-20T10:00:00Z"));
        SupplierMockOrderStore store = new SupplierMockOrderStore(
                0L, 0L, 1.0, clock, delta -> 0L, () -> 0.999);

        store.registerDispatch(ORDER_KEY);
        clock.advanceMillis(0L);

        SupplierMockOrderStore.MockResponse response = store.query(ORDER_KEY);
        assertThat(response.status()).isEqualTo("SUCCESS");
        assertThat(response.code()).isEqualTo("0");
    }

    @Test
    void successRateZeroAlwaysFails() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-20T10:00:00Z"));
        SupplierMockOrderStore store = new SupplierMockOrderStore(
                0L, 0L, 0.0, clock, delta -> 0L, () -> 0.0);

        store.registerDispatch(ORDER_KEY);
        clock.advanceMillis(0L);

        SupplierMockOrderStore.MockResponse response = store.query(ORDER_KEY);
        assertThat(response.status()).isEqualTo("FAILED");
        assertThat(response.code()).isEqualTo("E_MOCK_FAIL");
    }

    @Test
    void repeatedDispatchDoesNotResetReadyAtOrFinalStatus() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-20T10:00:00Z"));
        AtomicLong samplerCalls = new AtomicLong();
        SupplierMockOrderStore store = new SupplierMockOrderStore(
                1_000L, 1_000L, 1.0, clock,
                delta -> {
                    samplerCalls.incrementAndGet();
                    return 0L;
                },
                () -> {
                    samplerCalls.incrementAndGet();
                    return 0.0;
                });

        SupplierMockOrderStore.MockResponse first = store.registerDispatch(ORDER_KEY);
        clock.advanceMillis(500L);

        SupplierMockOrderStore.MockResponse second = store.registerDispatch(ORDER_KEY);
        assertThat(second.status()).isEqualTo("PROCESSING");
        assertThat(second.supplierOrderId()).isEqualTo(first.supplierOrderId());

        clock.advanceMillis(600L);
        SupplierMockOrderStore.MockResponse third = store.registerDispatch(ORDER_KEY);
        assertThat(third.status()).isEqualTo("SUCCESS");
        assertThat(third.supplierOrderId()).isEqualTo(first.supplierOrderId());
        assertThat(store.query(ORDER_KEY).status()).isEqualTo("SUCCESS");
        assertThat(samplerCalls.get()).isEqualTo(2L);
    }

    @Test
    void queryUnknownOrderReturnsFailedWithNotFoundCode() {
        MutableClock clock = new MutableClock(Instant.parse("2026-07-20T10:00:00Z"));
        SupplierMockOrderStore store = new SupplierMockOrderStore(
                0L, 0L, 1.0, clock, delta -> 0L, () -> 0.0);

        SupplierMockOrderStore.MockResponse response = store.query("missing-order");

        assertThat(response.status()).isEqualTo("FAILED");
        assertThat(response.code()).isEqualTo("E_MOCK_NOT_FOUND");
        assertThat(response.supplierOrderId()).isNull();
    }

    private static final class MutableClock extends Clock {

        private Instant instant;
        private final ZoneId zone = ZoneId.of("UTC");

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        private void advanceMillis(long millis) {
            instant = instant.plusMillis(millis);
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
