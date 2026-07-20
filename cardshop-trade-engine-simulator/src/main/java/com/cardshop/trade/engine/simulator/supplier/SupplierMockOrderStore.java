package com.cardshop.trade.engine.simulator.supplier;

import java.security.SecureRandom;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoubleSupplier;
import java.util.function.LongUnaryOperator;

/**
 * 供应商 mock 内存订单表，实现下单登记与查单判定。
 *
 * <p>下单立即返回 {@code PROCESSING}，并按配置抽样终态就绪时间与最终 {@code SUCCESS}/{@code FAILED}；
 * 查单在 {@code readyAt} 之前持续 {@code PROCESSING}，之后返回下单时已确定的终态。参见
 * {@code docs/superpowers/specs/2026-07-20-supplier-mock-design.md}「状态机」。
 */
public final class SupplierMockOrderStore {

    private static final String STATUS_PROCESSING = "PROCESSING";
    private static final String STATUS_SUCCESS = "SUCCESS";
    private static final String STATUS_FAILED = "FAILED";

    private static final String CODE_PROCESSING = "202";
    private static final String CODE_SUCCESS = "0";
    private static final String CODE_FAILED = "E_MOCK_FAIL";
    private static final String CODE_NOT_FOUND = "E_MOCK_NOT_FOUND";

    private static final String MESSAGE_PROCESSING = "订单处理中";
    private static final String MESSAGE_SUCCESS = "充值成功";
    private static final String MESSAGE_FAILED = "充值失败";
    private static final String MESSAGE_NOT_FOUND = "订单不存在";

    private final long delayMsMin;
    private final long delayMsMax;
    private final double successRate;
    private final Clock clock;
    private final LongUnaryOperator delaySampler;
    private final DoubleSupplier successSampler;
    private final ConcurrentHashMap<String, OrderRecord> orders = new ConcurrentHashMap<>();

    /**
     * 可注入抽样器的构造器，供单测使用固定时序与成功率。
     */
    public SupplierMockOrderStore(
            long delayMsMin,
            long delayMsMax,
            double successRate,
            Clock clock,
            LongUnaryOperator delaySampler,
            DoubleSupplier successSampler) {
        if (delayMsMin < 0 || delayMsMax < 0) {
            throw new IllegalArgumentException("final-result-delay must not be negative");
        }
        if (delayMsMin > delayMsMax) {
            throw new IllegalArgumentException("final-result-delay-ms-min must not be greater than final-result-delay-ms-max");
        }
        if (successRate < 0.0 || successRate > 1.0) {
            throw new IllegalArgumentException("success-rate must be between 0.0 and 1.0");
        }
        this.delayMsMin = delayMsMin;
        this.delayMsMax = delayMsMax;
        this.successRate = successRate;
        this.clock = clock;
        this.delaySampler = delaySampler;
        this.successSampler = successSampler;
    }

    /**
     * 使用 {@link SecureRandom} 抽样的便捷构造器，供运行时 mock 进程使用。
     */
    public SupplierMockOrderStore(long delayMsMin, long delayMsMax, double successRate) {
        this(delayMsMin, delayMsMax, successRate, Clock.systemUTC(), defaultDelaySampler(), defaultSuccessSampler());
    }

    /**
     * 登记下单：新订单抽样 {@code readyAt} 与终态，并<strong>始终</strong>立即返回 {@code PROCESSING}；
     * 重复下单幂等返回当前可见状态（{@code readyAt} 前 {@code PROCESSING}，之后终态）。
     */
    public MockResponse registerDispatch(String orderKey) {
        requireOrderKey(orderKey);
        AtomicBoolean created = new AtomicBoolean(false);
        OrderRecord record = orders.computeIfAbsent(orderKey, key -> {
            created.set(true);
            return createOrderRecord(key);
        });
        if (created.get()) {
            return toProcessingResponse(record);
        }
        return toVisibleResponse(record);
    }

    /**
     * 查单：未知订单返回 {@code FAILED + E_MOCK_NOT_FOUND}；已知订单按 {@code readyAt} 判定。
     */
    public MockResponse query(String orderKey) {
        requireOrderKey(orderKey);
        OrderRecord record = orders.get(orderKey);
        if (record == null) {
            return new MockResponse(STATUS_FAILED, CODE_NOT_FOUND, MESSAGE_NOT_FOUND, null);
        }
        return toVisibleResponse(record);
    }

    private OrderRecord createOrderRecord(String orderKey) {
        long delayDelta = delayMsMax - delayMsMin;
        long sampledDelay = delaySampler.applyAsLong(delayDelta);
        long readyAt = clock.millis() + delayMsMin + sampledDelay;
        String finalStatus = successSampler.getAsDouble() < successRate ? STATUS_SUCCESS : STATUS_FAILED;
        return new OrderRecord(UUID.randomUUID().toString(), readyAt, finalStatus);
    }

    private MockResponse toProcessingResponse(OrderRecord record) {
        return new MockResponse(STATUS_PROCESSING, CODE_PROCESSING, MESSAGE_PROCESSING, record.supplierOrderId());
    }

    private MockResponse toVisibleResponse(OrderRecord record) {
        if (clock.millis() < record.readyAt()) {
            return toProcessingResponse(record);
        }
        if (STATUS_SUCCESS.equals(record.finalStatus())) {
            return new MockResponse(STATUS_SUCCESS, CODE_SUCCESS, MESSAGE_SUCCESS, record.supplierOrderId());
        }
        return new MockResponse(STATUS_FAILED, CODE_FAILED, MESSAGE_FAILED, record.supplierOrderId());
    }

    private static void requireOrderKey(String orderKey) {
        if (orderKey == null || orderKey.isBlank()) {
            throw new IllegalArgumentException("orderKey must not be blank");
        }
    }

    private static LongUnaryOperator defaultDelaySampler() {
        SecureRandom random = new SecureRandom();
        return delta -> delta == 0L ? 0L : Math.floorMod(random.nextLong(), delta + 1);
    }

    private static DoubleSupplier defaultSuccessSampler() {
        SecureRandom random = new SecureRandom();
        return random::nextDouble;
    }

    /**
     * mock 响应体，字段语义与 {@code HttpRechargeSupplierGatewayAdapter} 解析约定一致。
     */
    public record MockResponse(String status, String code, String message, String supplierOrderId) {}

    private record OrderRecord(String supplierOrderId, long readyAt, String finalStatus) {}
}
