package com.cardshop.trade.engine.application.v1;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * 单次响应期限与首次受理竞争，参见详细设计9.3.4。
 * 原子发布阶段、已验签身份及安全响应；HTTP结束只关闭受理机会，不取消已开始的事务/执行。
 */
public final class InvalidRechargeOrderContext {
    public static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(5);
    public enum Phase { PREPARING, ADMITTING, OBSERVING, CLOSED }
    public record View(Phase phase, String orgCode, String outOrderSn, V1CardApiResult response) {}
    private final long deadlineNanos;
    private final LongSupplier nanoTime;
    private final AtomicReference<View> state = new AtomicReference<>(new View(Phase.PREPARING, null, null, null));

    public InvalidRechargeOrderContext(Duration timeout) {
        this(timeout, System::nanoTime);
    }

    /**
     * 使用同一单调纳秒时间源建立和读取预算，参见详细设计9.3.4。
     * 默认构造仍用System.nanoTime；此入口允许跨模块边界测试精确推进到截止前、同刻及之后，
     * 不改变受理或发布判定。时间源须与System.nanoTime一样单调，不能使用业务墙钟。
     */
    public InvalidRechargeOrderContext(Duration timeout, LongSupplier nanoTime) {
        if (timeout.isZero() || timeout.isNegative()) throw new IllegalArgumentException("invalidation timeout must be positive");
        this.nanoTime = nanoTime;
        deadlineNanos = nanoTime.getAsLong() + timeout.toNanos();
    }

    public long remainingNanos() { return Math.max(0, deadlineNanos - nanoTime.getAsLong()); }
    public View view() { return state.get(); }

    public boolean preparing() { return state.get().phase() == Phase.PREPARING && remainingNanos() > 0; }

    /** 验签及订单归属完成后、进入受理事务前调用；失败时禁止产生首次事件。 */
    public boolean beginAdmission(String orgCode, String outOrderSn, V1CardApiResult pending) {
        var before = state.get();
        return remainingNanos() > 0 && before.phase() == Phase.PREPARING
                && state.compareAndSet(before, new View(Phase.ADMITTING, orgCode, outOrderSn, pending));
    }

    public void prepared(V1CardApiResult result) { publish(result, Phase.OBSERVING); }
    public V1CardApiResult finish(V1CardApiResult result) {
        publish(result, Phase.CLOSED);
        return response();
    }

    private void publish(V1CardApiResult result, Phase phase) {
        for (;;) {
            var before = state.get();
            if (before.phase() == Phase.CLOSED || remainingNanos() == 0) return;
            if (state.compareAndSet(before, new View(phase, before.orgCode(), before.outOrderSn(), result))) return;
        }
    }

    /** 截止/断开在同一原子快照上关闭；PREPARING没有已验证订单数据，只返回繁忙。 */
    public V1CardApiResult closeResponse() {
        var closed = state.updateAndGet(v -> new View(Phase.CLOSED, v.orgCode(), v.outOrderSn(), v.response()));
        return closed.response() == null ? V1CardApiResultStatus.SYSTEM_BUSY.result() : closed.response();
    }

    public V1CardApiResult response() {
        var result = state.get().response();
        return result == null ? V1CardApiResultStatus.SYSTEM_BUSY.result() : result;
    }
}
