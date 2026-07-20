package com.cardshop.trade.engine.simulator.a39;

import java.util.Locale;

/**
 * A39 模拟器运行模式。
 *
 * <p>参见 {@code docs/superpowers/specs/2026-07-15-a39-simulator-load-test-design.md}：
 * {@link #SCENARIO} 保持单笔联调语义，{@link #LOAD} 只压创建订单入口。
 * {@link #SUPPLIER_MOCK} 参见 {@code docs/superpowers/specs/2026-07-20-supplier-mock-design.md}。
 */
public enum A39SimulatorMode {

    SCENARIO,
    LOAD,
    SUPPLIER_MOCK;

    static A39SimulatorMode parse(String value) {
        String normalized = value == null || value.isBlank() ? "scenario" : value.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "scenario" -> SCENARIO;
            case "load" -> LOAD;
            case "supplier-mock", "supplier_mock" -> SUPPLIER_MOCK;
            default -> throw new IllegalArgumentException("mode must be scenario, load, or supplier-mock");
        };
    }
}
