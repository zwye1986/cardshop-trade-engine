package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * D9/D10 outbox Worker 参数默认值测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」：兼容构造器的默认查单退避
 * 必须与设计文档和 Nacos/Spring 配置默认值保持一致，避免测试或兜底路径绕过配置对象后使用旧节奏。
 */
class RechargeOutboxWorkerSettingsTest {

    @Test
    void legacyConstructorUsesDesignSupplierQueryDelays() {
        RechargeOutboxWorkerSettings settings = new RechargeOutboxWorkerSettings(
                Duration.ofMinutes(2),
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                Duration.ofMinutes(30),
                Duration.ofSeconds(5));

        assertThat(settings.supplierQueryRetryDelays())
                .containsExactly(
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(4),
                        Duration.ofSeconds(6),
                        Duration.ofSeconds(8),
                        Duration.ofSeconds(16),
                        Duration.ofSeconds(32));
    }

    @Test
    void exposesDefaultSupplierQueryDelaysAsSharedConfigurationContract() {
        assertThat(RechargeOutboxWorkerSettings.defaultSupplierQueryRetryDelays())
                .containsExactly(
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(4),
                        Duration.ofSeconds(6),
                        Duration.ofSeconds(8),
                        Duration.ofSeconds(16),
                        Duration.ofSeconds(32));
    }
}
