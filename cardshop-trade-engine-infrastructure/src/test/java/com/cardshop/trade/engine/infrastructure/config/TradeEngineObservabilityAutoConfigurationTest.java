package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilitySnapshotProvider;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * 业务监控自动配置测试。
 *
 * <p>参见 {@code docs/开发进度.md}「5.1 监控告警最小闭环执行边界」。监控 Bean 只依赖 Micrometer
 * 和低频快照 Provider；Redis/MySQL 不齐备时也必须能用空快照启动，避免 local/slice 测试被外部中间件拖垮。
 */
class TradeEngineObservabilityAutoConfigurationTest {

    @Test
    void registersMetricsWhenMeterRegistryIsAvailable() {
        new ApplicationContextRunner()
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(TradeEngineObservabilitySnapshotProvider.class, TradeEngineObservabilitySnapshotProvider::empty)
                .withConfiguration(AutoConfigurations.of(TradeEngineObservabilityAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(TradeEngineObservabilityMetrics.class);
                    assertThat(context.getBean(SimpleMeterRegistry.class)
                                    .find("trade.engine.redis.pending.total")
                                    .tag("environment", "dev")
                                    .gauge())
                            .isNotNull();
                });
    }

    @Test
    void skipsMetricsWhenMeterRegistryIsMissing() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(TradeEngineObservabilityAutoConfiguration.class))
                .run(context -> assertThat(context).doesNotHaveBean(TradeEngineObservabilityMetrics.class));
    }
}
