package com.cardshop.trade.engine.infrastructure.observability;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.cardshop.trade.engine.application.recharge.MinisoWriteOffBatchResult;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoWriteOffEventMapper;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

import java.time.*;

/** 指标scrape使用缓存，不放大数据库压力；参见详细设计9.5。 */
class MinisoWriteOffMetricsTest {
    @Test
    void refreshesOnlyAtBatchBoundaryAndNeverQueriesOnScrape() {
        var mapper = mock(MinisoWriteOffEventMapper.class);
        when(mapper.pendingStats())
                .thenReturn(
                        new MinisoWriteOffEventMapper.PendingStats(
                                3, 1, LocalDateTime.of(2026, 10, 17, 9, 29)));
        when(mapper.pendingNotifications()).thenReturn(2L);
        var beans = new StaticListableBeanFactory();
        beans.addBean("events", mapper);
        var registry = new SimpleMeterRegistry();
        try {
            var metrics =
                    new MinisoWriteOffMetrics(
                            registry,
                            beans.getBeanProvider(MinisoWriteOffEventMapper.class),
                            Clock.fixed(Instant.parse("2026-10-17T01:30:00Z"), ZoneOffset.UTC));
            metrics.event("accepted");
            metrics.batch(new MinisoWriteOffBatchResult(1, 1, 0, 0, 0, 0));
            clearInvocations(mapper);
            assertThat(registry.get("trade.miniso.writeoff.pending").gauge().value()).isEqualTo(3);
            assertThat(registry.get("trade.miniso.writeoff.oldest.age.seconds").gauge().value())
                    .isEqualTo(60);
            assertThat(registry.get("trade.miniso.writeoff.notifications.pending").gauge().value())
                    .isEqualTo(2);
            assertThat(
                            registry.get("trade.miniso.writeoff.received")
                                    .tag("outcome", "accepted")
                                    .counter()
                                    .count())
                    .isEqualTo(1);
            verifyNoInteractions(mapper);
        } finally {
            registry.close();
        }
    }
}
