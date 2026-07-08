package com.cardshop.trade.engine.bootstrap.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.CancelUnconfirmedAcceptResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistQueuedResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistStateResult;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.application.recharge.ReplicationWaitResult;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

/**
 * 异步落库 Consumer 核心服务后置自动配置测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。真实运行时
 * {@link RechargeOrderPersistencePort} 可能来自 MyBatis 后置自动配置，普通 bootstrap configuration 过早按该端口做
 * {@code @ConditionalOnBean} 会跳过 {@link PersistRechargeOrderConsumerService}，进而导致 Rabbit listener 无法创建。
 * 这里固化 Consumer 必须能在依赖端口后置出现时被兜底装配。
 */
class TradeEngineConsumerAutoConfigurationTest {

    @Test
    void registersConsumerWhenPersistencePortComesFromLaterAutoConfiguration() {
        new ApplicationContextRunner()
                .withBean(Clock.class, Clock::systemUTC)
                .withConfiguration(AutoConfigurations.of(
                        TestConsumerDependencyAutoConfiguration.class,
                        TradeEngineConsumerAutoConfiguration.class))
                .run(context -> assertThat(context).hasSingleBean(PersistRechargeOrderConsumerService.class));
    }

    @AutoConfiguration(before = TradeEngineConsumerAutoConfiguration.class)
    static class TestConsumerDependencyAutoConfiguration {

        @Bean
        RechargeOrderPersistencePort rechargeOrderPersistencePort() {
            return aggregate -> {
            };
        }

        @Bean
        RechargeAcceptReliabilityPort rechargeAcceptReliabilityPort() {
            return new StubReliabilityPort();
        }
    }

    private static final class StubReliabilityPort implements RechargeAcceptReliabilityPort {

        @Override
        public CompletionStage<ReplicationWaitResult> waitForReplication(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult) {
            throw new AssertionError("configuration test should not wait for redis replication");
        }

        @Override
        public CompletionStage<CancelUnconfirmedAcceptResult> cancelUnconfirmedAccept(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String reason) {
            throw new AssertionError("configuration test should not cancel redis accept");
        }

        @Override
        public CompletionStage<MarkPersistQueuedResult> markPersistQueued(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String persistMessageId,
                int publishCount) {
            throw new AssertionError("configuration test should not mark redis persist queued");
        }

        @Override
        public CompletionStage<MarkPersistStateResult> markDbPersisted(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                long persistedVersion,
                long nowEpochMillis) {
            return CompletableFuture.completedFuture(MarkPersistStateResult.success());
        }

        @Override
        public CompletionStage<MarkPersistStateResult> markPersistFailed(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String failCode,
                String failReason,
                int retryCount,
                long nextRetryEpochMillis,
                long nowEpochMillis) {
            return CompletableFuture.completedFuture(MarkPersistStateResult.success());
        }
    }
}
