package com.cardshop.trade.engine.bootstrap.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.CancelUnconfirmedAcceptResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistQueuedResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistStateResult;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderConsumerService;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.ReplicationWaitResult;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderMessageListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

/**
 * RabbitMQ listener 后置自动配置测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「7. RabbitMQ 持久化消息层」和
 * 「8.3 异步落库 Consumer」。真实 dev/test/prod 中 {@link RabbitTemplate} 来自 Spring Boot
 * AMQP 自动配置，业务侧 {@code @Configuration} 过早使用 {@code @ConditionalOnBean(RabbitTemplate.class)}
 * 会在 Rabbit 自动配置注册前被判定为不满足，最终表现为应用健康但 RabbitMQ 管理端没有 connection/consumer。
 * 这里用后置自动配置模拟该顺序，防止 listener 再次被静默跳过。
 */
class TradeEngineRabbitListenerAutoConfigurationTest {

    @Test
    void registersListenerWhenRabbitTemplateComesFromLaterAutoConfiguration() {
        new ApplicationContextRunner()
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withBean(PersistRechargeOrderConsumerService.class,
                        TradeEngineRabbitListenerAutoConfigurationTest::consumerService)
                .withConfiguration(AutoConfigurations.of(
                        TestRabbitTemplateAutoConfiguration.class,
                        TradeEngineRabbitListenerAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(RabbitTemplate.class);
                    assertThat(context).hasSingleBean(PersistRechargeOrderConsumerService.class);
                    assertThat(context).hasSingleBean(RabbitPersistRechargeOrderMessageListener.class);
                });
    }

    @AutoConfiguration(before = TradeEngineRabbitListenerAutoConfiguration.class)
    static class TestRabbitTemplateAutoConfiguration {

        @Bean
        RabbitTemplate rabbitTemplate() {
            return new TestRabbitTemplate();
        }
    }

    private static PersistRechargeOrderConsumerService consumerService() {
        return new PersistRechargeOrderConsumerService(
                com.cardshop.trade.engine.application.recharge.PersistOrderSnapshotPort.noop(),
                aggregate -> {
                },
                new StubReliabilityPort(),
                Clock.systemUTC());
    }

    private static final class TestRabbitTemplate extends RabbitTemplate {

        @Override
        public void afterPropertiesSet() {
            // 装配测试只需要 RabbitTemplate bean 存在，不需要真实 RabbitMQ ConnectionFactory。
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
