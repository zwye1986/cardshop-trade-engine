package com.cardshop.trade.engine.bootstrap.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.PendingPersistOrderPort;
import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargePersistMqRepublishService;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

/**
 * XXL-Job 真实调度入口装配测试。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」和
 * {@code docs/开发进度.md} 第 7 节第 2 项。真实 handler 必须复用现有
 * {@link com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob#runOnce(int, int, int, String)}
 * 门面，不能另写一套补投逻辑；同时默认关闭 executor，避免 dev/test 未配置调度中心时影响应用启动。
 */
class TradeEngineXxlJobConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestBeans.class, TradeEngineReliabilityConfiguration.class)
            .withUserConfiguration(xxlJobConfigurationClass());

    @Test
    void doesNotStartXxlExecutorWhenSwitchIsDisabled() throws Exception {
        Class<?> executorType = Class.forName("com.xxl.job.core.executor.impl.XxlJobSpringExecutor");
        Class<?> handlerType = Class.forName(
                "com.cardshop.trade.engine.bootstrap.config.TradeEnginePersistMqRepublishXxlJobHandler");

        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(handlerType);
            assertThat(context).doesNotHaveBean(executorType);
        });
    }

    @Test
    void createsXxlExecutorFromBoundPropertiesWhenSwitchIsEnabled() {
        TradeEngineProperties properties = new TradeEngineProperties();
        properties.getXxlJob().setEnabled(true);
        properties.getXxlJob().setAdminAddresses("http://127.0.0.1:8080/xxl-job-admin");
        properties.getXxlJob().setAccessToken("test-token");
        properties.getXxlJob().getExecutor().setAppname("cardshop-trade-engine-dev");
        properties.getXxlJob().getExecutor().setIp("127.0.0.1");
        properties.getXxlJob().getExecutor().setPort(19999);
        properties.getXxlJob().getExecutor().setLogpath("/tmp/cardshop-trade-engine/xxl-job");

        Object executor = new TradeEngineXxlJobConfiguration().xxlJobSpringExecutor(properties);

        assertThat(executor).isInstanceOf(com.xxl.job.core.executor.impl.XxlJobSpringExecutor.class);
    }

    @Test
    void handlerParsesXxlParametersAndDelegatesToExistingJobFacade() {
        RecordingPersistMqRepublishService service = new RecordingPersistMqRepublishService();
        TradeEnginePersistMqRepublishXxlJobHandler handler = new TradeEnginePersistMqRepublishXxlJobHandler(
                new RechargePersistMqRepublishJob(service));

        RechargeCompensationBatchResult result = handler.runFromXxlContext(
                4,
                2,
                99L,
                "limit=25,batchId=batch-1,workerId=xxl-worker-1");

        assertThat(result.scanned()).isEqualTo(25);
        assertThat(service.calls).containsExactly("4:2:25:xxl-worker-1");
    }

    private static Class<?> xxlJobConfigurationClass() {
        try {
            return Class.forName("com.cardshop.trade.engine.bootstrap.config.TradeEngineXxlJobConfiguration");
        } catch (ClassNotFoundException ex) {
            throw new AssertionError("TradeEngineXxlJobConfiguration must exist for real XXL-Job handler wiring", ex);
        }
    }

    @EnableConfigurationProperties(TradeEngineProperties.class)
    static class TestBeans {

        @Bean
        PersistRechargeOrderMessagePublisher persistRechargeOrderMessagePublisher() {
            return new PersistRechargeOrderMessagePublisher() {
                @Override
                public boolean isAvailable() {
                    return true;
                }

                @Override
                public java.util.concurrent.CompletionStage<PersistMessagePublishResult> publish(
                        PersistTradeOrderMessage message) {
                    return CompletableFuture.completedFuture(PersistMessagePublishResult.confirmed(message.messageId(), 1));
                }
            };
        }

        @Bean
        PendingPersistOrderPort pendingPersistOrderPort() {
            return PendingPersistOrderPort.noop();
        }

        @Bean
        RechargePersistMqRepublishService rechargePersistMqRepublishService(
                PendingPersistOrderPort pendingPersistOrderPort,
                PersistRechargeOrderMessagePublisher publisher) {
            return new RechargePersistMqRepublishService(pendingPersistOrderPort, publisher);
        }
    }

    private static final class RecordingPersistMqRepublishService extends RechargePersistMqRepublishService {

        private final List<String> calls = new ArrayList<>();

        private RecordingPersistMqRepublishService() {
            super(PendingPersistOrderPort.noop(), new TestBeans().persistRechargeOrderMessagePublisher());
        }

        @Override
        public RechargeCompensationBatchResult republishDue(int shardTotal, int shardIndex, int limit, String workerId) {
            calls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return new RechargeCompensationBatchResult(limit, 0, 0, 0);
        }
    }
}
