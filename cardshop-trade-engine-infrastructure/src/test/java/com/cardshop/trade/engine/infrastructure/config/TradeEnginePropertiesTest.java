package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/**
 * `trade.engine.*` 配置绑定测试。
 *
 * <p>配置字段对应 {@code docs/交行直充业务系统详细设计.md}「11. 配置中心、部署与健康检查」和
 * 「11. 配置中心、部署与健康检查」。新增配置项时应同步补充默认值和环境覆盖测试，防止 Nacos 模板、
 * 本地 YAML 与 Java 绑定对象语义漂移。
 */
class TradeEnginePropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(PropertiesConfiguration.class);

    @Test
    void bindsDesignDefaults() {
        contextRunner.run(context -> {
            var properties = context.getBean(TradeEngineProperties.class);

            // 默认值遵循生产 fail-closed：Redis 写入必须由 profile、Nacos 或部署密钥显式开启。
            assertThat(properties.getRedis().isWriteEnabled()).isFalse();
            assertThat(properties.getRedis().getWaitReplicas()).isEqualTo(1);
            assertThat(properties.getRedis().getPendingOrgBucketCount()).isEqualTo(128);
            assertThat(properties.getRedis().getKeyPrefix()).isEqualTo("cardshop:trade");
            assertThat(properties.getMq().getPersistQueue()).isEqualTo("recharge.persist.mysql.queue");
            assertThat(properties.getMq().getPersistDeadLetterExchange()).isEqualTo("recharge.persist.dlx");
            assertThat(properties.getMq().getPersistDeadLetterQueue()).isEqualTo("recharge.persist.mysql.dlq");
            assertThat(properties.getMq().getPersistDeadLetterRoutingKey()).isEqualTo("recharge.persist.mysql.dlq");
            assertThat(properties.getMq().getPersistRetryExchange()).isEqualTo("recharge.persist.retry.exchange");
            assertThat(properties.getMq().getPersistRetryQueue()).isEqualTo("recharge.persist.mysql.retry.queue");
            assertThat(properties.getMq().getPersistRetryRoutingKey()).isEqualTo("recharge.persist.mysql.retry");
            assertThat(properties.getMq().getConsumerRetryDelayMs()).isEqualTo(30000);
            assertThat(properties.getMq().getPublishConfirmTimeoutMs()).isEqualTo(500);
            assertThat(properties.getMq().isPersistBatchEnabled()).isFalse();
            assertThat(properties.getMq().getPersistBatchSize()).isEqualTo(10);
            assertThat(properties.getMq().getPersistBatchReceiveTimeoutMs()).isEqualTo(200);
            assertThat(properties.getMq().getBreaker().isEnabled()).isTrue();
            assertThat(properties.getPersist().isOutboxAsyncEnabled()).isFalse();
            assertThat(properties.getPersist().getOutboxWriterHikariMax()).isEqualTo(8);
            assertThat(properties.getPersist().isOutboxWriterBatchEnabled()).isTrue();
            assertThat(properties.getOrder().getPendingTtlDays()).isEqualTo(7);
            assertThat(properties.getOrder().getTerminalTtlHours()).isEqualTo(72);
            assertThat(properties.getRouting().getSupportedPaths())
                    .contains("/v1/cardapi/order/createRechargeOrder");
            assertThat(properties.getV1().getNonceReplayTtlSeconds()).isEqualTo(600);
            assertThat(properties.getV1().getTokenClockSkewSeconds()).isEqualTo(30);
            assertThat(properties.getV1().getProductCache().getRefreshAfter()).isEqualTo(Duration.ofMinutes(25));
            assertThat(properties.getV1().getProductCache().getMaxStaleAge()).isEqualTo(Duration.ofMinutes(30));
            assertThat(properties.getV1().getProductCache().getPhysicalTtl()).isEqualTo(Duration.ofMinutes(60));
            assertThat(properties.getV1().getProductCache().getRefreshLockTtl()).isEqualTo(Duration.ofSeconds(5));
            assertThat(properties.getV1().getProductCache().getRefreshWaitTimeout()).isEqualTo(Duration.ofSeconds(1));
            assertThat(properties.getV1().getProductCache().getNegativeCacheTtl()).isEqualTo(Duration.ofMinutes(1));
            assertThat(properties.getV1().getMerchantProfileCache().isEnabled()).isTrue();
            assertThat(properties.getV1().getMerchantProfileCache().getTtl()).isEqualTo(Duration.ofSeconds(60));
            assertThat(properties.getV1().getMerchantProfileCache().getNegativeTtl()).isEqualTo(Duration.ofSeconds(10));
            assertThat(properties.getV1().getMerchantProfileCache().getMaximumSize()).isEqualTo(1024);
            assertThat(properties.getV1().getMerchantConfigPreheat().isEnabled()).isFalse();
            assertThat(properties.getV1().getMerchantConfigPreheat().getOrgCodes()).isEmpty();
            assertThat(properties.getV1().getMerchantConfigPreheat().isFailOnError()).isTrue();
            assertThat(properties.getBalance().getOwnerMode()).isEqualTo("NEW");
            // 余额 Delta 刷账 outbox 默认值；参见详细设计「9.2 余额 Delta 刷账 Outbox」「11. 配置中心」。
            assertThat(properties.getOutbox().getBalanceDeltaFlushBatchSize()).isEqualTo(500);
            assertThat(properties.getOutbox().getBalanceDeltaClaimLeaseSeconds()).isEqualTo(120);
            assertThat(properties.getOutbox().getBalanceDeltaRetryBackoffSeconds()).isEqualTo("5,30,120,600");
            assertThat(properties.getOutbox().balanceDeltaRetryBackoffDurations())
                    .containsExactly(
                            Duration.ofSeconds(5),
                            Duration.ofSeconds(30),
                            Duration.ofSeconds(120),
                            Duration.ofSeconds(600));
            assertThat(properties.getFulfillment().getSupplierQueryRetryDelaySeconds()).isEqualTo("2,4,6,8,16,32");
            assertThat(properties.getFulfillment().getOutboxRetryBackoffSeconds()).isEqualTo(10);
            assertThat(properties.getFulfillment().getDispatchInitialDelaySeconds()).isEqualTo(1);
            assertThat(properties.getFulfillment().dispatchInitialDelay()).isEqualTo(Duration.ofSeconds(1));
            assertThat(properties.getFulfillment().isTimerEnabled()).isFalse();
            assertThat(properties.getFulfillment().getTimerMaxPendingTasks()).isEqualTo(200000);
            assertThat(properties.getFulfillment().supplierQueryRetryDelays())
                    .containsExactly(
                            Duration.ofSeconds(2),
                            Duration.ofSeconds(4),
                            Duration.ofSeconds(6),
                            Duration.ofSeconds(8),
                            Duration.ofSeconds(16),
                            Duration.ofSeconds(32));
            assertThat(properties.getThreadPool().getDbCoreSize()).isEqualTo(16);
            assertThat(properties.getThreadPool().getDbMaxSize()).isEqualTo(32);
            assertThat(properties.getIntegration().getManualJobTimeoutMs()).isEqualTo(30000);
        });
    }

    @Test
    void bindsEnvironmentOverrides() {
        contextRunner
                .withPropertyValues(
                        "trade.engine.redis.write-enabled=true",
                        "trade.engine.redis.wait-replicas=2",
                        "trade.engine.mq.persist-queue=recharge.persist.mysql.gray.queue",
                        "trade.engine.mq.persist-dead-letter-exchange=recharge.persist.gray.dlx",
                        "trade.engine.mq.persist-dead-letter-queue=recharge.persist.mysql.gray.dlq",
                        "trade.engine.mq.persist-dead-letter-routing-key=recharge.persist.mysql.gray.dlq",
                        "trade.engine.mq.persist-retry-exchange=recharge.persist.gray.retry.exchange",
                        "trade.engine.mq.persist-retry-queue=recharge.persist.mysql.gray.retry.queue",
                        "trade.engine.mq.persist-retry-routing-key=recharge.persist.mysql.gray.retry",
                        "trade.engine.mq.consumer-retry-delay-ms=1500",
                        "trade.engine.mq.persist-batch-enabled=true",
                        "trade.engine.mq.persist-batch-size=5",
                        "trade.engine.mq.persist-batch-receive-timeout-ms=250",
                        "trade.engine.mq.persist-batch-poll-timeout-ms=120",
                        "trade.engine.mq.breaker.failure-threshold=9",
                        "trade.engine.persist.outbox-async-enabled=true",
                        "trade.engine.persist.outbox-writer-hikari-max=3",
                        "trade.engine.persist.outbox-writer-batch-enabled=false",
                        "trade.engine.routing.gray-org-codes[0]=A11",
                        "trade.engine.routing.supported-paths[0]=/v1/cardapi/order/getRechargeOrder",
                        "trade.engine.v1.crypto.key-rotation-previous-enabled=false",
                        "trade.engine.v1.product-cache.refresh-after=20m",
                        "trade.engine.v1.product-cache.max-stale-age=29m",
                        "trade.engine.v1.merchant-profile-cache.enabled=false",
                        "trade.engine.v1.merchant-profile-cache.ttl=30s",
                        "trade.engine.v1.merchant-profile-cache.negative-ttl=5s",
                        "trade.engine.v1.merchant-profile-cache.maximum-size=128",
                        "trade.engine.v1.merchant-config-preheat.enabled=true",
                        "trade.engine.v1.merchant-config-preheat.org-codes[0]=A39",
                        "trade.engine.v1.merchant-config-preheat.org-codes[1]=B01",
                        "trade.engine.v1.merchant-config-preheat.fail-on-error=false",
                        "trade.engine.supplier.channels.fake.query-url=http://127.0.0.1:18082/supplier/query",
                        "trade.engine.fulfillment.timer-enabled=true",
                        "trade.engine.fulfillment.dispatch-initial-delay-seconds=5",
                        "trade.engine.fulfillment.supplier-query-retry-delay-seconds=3,9",
                        "trade.engine.fulfillment.timer-max-pending-tasks=1234",
                        "trade.engine.outbox.balance-delta-flush-batch-size=200",
                        "trade.engine.outbox.balance-delta-claim-lease-seconds=90",
                        "trade.engine.outbox.balance-delta-retry-backoff-seconds=10,60,300",
                        "trade.engine.thread-pool.db-core-size=4",
                        "trade.engine.thread-pool.db-max-size=8",
                        "trade.engine.integration.manual-job-timeout-ms=12345")
                .run(context -> {
                    var properties = context.getBean(TradeEngineProperties.class);

                    // 环境覆盖验证用于保证 Nacos 灰度配置能被 Spring Boot 正确绑定。
                    assertThat(properties.getRedis().isWriteEnabled()).isTrue();
                    assertThat(properties.getRedis().getWaitReplicas()).isEqualTo(2);
                    assertThat(properties.getMq().getPersistQueue()).isEqualTo("recharge.persist.mysql.gray.queue");
                    assertThat(properties.getMq().getPersistDeadLetterExchange()).isEqualTo("recharge.persist.gray.dlx");
                    assertThat(properties.getMq().getPersistDeadLetterQueue()).isEqualTo("recharge.persist.mysql.gray.dlq");
                    assertThat(properties.getMq().getPersistDeadLetterRoutingKey()).isEqualTo("recharge.persist.mysql.gray.dlq");
                    assertThat(properties.getMq().getPersistRetryExchange()).isEqualTo("recharge.persist.gray.retry.exchange");
                    assertThat(properties.getMq().getPersistRetryQueue()).isEqualTo("recharge.persist.mysql.gray.retry.queue");
                    assertThat(properties.getMq().getPersistRetryRoutingKey()).isEqualTo("recharge.persist.mysql.gray.retry");
                    assertThat(properties.getMq().getConsumerRetryDelayMs()).isEqualTo(1500);
                    assertThat(properties.getMq().isPersistBatchEnabled()).isTrue();
                    assertThat(properties.getMq().getPersistBatchSize()).isEqualTo(5);
                    assertThat(properties.getMq().getPersistBatchReceiveTimeoutMs()).isEqualTo(250);
                    assertThat(properties.getMq().getPersistBatchPollTimeoutMs()).isEqualTo(120);
                    assertThat(properties.getMq().getBreaker().getFailureThreshold()).isEqualTo(9);
                    assertThat(properties.getPersist().isOutboxAsyncEnabled()).isTrue();
                    assertThat(properties.getPersist().getOutboxWriterHikariMax()).isEqualTo(3);
                    assertThat(properties.getPersist().isOutboxWriterBatchEnabled()).isFalse();
                    assertThat(properties.getRouting().getGrayOrgCodes()).containsExactly("A11");
                    assertThat(properties.getRouting().getSupportedPaths())
                            .containsExactly("/v1/cardapi/order/getRechargeOrder");
                    assertThat(properties.getV1().getCrypto().isKeyRotationPreviousEnabled()).isFalse();
                    assertThat(properties.getV1().getProductCache().getRefreshAfter()).isEqualTo(Duration.ofMinutes(20));
                    assertThat(properties.getV1().getProductCache().getMaxStaleAge()).isEqualTo(Duration.ofMinutes(29));
                    assertThat(properties.getV1().getMerchantProfileCache().isEnabled()).isFalse();
                    assertThat(properties.getV1().getMerchantProfileCache().getTtl()).isEqualTo(Duration.ofSeconds(30));
                    assertThat(properties.getV1().getMerchantProfileCache().getNegativeTtl()).isEqualTo(Duration.ofSeconds(5));
                    assertThat(properties.getV1().getMerchantProfileCache().getMaximumSize()).isEqualTo(128);
                    assertThat(properties.getV1().getMerchantConfigPreheat().isEnabled()).isTrue();
                    assertThat(properties.getV1().getMerchantConfigPreheat().getOrgCodes())
                            .containsExactly("A39", "B01");
                    assertThat(properties.getV1().getMerchantConfigPreheat().isFailOnError()).isFalse();
                    assertThat(properties.getSupplier().getChannels().get("fake").getQueryUrl())
                            .isEqualTo("http://127.0.0.1:18082/supplier/query");
                    assertThat(properties.getOutbox().getBalanceDeltaFlushBatchSize()).isEqualTo(200);
                    assertThat(properties.getOutbox().getBalanceDeltaClaimLeaseSeconds()).isEqualTo(90);
                    assertThat(properties.getOutbox().getBalanceDeltaRetryBackoffSeconds()).isEqualTo("10,60,300");
                    assertThat(properties.getOutbox().balanceDeltaRetryBackoffDurations())
                            .containsExactly(
                                    Duration.ofSeconds(10),
                                    Duration.ofSeconds(60),
                                    Duration.ofSeconds(300));
                    assertThat(properties.getFulfillment().getSupplierQueryRetryDelaySeconds()).isEqualTo("3,9");
                    assertThat(properties.getFulfillment().getDispatchInitialDelaySeconds()).isEqualTo(5);
                    assertThat(properties.getFulfillment().dispatchInitialDelay()).isEqualTo(Duration.ofSeconds(5));
                    assertThat(properties.getFulfillment().isTimerEnabled()).isTrue();
                    assertThat(properties.getFulfillment().supplierQueryRetryDelays())
                            .containsExactly(Duration.ofSeconds(3), Duration.ofSeconds(9));
                    assertThat(properties.getFulfillment().getTimerMaxPendingTasks()).isEqualTo(1234);
                    assertThat(properties.getThreadPool().getDbCoreSize()).isEqualTo(4);
                    assertThat(properties.getThreadPool().getDbMaxSize()).isEqualTo(8);
                    assertThat(properties.getIntegration().getManualJobTimeoutMs()).isEqualTo(12345);
                });
    }

    @Test
    void bindsV1LoadTestToken() {
        contextRunner
                .withPropertyValues("trade.engine.v1.load-test-token=load-secret")
                .run(context -> {
                    var properties = context.getBean(TradeEngineProperties.class);
                    assertThat(properties.getV1().getLoadTestToken()).isEqualTo("load-secret");
                });
    }

    @Test
    void outboxBackoffUsesDesignDefaultWhenBlank() {
        contextRunner
                .withPropertyValues("trade.engine.outbox.balance-delta-retry-backoff-seconds=  ,  ,")
                .run(context -> {
                    var properties = context.getBean(TradeEngineProperties.class);
                    assertThat(properties.getOutbox().balanceDeltaRetryBackoffDurations())
                            .containsExactly(
                                    Duration.ofSeconds(5),
                                    Duration.ofSeconds(30),
                                    Duration.ofSeconds(120),
                                    Duration.ofSeconds(600));
                });
    }

    @Test
    void outboxBackoffRejectsInvalidConfigWithoutSilentFallback() {
        contextRunner
                .withPropertyValues("trade.engine.outbox.balance-delta-retry-backoff-seconds=5,abc,30")
                .run(context -> {
                    var properties = context.getBean(TradeEngineProperties.class);
                    assertThatThrownBy(() -> properties.getOutbox().balanceDeltaRetryBackoffDurations())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("balance-delta-retry-backoff-seconds")
                            .hasMessageContaining("abc");
                });
    }

    @Test
    void fulfillmentSupplierQueryBackoffRejectsInvalidConfigWithoutSilentFallback() {
        contextRunner
                .withPropertyValues("trade.engine.fulfillment.supplier-query-retry-delay-seconds=2,0,abc")
                .run(context -> {
                    var properties = context.getBean(TradeEngineProperties.class);
                    assertThatThrownBy(() -> properties.getFulfillment().supplierQueryRetryDelays())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("supplier-query-retry-delay-seconds")
                            .hasMessageContaining("abc");
                });
    }

    @Test
    void fulfillmentDispatchInitialDelayRejectsNonPositiveConfigWithoutSilentFallback() {
        contextRunner
                .withPropertyValues("trade.engine.fulfillment.dispatch-initial-delay-seconds=0")
                .run(context -> {
                    var properties = context.getBean(TradeEngineProperties.class);
                    assertThatThrownBy(() -> properties.getFulfillment().dispatchInitialDelay())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("dispatch-initial-delay-seconds");
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(TradeEngineProperties.class)
    static class PropertiesConfiguration {
    }
}
