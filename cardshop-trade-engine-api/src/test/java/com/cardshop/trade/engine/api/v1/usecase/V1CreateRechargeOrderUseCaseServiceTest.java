package com.cardshop.trade.engine.api.v1.usecase;

import com.cardshop.trade.engine.api.v1.codec.V1RequestBodyCodec;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckService;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolSettings;
import com.cardshop.trade.engine.api.v1.security.V1Sm3SignatureVerifier;
import com.cardshop.trade.engine.api.v1.security.V1TimestampSettings;
import com.cardshop.trade.engine.api.v1.security.V1TimestampValidator;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecord;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecordPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import com.cardshop.trade.engine.application.recharge.CancelUnconfirmedAcceptResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistQueuedResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistStateResult;
import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.ReplicationWaitResult;
import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.application.v1.V1MerchantCommand;
import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.application.v1.V1ProductCandidateSelector;
import com.cardshop.trade.engine.application.v1.V1ProductConfigPreheatPort;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.domain.service.RequestHashCanonicalizer;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V1 创建直充订单新链路用例测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」、
 * 「5. Domain 状态与业务规则」和「6. Redis 受理与缓存层」。`product_id` 是请求父产品号，
 * 验签通过后从商户授权候选中选定子商品并补热缺失配置，再进入 Redis 原子接单。
 */
class V1CreateRechargeOrderUseCaseServiceTest {

    private static final Instant NOW = Instant.parse("2026-07-05T04:15:58Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final Executor SYNC_EXECUTOR = Runnable::run;

    private final CapturingAcceptPort acceptPort = new CapturingAcceptPort();
    private final RecordingProductConfigPreheatPort productConfigPreheatPort = new RecordingProductConfigPreheatPort();
    private final V1CreateRechargeOrderUseCaseService service = new V1CreateRechargeOrderUseCaseService(
            precheckService(),
            new RequestHashCanonicalizer(),
            (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "AES-KEY", true, 7L)),
            (orgCode, productId, account) -> V1ProductSelection.passed(
                    productId,
                    "73301289",
                    "TF_KA",
                    "话费50元",
                    "codekey",
                    1,
                    5200L,
                    5000L,
                    4900L,
                    11L),
            acceptPort,
            productConfigPreheatPort,
            () -> "202607051337458961474292",
            CLOCK,
            new V1RechargeAcceptSettings(Duration.ofMinutes(5)),
            SYNC_EXECUTOR);

    @Test
    void acceptsValidCreateRequestAndBuildsRedisCommandFromLegacyV1Fields() {
        acceptPort.nextResult = new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.ACCEPTED,
                "202607051337458961474292",
                "PASSED",
                "",
                "",
                true,
                true,
                1L);

        V1CardApiResult result = service.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-1001", "nonce-12345678901234567890123456", 1)))
                .toCompletableFuture().join();

        assertThat(result.code()).isEqualTo("200");
        assertThat(result.msg()).isEqualTo("操作成功");
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) result.data();
        assertThat(data)
                .containsEntry("order_sn", "202607051337458961474292")
                .containsEntry("out_order_sn", "OUT-1001");
        assertThat(acceptPort.lastCommand).isNotNull();
        assertThat(acceptPort.lastCommand.orgCode()).isEqualTo("A36");
        assertThat(acceptPort.lastCommand.productId()).isEqualTo("73301280");
        assertThat(acceptPort.lastCommand.selectedProductId()).isEqualTo("73301289");
        assertThat(acceptPort.lastCommand.orderAmountCent()).isEqualTo(5200L);
        assertThat(acceptPort.lastCommand.candidateValidationStatus()).isEqualTo(ValidationStatus.PASSED);
        assertThat(acceptPort.lastCommand.nextRetryEpochMillis()).isEqualTo(NOW.plus(Duration.ofMinutes(5)).toEpochMilli());
        assertThat(productConfigPreheatPort.orgCode).isEqualTo("A36");
        assertThat(productConfigPreheatPort.selection).isNotNull();
        assertThat(productConfigPreheatPort.selection.productId()).isEqualTo("73301280");
        assertThat(productConfigPreheatPort.selection.selectedProductId()).isEqualTo("73301289");
    }

    @Test
    void rejectsBadSignatureBeforeCallingRedis() {
        String rawBody = validRawBody("OUT-1002", "nonce-12345678901234567890123457", 1)
                .replaceFirst("\"sign\"\\s*:\\s*\"[0-9a-f]+\"", "\"sign\": \"bad-sign\"");

        V1CardApiResult result = service.createRechargeOrder(new V1MerchantCommand("app-gray", "A36", rawBody))
                .toCompletableFuture().join();

        assertThat(result.code()).isEqualTo("501");
        assertThat(result.msg()).isEqualTo("验签不合规！");
        assertThat(acceptPort.lastCommand).isNull();
    }

    @Test
    void rejectsMalformedNotifyUrlWithoutErrorLogNoise() {
        Logger logger = (Logger) LoggerFactory.getLogger(V1CreateRechargeOrderUseCaseService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            V1CardApiResult result = service.createRechargeOrder(
                            new V1MerchantCommand(
                                    "app-gray",
                                    "A36",
                                    rawBody("OUT-BAD-NOTIFY", "nonce-12345678901234567890123447", 1, "http://[")))
                    .toCompletableFuture()
                    .join();

            assertThat(result.code()).isEqualTo("506");
            assertThat(result.msg()).isEqualTo("notify_url格式不正确");
            assertThat(acceptPort.lastCommand).isNull();
            assertThat(appender.list)
                    .extracting(ILoggingEvent::getLevel)
                    .doesNotContain(Level.ERROR);
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void mapsDuplicateRedisResultToLegacyOrderWarnResponse() {
        acceptPort.nextResult = new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.DUPLICATE,
                "202607051337458961474292",
                "",
                "DUPLICATE_OUT_ORDER_SN",
                "订单号重复",
                false,
                false,
                0L);

        V1CardApiResult result = service.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-1003", "nonce-12345678901234567890123458", 1)))
                .toCompletableFuture().join();

        assertThat(result.code()).isEqualTo("505");
        assertThat(result.msg()).isEqualTo("订单号重复");
    }

    @Test
    void sendsProductCandidateFailureToRedisSoLuaCanWriteFailedOrder() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        localAcceptPort.nextResult = new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.ACCEPTED_FAILED,
                "202607051337458961474292",
                "FAILED",
                "PRODUCT_NO_PERMISSION",
                "商品无权限",
                false,
                false,
                1L);
        V1CreateRechargeOrderUseCaseService localService = new V1CreateRechargeOrderUseCaseService(
                precheckService(),
                new RequestHashCanonicalizer(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "AES-KEY", true, 7L)),
                (orgCode, productId, account) -> V1ProductSelection.failed(
                        productId,
                        "PRODUCT_NO_PERMISSION",
                        "商品无权限",
                        11L),
                localAcceptPort,
                () -> "202607051337458961474292",
                CLOCK,
                new V1RechargeAcceptSettings(Duration.ofMinutes(5)),
                SYNC_EXECUTOR);

        V1CardApiResult result = localService.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-1004", "nonce-12345678901234567890123459", 1)))
                .toCompletableFuture().join();

        assertThat(localAcceptPort.lastCommand).isNotNull();
        assertThat(localAcceptPort.lastCommand.candidateValidationStatus()).isEqualTo(ValidationStatus.FAILED);
        assertThat(localAcceptPort.lastCommand.candidateFailCode()).isEqualTo("PRODUCT_NO_PERMISSION");
        assertThat(result.code()).isEqualTo("503");
        assertThat(result.msg()).isEqualTo("商品无权限");
    }

    @Test
    void preheatsResolvedChildSnapshotBeforeLuaEvenWhenCandidateValidationFails() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        RecordingProductConfigPreheatPort localPreheatPort = new RecordingProductConfigPreheatPort();
        localAcceptPort.nextResult = new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.ACCEPTED_FAILED,
                "202607051337458961474292",
                "FAILED",
                "NOT_RECHARGE_PRODUCT",
                "该商品不是直充产品",
                false,
                false,
                1L);
        V1CreateRechargeOrderUseCaseService localService = new V1CreateRechargeOrderUseCaseService(
                precheckService(),
                new RequestHashCanonicalizer(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "AES-KEY", true, 7L)),
                (orgCode, productId, account) -> new V1ProductSelection(
                        productId,
                        "80100023",
                        "MINISO",
                        "名创直充100",
                        "codekey",
                        0,
                        1L,
                        1L,
                        1L,
                        1L,
                        true,
                        ValidationStatus.FAILED,
                        "NOT_RECHARGE_PRODUCT",
                        "该商品不是直充产品"),
                localAcceptPort,
                localPreheatPort,
                () -> "202607051337458961474292",
                CLOCK,
                new V1RechargeAcceptSettings(Duration.ofMinutes(5)),
                SYNC_EXECUTOR);

        localService.createRechargeOrder(
                        new V1MerchantCommand("app-gray", "A36", validRawBody(
                                "OUT-RESOLVED-FAIL",
                                "nonce-12345678901234567890123460",
                                1)))
                .toCompletableFuture()
                .join();

        assertThat(localPreheatPort.selection).isNotNull();
        assertThat(localPreheatPort.selection.productSnapshotAvailable()).isTrue();
        assertThat(localPreheatPort.selection.selectedProductId()).isEqualTo("80100023");
    }

    @Test
    void mapsAcceptedFailedWithoutFailReasonToBusinessFailureFallbackMessage() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        localAcceptPort.nextResult = new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.ACCEPTED_FAILED,
                "202607051337458961474292",
                "FAILED",
                "PRODUCT_NO_PERMISSION",
                "",
                false,
                false,
                1L);
        V1CreateRechargeOrderUseCaseService localService = service(localAcceptPort, new RecordingReliabilityPort(), new RecordingPersistPublisher());

        V1CardApiResult result = localService.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-EMPTY-FAIL", "nonce-12345678901234567890123449", 1)))
                .toCompletableFuture().join();

        assertThat(result.code()).isEqualTo("503");
        assertThat(result.msg()).isEqualTo("订单失败");
    }

    @Test
    void executesPrepareOnDbTaskExecutorThread() throws Exception {
        var executorThread = new AtomicReference<String>();
        var realExecutor = Executors.newSingleThreadExecutor();
        Executor recordingExecutor = command -> realExecutor.execute(() -> {
            executorThread.set(Thread.currentThread().getName());
            command.run();
        });
        var localAcceptPort = new CapturingAcceptPort();
        var localService = new V1CreateRechargeOrderUseCaseService(
                precheckService(),
                new RequestHashCanonicalizer(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "AES-KEY", true, 7L)),
                (orgCode, productId, account) -> V1ProductSelection.passed(
                        productId, productId, "TF_KA", "话费50元", "codekey", 1, 5200L, 5000L, 4900L, 11L),
                localAcceptPort,
                () -> "202607051337458961474292",
                CLOCK,
                new V1RechargeAcceptSettings(Duration.ofMinutes(5)),
                recordingExecutor);

        try {
            localService.createRechargeOrder(
                    new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-ASYNC", "nonce-12345678901234567890123450", 1)))
                    .toCompletableFuture().join();
        } finally {
            realExecutor.shutdown();
        }

        assertThat(executorThread.get())
                .as("prepare() 应在 tradeDbTaskExecutor 线程执行，不在调用线程")
                .isNotNull()
                .isNotEqualTo(Thread.currentThread().getName());
    }

    @Test
    void rejectsBeforeRedisAcceptWhenMqPublisherIsUnavailable() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingPersistPublisher publisher = new RecordingPersistPublisher();
        publisher.available = false;
        V1CreateRechargeOrderUseCaseService localService = service(localAcceptPort, reliabilityPort, publisher);

        V1CardApiResult result = localService.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-MQ-OFF", "nonce-12345678901234567890123451", 1)))
                .toCompletableFuture().join();

        assertThat(result.code()).isEqualTo("500");
        assertThat(result.msg()).isEqualTo("系统繁忙");
        assertThat(localAcceptPort.lastCommand).isNull();
        assertThat(reliabilityPort.waitCalls).isZero();
        assertThat(publisher.publishCalls).isZero();
    }

    @Test
    void rejectsBeforeRedisAcceptWhenRedisHaGuardClosed() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingPersistPublisher publisher = new RecordingPersistPublisher();
        RecordingRedisHaGuard redisHaGuard = new RecordingRedisHaGuard();
        redisHaGuard.writeAllowed = false;
        V1CreateRechargeOrderUseCaseService localService = service(
                localAcceptPort,
                reliabilityPort,
                publisher,
                AcceptAnomalyRecordPort.noop(),
                redisHaGuard);

        V1CardApiResult result = localService.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-REDIS-GUARD", "nonce-12345678901234567890123460", 1)))
                .toCompletableFuture().join();

        assertThat(result.code()).isEqualTo("500");
        assertThat(result.msg()).isEqualTo("系统繁忙");
        assertThat(localAcceptPort.lastCommand).isNull();
        assertThat(reliabilityPort.waitCalls).isZero();
        assertThat(publisher.publishCalls).isZero();
    }

    @Test
    void waitsPublishesAndMarksQueuedBeforeReturningAcceptedOrder() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingPersistPublisher publisher = new RecordingPersistPublisher();
        V1CreateRechargeOrderUseCaseService localService = service(localAcceptPort, reliabilityPort, publisher);

        V1CardApiResult result = localService.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-RELIABLE", "nonce-12345678901234567890123452", 1)))
                .toCompletableFuture().join();

        assertThat(result.code()).isEqualTo("200");
        assertThat(reliabilityPort.waitCalls).isEqualTo(1);
        assertThat(publisher.publishCalls).isEqualTo(1);
        assertThat(reliabilityPort.markQueuedCalls).isEqualTo(1);
        assertThat(publisher.lastMessage.outOrderSn()).isEqualTo("OUT-RELIABLE");
        assertThat(publisher.lastMessage.initialOrderSnapshot())
                .containsEntry("order_id", "202607051337458961474292")
                .containsEntry("persist_status", "PENDING_MQ")
                .containsEntry("product_name", "话费50元")
                .containsEntry("product_value", 5000L)
                .containsEntry("product_type", 1)
                .containsEntry("coupon_type", "codekey")
                .containsEntry("supplier_channel", "TF_KA")
                .containsEntry("request_hash_version", "v1")
                .containsEntry("request_hash_alg", "SHA-256");
        assertThat(reliabilityPort.lastPersistMessageId).isEqualTo(publisher.lastMessage.messageId());
    }

    @Test
    void publishesMysqlPersistMessageWhenConfigChangedEvenIfRedisOrderWasNotWritten() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        localAcceptPort.nextResult = new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.CONFIG_CHANGED,
                "202607051337458961474292",
                "",
                "PRODUCT_CONFIG_STALE",
                "商品配置缺失或版本不一致",
                false,
                false,
                0L);
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingPersistPublisher publisher = new RecordingPersistPublisher();
        V1CreateRechargeOrderUseCaseService localService = service(localAcceptPort, reliabilityPort, publisher);

        V1CardApiResult result = localService.createRechargeOrder(
                        new V1MerchantCommand(
                                "app-gray",
                                "A36",
                                validRawBody("OUT-CONFIG-CHANGED", "nonce-12345678901234567890123461", 1)))
                .toCompletableFuture()
                .join();

        assertThat(result.code()).isEqualTo("500");
        assertThat(result.msg()).isEqualTo("商品配置缺失或版本不一致");
        assertThat(reliabilityPort.waitCalls).isZero();
        assertThat(reliabilityPort.markQueuedCalls).isZero();
        assertThat(publisher.publishCalls).isEqualTo(1);
        assertThat(publisher.lastMessage.orderVersion()).isEqualTo(1L);
        assertThat(publisher.lastMessage.initialOrderSnapshot())
                .containsEntry("order_id", "202607051337458961474292")
                .containsEntry("out_order_sn", "OUT-CONFIG-CHANGED")
                .containsEntry("order_status", "VALIDATION_FAILED")
                .containsEntry("validation_status", "FAILED")
                .containsEntry("fail_code", "PRODUCT_CONFIG_STALE")
                .containsEntry("fail_reason", "商品配置缺失或版本不一致")
                .containsEntry("dispatch_allowed", false)
                .containsEntry("balance_reserved", false)
                .containsEntry("persist_status", "PENDING_MQ")
                .containsEntry("order_version", 1L)
                .containsEntry("redis_order_written", false);
    }

    @Test
    void cancelsUnconfirmedAcceptWhenRedisWaitFails() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        reliabilityPort.waitResult = ReplicationWaitResult.missed(0, "WAIT replicas not reached");
        RecordingPersistPublisher publisher = new RecordingPersistPublisher();
        V1CreateRechargeOrderUseCaseService localService = service(localAcceptPort, reliabilityPort, publisher);

        V1CardApiResult result = localService.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-WAIT-FAIL", "nonce-12345678901234567890123453", 1)))
                .toCompletableFuture().join();

        assertThat(result.code()).isEqualTo("500");
        assertThat(result.msg()).isEqualTo("系统繁忙，请稍后重试");
        assertThat(reliabilityPort.waitCalls).isEqualTo(1);
        assertThat(reliabilityPort.cancelCalls).isEqualTo(1);
        assertThat(publisher.publishCalls).isZero();
        assertThat(reliabilityPort.markQueuedCalls).isZero();
    }

    @Test
    void recordsAcceptAnomalyWhenRedisWaitFailsAndCancelCannotConfirmRollback() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingAcceptAnomalyPort anomalyPort = new RecordingAcceptAnomalyPort();
        RecordingRedisHaGuard redisHaGuard = new RecordingRedisHaGuard();
        reliabilityPort.waitResult = ReplicationWaitResult.missed(0, "WAIT replicas not reached");
        reliabilityPort.cancelResult = CancelUnconfirmedAcceptResult.failed("CANCEL_FAILED", "订单状态不允许回滚");
        RecordingPersistPublisher publisher = new RecordingPersistPublisher();
        V1CreateRechargeOrderUseCaseService localService = service(
                localAcceptPort,
                reliabilityPort,
                publisher,
                anomalyPort,
                redisHaGuard);

        V1CardApiResult result = localService.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-WAIT-UNKNOWN", "nonce-12345678901234567890123455", 1)))
                .toCompletableFuture().join();

        assertThat(result.code()).isEqualTo("500");
        assertThat(result.msg()).isEqualTo("系统繁忙，请稍后查询订单状态");
        assertThat(anomalyPort.lastRecord).isNotNull();
        assertThat(anomalyPort.lastRecord.orgCode()).isEqualTo("A36");
        assertThat(anomalyPort.lastRecord.outOrderSn()).isEqualTo("OUT-WAIT-UNKNOWN");
        assertThat(anomalyPort.lastRecord.orderId()).isEqualTo("202607051337458961474292");
        assertThat(anomalyPort.lastRecord.rawContext())
                .containsEntry("wait_reason", "WAIT replicas not reached")
                .containsEntry("cancel_fail_code", "CANCEL_FAILED")
                .containsEntry("cancel_fail_reason", "订单状态不允许回滚");
        assertThat(redisHaGuard.closedReason).isEqualTo("WAIT_FAILED_CANCEL_FAILED");
        assertThat(redisHaGuard.closedContext)
                .containsEntry("org_code", "A36")
                .containsEntry("out_order_sn", "OUT-WAIT-UNKNOWN")
                .containsEntry("order_id", "202607051337458961474292");
    }

    @Test
    void stillReturnsProcessingWhenAcceptAnomalyRecordFails() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingAcceptAnomalyPort anomalyPort = new RecordingAcceptAnomalyPort();
        anomalyPort.failure = new IllegalStateException("anomaly table unavailable");
        reliabilityPort.waitResult = ReplicationWaitResult.missed(0, "WAIT_ERROR");
        reliabilityPort.cancelResult = CancelUnconfirmedAcceptResult.failed("CANCEL_FAILED", "订单状态不允许回滚");
        RecordingPersistPublisher publisher = new RecordingPersistPublisher();
        V1CreateRechargeOrderUseCaseService localService = service(localAcceptPort, reliabilityPort, publisher, anomalyPort);

        V1CardApiResult result = localService.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-WAIT-RECORD-FAIL", "nonce-12345678901234567890123456", 1)))
                .toCompletableFuture().join();

        assertThat(result.code()).isEqualTo("500");
        assertThat(result.msg()).isEqualTo("系统繁忙，请稍后查询订单状态");
        assertThat(anomalyPort.lastRecord).isNotNull();
        assertThat(publisher.publishCalls).isZero();
    }

    @Test
    void returnsProcessingWithoutWaitingForAcceptAnomalyDbWrite() throws Exception {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingAcceptAnomalyPort anomalyPort = new RecordingAcceptAnomalyPort();
        reliabilityPort.waitResult = ReplicationWaitResult.missed(0, "WAIT_ERROR");
        reliabilityPort.cancelResult = CancelUnconfirmedAcceptResult.failed("CANCEL_FAILED", "订单状态不允许回滚");
        RecordingPersistPublisher publisher = new RecordingPersistPublisher();
        OneInlineThenQueuedExecutor dbExecutor = new OneInlineThenQueuedExecutor();
        V1CreateRechargeOrderUseCaseService localService = service(
                localAcceptPort,
                reliabilityPort,
                publisher,
                anomalyPort,
                dbExecutor);

        CompletableFuture<V1CardApiResult> response = localService.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-WAIT-NO-BLOCK", "nonce-12345678901234567890123458", 1)))
                .toCompletableFuture();

        V1CardApiResult result = response.get(200, TimeUnit.MILLISECONDS);
        assertThat(result.code()).isEqualTo("500");
        assertThat(result.msg()).isEqualTo("系统繁忙，请稍后查询订单状态");
        assertThat(anomalyPort.lastRecord).isNull();
        assertThat(dbExecutor.queuedTasks()).isEqualTo(1);

        dbExecutor.runQueuedTasks();
        assertThat(anomalyPort.lastRecord).isNotNull();
    }

    @Test
    void usesRequestHashInAcceptAnomalyIdWhenLuaResultHasNoOrderId() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        localAcceptPort.nextResult = new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.ACCEPTED,
                "",
                "PASSED",
                "",
                "",
                true,
                true,
                1L);
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingAcceptAnomalyPort anomalyPort = new RecordingAcceptAnomalyPort();
        reliabilityPort.waitResult = ReplicationWaitResult.missed(0, "WAIT_ERROR");
        reliabilityPort.cancelResult = CancelUnconfirmedAcceptResult.failed("CANCEL_FAILED", "订单状态不允许回滚");
        RecordingPersistPublisher publisher = new RecordingPersistPublisher();
        V1CreateRechargeOrderUseCaseService localService = service(localAcceptPort, reliabilityPort, publisher, anomalyPort);

        localService.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-WAIT-NO-ORDER-ID", "nonce-12345678901234567890123459", 1)))
                .toCompletableFuture().join();

        assertThat(anomalyPort.lastRecord).isNotNull();
        assertThat(anomalyPort.lastRecord.anomalyId())
                .isEqualTo("accept-WAIT_CANCEL_FAILED-request-" + anomalyPort.lastRecord.requestHash())
                .doesNotContain("unknown");
    }

    @Test
    void recordsAcceptAnomalyOnDbExecutorWhenRedisCancelCompletesOnCallbackThread() throws Exception {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingAcceptAnomalyPort anomalyPort = new RecordingAcceptAnomalyPort();
        reliabilityPort.waitResult = ReplicationWaitResult.missed(0, "WAIT_ERROR");
        reliabilityPort.cancelResult = CancelUnconfirmedAcceptResult.failed("CANCEL_FAILED", "订单状态不允许回滚");
        var redisCallbackExecutor = Executors.newSingleThreadExecutor(command -> new Thread(command, "redis-callback-test"));
        var dbExecutor = Executors.newSingleThreadExecutor(command -> new Thread(command, "trade-db-anomaly-test"));
        reliabilityPort.cancelExecutor = redisCallbackExecutor;
        reliabilityPort.cancelDelayMillis = 50L;
        RecordingPersistPublisher publisher = new RecordingPersistPublisher();
        V1CreateRechargeOrderUseCaseService localService = service(
                localAcceptPort,
                reliabilityPort,
                publisher,
                anomalyPort,
                dbExecutor);

        try {
            V1CardApiResult result = localService.createRechargeOrder(
                    new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-WAIT-THREAD", "nonce-12345678901234567890123457", 1)))
                    .toCompletableFuture().join();

            assertThat(result.msg()).isEqualTo("系统繁忙，请稍后查询订单状态");
            assertThat(anomalyPort.awaitRecord(Duration.ofSeconds(1))).isTrue();
            assertThat(anomalyPort.recordThreadName)
                    .as("anomaly MyBatis 写库必须隔离到 tradeDbTaskExecutor，不能阻塞 Redis/Lettuce 回调线程")
                    .isEqualTo("trade-db-anomaly-test");
        } finally {
            redisCallbackExecutor.shutdown();
            dbExecutor.shutdown();
        }
    }

    @Test
    void returnsProcessingWhenMqConfirmFailsAfterRedisWaitSucceeded() {
        CapturingAcceptPort localAcceptPort = new CapturingAcceptPort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingPersistPublisher publisher = new RecordingPersistPublisher();
        publisher.nextResult = PersistMessagePublishResult.failed("msg-1", 1, "CONFIRM_NACK", "RabbitMQ confirm nack");
        V1CreateRechargeOrderUseCaseService localService = service(localAcceptPort, reliabilityPort, publisher);

        V1CardApiResult result = localService.createRechargeOrder(
                new V1MerchantCommand("app-gray", "A36", validRawBody("OUT-MQ-NACK", "nonce-12345678901234567890123454", 1)))
                .toCompletableFuture().join();

        assertThat(result.code()).isEqualTo("500");
        assertThat(result.msg()).isEqualTo("系统繁忙，请稍后查询订单状态");
        assertThat(publisher.publishCalls).isEqualTo(1);
        assertThat(reliabilityPort.markQueuedCalls).isZero();
        assertThat(result.data()).isEqualTo(Map.of("out_order_sn", "OUT-MQ-NACK", "order_status", "2"));
    }

    private static V1ProtocolPrecheckService precheckService() {
        return new V1ProtocolPrecheckService(
                new V1RequestBodyCodec(),
                new V1TimestampValidator(new V1TimestampSettings(Duration.ofSeconds(300)), CLOCK),
                new V1Sm3SignatureVerifier(),
                new V1ProtocolSettings(false));
    }

    private static V1CreateRechargeOrderUseCaseService service(
            AcceptRechargeOrderPort acceptPort,
            RechargeAcceptReliabilityPort reliabilityPort,
            PersistRechargeOrderMessagePublisher publisher) {
        return service(acceptPort, reliabilityPort, publisher, AcceptAnomalyRecordPort.noop());
    }

    private static V1CreateRechargeOrderUseCaseService service(
            AcceptRechargeOrderPort acceptPort,
            RechargeAcceptReliabilityPort reliabilityPort,
            PersistRechargeOrderMessagePublisher publisher,
            AcceptAnomalyRecordPort anomalyRecordPort) {
        return service(acceptPort, reliabilityPort, publisher, anomalyRecordPort, RedisHaGuardPort.noop(), SYNC_EXECUTOR);
    }

    private static V1CreateRechargeOrderUseCaseService service(
            AcceptRechargeOrderPort acceptPort,
            RechargeAcceptReliabilityPort reliabilityPort,
            PersistRechargeOrderMessagePublisher publisher,
            AcceptAnomalyRecordPort anomalyRecordPort,
            RedisHaGuardPort redisHaGuardPort) {
        return service(acceptPort, reliabilityPort, publisher, anomalyRecordPort, redisHaGuardPort, SYNC_EXECUTOR);
    }

    private static V1CreateRechargeOrderUseCaseService service(
            AcceptRechargeOrderPort acceptPort,
            RechargeAcceptReliabilityPort reliabilityPort,
            PersistRechargeOrderMessagePublisher publisher,
            AcceptAnomalyRecordPort anomalyRecordPort,
            Executor tradeDbTaskExecutor) {
        return service(acceptPort, reliabilityPort, publisher, anomalyRecordPort, RedisHaGuardPort.noop(), tradeDbTaskExecutor);
    }

    private static V1CreateRechargeOrderUseCaseService service(
            AcceptRechargeOrderPort acceptPort,
            RechargeAcceptReliabilityPort reliabilityPort,
            PersistRechargeOrderMessagePublisher publisher,
            AcceptAnomalyRecordPort anomalyRecordPort,
            RedisHaGuardPort redisHaGuardPort,
            Executor tradeDbTaskExecutor) {
        return new V1CreateRechargeOrderUseCaseService(
                precheckService(),
                new RequestHashCanonicalizer(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "AES-KEY", true, 7L)),
                (orgCode, productId, account) -> V1ProductSelection.passed(
                        productId,
                        productId,
                        "TF_KA",
                        "话费50元",
                        "codekey",
                        1,
                        5200L,
                        5000L,
                        4900L,
                        11L),
                acceptPort,
                reliabilityPort,
                publisher,
                anomalyRecordPort,
                redisHaGuardPort,
                () -> "202607051337458961474292",
                CLOCK,
                new V1RechargeAcceptSettings(Duration.ofMinutes(5)),
                tradeDbTaskExecutor);
    }

    private static String validRawBody(String outOrderSn, String nonceStr, int num) {
        return rawBody(outOrderSn, nonceStr, num, "http://notify");
    }

    private static String rawBody(String outOrderSn, String nonceStr, int num, String notifyUrl) {
        String signSource = "account=18186222336,aesKey=AES-KEY,nonce_str=%s,notify_url=%s,num=%d,"
                + "out_order_sn=%s,product_id=73301280,timestamp=1783224958";
        String sign = new V1Sm3SignatureVerifier().digestHex(signSource.formatted(nonceStr, notifyUrl, num, outOrderSn));
        return """
                {
                  "product_id": "73301280",
                  "out_order_sn": "%s",
                  "account": "18186222336",
                  "timestamp": "1783224958",
                  "nonce_str": "%s",
                  "notify_url": "%s",
                  "num": %d,
                  "order_price": 5200,
                  "product_type": 1,
                  "sign": "%s"
                }
                """.formatted(outOrderSn, nonceStr, notifyUrl, num, sign);
    }

    private static final class CapturingAcceptPort implements AcceptRechargeOrderPort {

        private AcceptRechargeOrderCommand lastCommand;
        private AcceptRechargeOrderResult nextResult = new AcceptRechargeOrderResult(
                AcceptRechargeOrderStatus.ACCEPTED,
                "202607051337458961474292",
                "PASSED",
                "",
                "",
                true,
                true,
                1L);

        @Override
        public CompletableFuture<AcceptRechargeOrderResult> accept(AcceptRechargeOrderCommand command) {
            this.lastCommand = command;
            return CompletableFuture.completedFuture(nextResult);
        }
    }

    private static final class RecordingProductConfigPreheatPort implements V1ProductConfigPreheatPort {

        private String orgCode;
        private V1ProductSelection selection;

        @Override
        public void preheatIfMissing(String orgCode, V1ProductSelection selection) {
            this.orgCode = orgCode;
            this.selection = selection;
        }
    }

    private static final class RecordingReliabilityPort implements RechargeAcceptReliabilityPort {

        private ReplicationWaitResult waitResult = ReplicationWaitResult.confirmed(1);
        private CancelUnconfirmedAcceptResult cancelResult = CancelUnconfirmedAcceptResult.success();
        private MarkPersistQueuedResult markResult = MarkPersistQueuedResult.success();
        private int waitCalls;
        private int cancelCalls;
        private int markQueuedCalls;
        private String lastPersistMessageId;
        private Executor cancelExecutor = SYNC_EXECUTOR;
        private long cancelDelayMillis;

        @Override
        public CompletableFuture<ReplicationWaitResult> waitForReplication(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult) {
            waitCalls++;
            return CompletableFuture.completedFuture(waitResult);
        }

        @Override
        public CompletableFuture<CancelUnconfirmedAcceptResult> cancelUnconfirmedAccept(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String reason) {
            cancelCalls++;
            return CompletableFuture.supplyAsync(() -> {
                sleepQuietly(cancelDelayMillis);
                return cancelResult;
            }, cancelExecutor);
        }

        @Override
        public CompletableFuture<MarkPersistQueuedResult> markPersistQueued(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String persistMessageId,
                int publishCount) {
            markQueuedCalls++;
            lastPersistMessageId = persistMessageId;
            return CompletableFuture.completedFuture(markResult);
        }

        @Override
        public CompletableFuture<MarkPersistStateResult> markDbPersisted(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                long persistedVersion,
                long nowEpochMillis) {
            return CompletableFuture.completedFuture(MarkPersistStateResult.success());
        }

        @Override
        public CompletableFuture<MarkPersistStateResult> markPersistFailed(
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

    private static final class RecordingPersistPublisher implements PersistRechargeOrderMessagePublisher {

        private boolean available = true;
        private int publishCalls;
        private PersistTradeOrderMessage lastMessage;
        private PersistMessagePublishResult nextResult = PersistMessagePublishResult.confirmed("msg-1", 1);

        @Override
        public boolean isAvailable() {
            return available;
        }

        @Override
        public CompletableFuture<PersistMessagePublishResult> publish(PersistTradeOrderMessage message) {
            publishCalls++;
            lastMessage = message;
            return CompletableFuture.completedFuture(nextResult.withMessageId(message.messageId()));
        }
    }

    private static void sleepQuietly(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("test sleep interrupted", ex);
        }
    }

    private static final class RecordingAcceptAnomalyPort implements AcceptAnomalyRecordPort {

        private AcceptAnomalyRecord lastRecord;
        private RuntimeException failure;
        private String recordThreadName;
        private final CountDownLatch recordLatch = new CountDownLatch(1);

        @Override
        public void record(AcceptAnomalyRecord record) {
            this.lastRecord = record;
            this.recordThreadName = Thread.currentThread().getName();
            this.recordLatch.countDown();
            if (failure != null) {
                throw failure;
            }
        }

        private boolean awaitRecord(Duration timeout) throws InterruptedException {
            return recordLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static final class RecordingRedisHaGuard implements RedisHaGuardPort {

        private boolean writeAllowed = true;
        private String closedReason;
        private Map<String, Object> closedContext = Map.of();

        @Override
        public boolean isWriteAllowed() {
            return writeAllowed;
        }

        @Override
        public void closeWrite(String reason, Map<String, Object> context) {
            this.writeAllowed = false;
            this.closedReason = reason;
            this.closedContext = context;
        }
    }

    private static final class OneInlineThenQueuedExecutor implements Executor {

        private boolean firstTask = true;
        private final Queue<Runnable> queued = new ConcurrentLinkedQueue<>();

        @Override
        public void execute(Runnable command) {
            if (firstTask) {
                firstTask = false;
                command.run();
                return;
            }
            queued.add(command);
        }

        private int queuedTasks() {
            return queued.size();
        }

        private void runQueuedTasks() {
            Runnable task;
            while ((task = queued.poll()) != null) {
                task.run();
            }
        }
    }
}
