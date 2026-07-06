package com.cardshop.trade.engine.api.v1.usecase;

import com.cardshop.trade.engine.api.v1.codec.V1RequestBodyCodec;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckService;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolSettings;
import com.cardshop.trade.engine.api.v1.security.V1Sm3SignatureVerifier;
import com.cardshop.trade.engine.api.v1.security.V1TimestampSettings;
import com.cardshop.trade.engine.api.v1.security.V1TimestampValidator;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderResult;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import com.cardshop.trade.engine.application.recharge.CancelUnconfirmedAcceptResult;
import com.cardshop.trade.engine.application.recharge.MarkPersistQueuedResult;
import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.PersistTradeOrderMessage;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.ReplicationWaitResult;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.application.v1.V1MerchantCommand;
import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.application.v1.V1ProductCandidateSelector;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.domain.service.RequestHashCanonicalizer;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V1 创建直充订单新链路用例测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」、
 * 「20.13.5 商品选择规则」和「20.13.6 Redis Lua 下单结果」。旧卡管 V3/V2 的核心兼容点是：
 * `product_id` 表示父商品编码，验签通过后再按父商品选择可售子商品并进入 Redis 原子接单。
 */
class V1CreateRechargeOrderUseCaseServiceTest {

    private static final Instant NOW = Instant.parse("2026-07-05T04:15:58Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final Executor SYNC_EXECUTOR = Runnable::run;

    private final CapturingAcceptPort acceptPort = new CapturingAcceptPort();
    private final V1CreateRechargeOrderUseCaseService service = new V1CreateRechargeOrderUseCaseService(
            precheckService(),
            new RequestHashCanonicalizer(),
            (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "AES-KEY", true, 7L)),
            (orgCode, parentProductId, account) -> V1ProductSelection.passed(
                    parentProductId,
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
                (orgCode, parentProductId, account) -> V1ProductSelection.failed(
                        parentProductId,
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
                (orgCode, parentProductId, account) -> V1ProductSelection.passed(
                        parentProductId, "73301289", "TF_KA", "话费50元", "codekey", 1, 5200L, 5000L, 4900L, 11L),
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
                .containsEntry("persist_status", "PENDING_MQ");
        assertThat(reliabilityPort.lastPersistMessageId).isEqualTo(publisher.lastMessage.messageId());
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
        return new V1CreateRechargeOrderUseCaseService(
                precheckService(),
                new RequestHashCanonicalizer(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "AES-KEY", true, 7L)),
                (orgCode, parentProductId, account) -> V1ProductSelection.passed(
                        parentProductId,
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
                reliabilityPort,
                publisher,
                () -> "202607051337458961474292",
                CLOCK,
                new V1RechargeAcceptSettings(Duration.ofMinutes(5)),
                SYNC_EXECUTOR);
    }

    private static String validRawBody(String outOrderSn, String nonceStr, int num) {
        String signSource = "account=18186222336,aesKey=AES-KEY,nonce_str=%s,notify_url=http://notify,num=%d,"
                + "out_order_sn=%s,product_id=73301280,timestamp=1783224958";
        String sign = new V1Sm3SignatureVerifier().digestHex(signSource.formatted(nonceStr, num, outOrderSn));
        return """
                {
                  "product_id": "73301280",
                  "out_order_sn": "%s",
                  "account": "18186222336",
                  "timestamp": "1783224958",
                  "nonce_str": "%s",
                  "notify_url": "http://notify",
                  "num": %d,
                  "order_price": 5200,
                  "product_type": 1,
                  "sign": "%s"
                }
                """.formatted(outOrderSn, nonceStr, num, sign);
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

    private static final class RecordingReliabilityPort implements RechargeAcceptReliabilityPort {

        private ReplicationWaitResult waitResult = ReplicationWaitResult.confirmed(1);
        private CancelUnconfirmedAcceptResult cancelResult = CancelUnconfirmedAcceptResult.success();
        private MarkPersistQueuedResult markResult = MarkPersistQueuedResult.success();
        private int waitCalls;
        private int cancelCalls;
        private int markQueuedCalls;
        private String lastPersistMessageId;

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
            return CompletableFuture.completedFuture(cancelResult);
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
}
