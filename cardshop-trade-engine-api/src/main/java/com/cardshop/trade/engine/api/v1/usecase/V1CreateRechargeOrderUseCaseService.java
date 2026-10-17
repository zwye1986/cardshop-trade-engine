package com.cardshop.trade.engine.api.v1.usecase;

import com.cardshop.trade.engine.api.v1.dto.V1RechargeCreateOrderRequest;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolFailureReason;
import com.cardshop.trade.engine.api.v1.protocol.V1MerchantSecurityProfile;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckResult;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckService;
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
import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.v1.CreateRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.RechargeOrderIdGenerator;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.application.v1.V1CardApiResultStatus;
import com.cardshop.trade.engine.application.v1.V1CreateOrderStageObserver;
import com.cardshop.trade.engine.application.v1.V1MerchantCommand;
import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.application.v1.V1ProductCandidateSelector;
import com.cardshop.trade.engine.application.v1.V1ProductCacheUnavailableException;
import com.cardshop.trade.engine.application.v1.V1ProductConfigPreheatPort;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
import com.cardshop.trade.engine.domain.order.PersistStatus;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.domain.service.RequestHashCanonicalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * V1 创建直充订单新链路用例。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」、
 * 「5. Domain 状态与业务规则」和「6. Redis 受理与缓存层」。该用例完成协议预检、request_hash、
 * Java 商品候选校验、Redis-first 原子接单、Redis WAIT 和 RabbitMQ publisher confirm。
 * MySQL 落库仍由后续 Consumer 异步完成，本用例只保证接口返回前订单进入可恢复的 Redis/MQ 通道。
 */
public class V1CreateRechargeOrderUseCaseService implements CreateRechargeOrderUseCase {

    private static final Logger log = LoggerFactory.getLogger(V1CreateRechargeOrderUseCaseService.class);
    private static final String PERSIST_STATUS_PENDING_MQ = PersistStatus.PENDING_MQ.name();
    private static final String HTTP_SCHEME = "http";
    private static final String HTTPS_SCHEME = "https";
    private static final String FAIL_CODE_MERCHANT_DISABLED = "MERCHANT_DISABLED";
    private static final String FAIL_CODE_INVALID_NUM = "INVALID_NUM";
    private static final String FAIL_CODE_BALANCE_NOT_ENOUGH = "BALANCE_NOT_ENOUGH";
    private static final int DIRECT_RECHARGE_ORDER_NUM = 1;
    private static final int NONCE_STR_LENGTH = 32;
    private static final int MAX_ACCEPT_ANOMALY_ID_LENGTH = 100;
    private static final String ORDER_STATUS_ACCEPTED = "ACCEPTED";
    private static final String ORDER_STATUS_VALIDATION_FAILED = "VALIDATION_FAILED";
    private static final String REDIS_ORDER_WRITTEN_FIELD = "redis_order_written";
    private static final Duration PRODUCT_CACHE_ERROR_LOG_INTERVAL = Duration.ofSeconds(10);
    private static final String LOG_EVENT_PRODUCT_CACHE_UNAVAILABLE = "v1-create-order.product-cache.unavailable";
    private static final LogStormGuard PRODUCT_CACHE_ERROR_LOG_GUARD =
            LogStormGuard.atMostOncePer(PRODUCT_CACHE_ERROR_LOG_INTERVAL);

    private final V1ProtocolPrecheckService precheckService;
    private final RequestHashCanonicalizer requestHashCanonicalizer;
    private final V1MerchantProfilePort merchantProfilePort;
    private final V1ProductCandidateSelector productCandidateSelector;
    private final V1ProductConfigPreheatPort productConfigPreheatPort;
    private final AcceptRechargeOrderPort acceptRechargeOrderPort;
    private final RechargeAcceptReliabilityPort reliabilityPort;
    private final PersistRechargeOrderMessagePublisher persistMessagePublisher;
    private final AcceptAnomalyRecordPort acceptAnomalyRecordPort;
    private final RedisHaGuardPort redisHaGuardPort;
    private final RechargeOrderIdGenerator orderIdGenerator;
    private final Clock clock;
    private final V1RechargeAcceptSettings settings;
    private final Executor tradeDbTaskExecutor;
    private final Supplier<V1CreateOrderStageObserver> stageObserverSupplier;

    public V1CreateRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            RequestHashCanonicalizer requestHashCanonicalizer,
            V1MerchantProfilePort merchantProfilePort,
            V1ProductCandidateSelector productCandidateSelector,
            AcceptRechargeOrderPort acceptRechargeOrderPort,
            RechargeOrderIdGenerator orderIdGenerator,
            Clock clock,
            V1RechargeAcceptSettings settings,
            Executor tradeDbTaskExecutor) {
        this(
                precheckService,
                requestHashCanonicalizer,
                merchantProfilePort,
                productCandidateSelector,
                acceptRechargeOrderPort,
                new NoopRechargeAcceptReliabilityPort(),
                new NoopPersistRechargeOrderMessagePublisher(),
                AcceptAnomalyRecordPort.noop(),
                RedisHaGuardPort.noop(),
                V1ProductConfigPreheatPort.noop(),
                orderIdGenerator,
                clock,
                settings,
                tradeDbTaskExecutor,
                V1CreateOrderStageObserver.noop());
    }

    public V1CreateRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            RequestHashCanonicalizer requestHashCanonicalizer,
            V1MerchantProfilePort merchantProfilePort,
            V1ProductCandidateSelector productCandidateSelector,
            AcceptRechargeOrderPort acceptRechargeOrderPort,
            V1ProductConfigPreheatPort productConfigPreheatPort,
            RechargeOrderIdGenerator orderIdGenerator,
            Clock clock,
            V1RechargeAcceptSettings settings,
            Executor tradeDbTaskExecutor) {
        this(
                precheckService,
                requestHashCanonicalizer,
                merchantProfilePort,
                productCandidateSelector,
                acceptRechargeOrderPort,
                new NoopRechargeAcceptReliabilityPort(),
                new NoopPersistRechargeOrderMessagePublisher(),
                AcceptAnomalyRecordPort.noop(),
                RedisHaGuardPort.noop(),
                productConfigPreheatPort,
                orderIdGenerator,
                clock,
                settings,
                tradeDbTaskExecutor,
                V1CreateOrderStageObserver.noop());
    }

    public V1CreateRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            RequestHashCanonicalizer requestHashCanonicalizer,
            V1MerchantProfilePort merchantProfilePort,
            V1ProductCandidateSelector productCandidateSelector,
            AcceptRechargeOrderPort acceptRechargeOrderPort,
            RechargeAcceptReliabilityPort reliabilityPort,
            PersistRechargeOrderMessagePublisher persistMessagePublisher,
            RechargeOrderIdGenerator orderIdGenerator,
            Clock clock,
            V1RechargeAcceptSettings settings,
            Executor tradeDbTaskExecutor) {
        this(
                precheckService,
                requestHashCanonicalizer,
                merchantProfilePort,
                productCandidateSelector,
                acceptRechargeOrderPort,
                reliabilityPort,
                persistMessagePublisher,
                AcceptAnomalyRecordPort.noop(),
                RedisHaGuardPort.noop(),
                V1ProductConfigPreheatPort.noop(),
                orderIdGenerator,
                clock,
                settings,
                tradeDbTaskExecutor,
                V1CreateOrderStageObserver.noop());
    }

    public V1CreateRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            RequestHashCanonicalizer requestHashCanonicalizer,
            V1MerchantProfilePort merchantProfilePort,
            V1ProductCandidateSelector productCandidateSelector,
            AcceptRechargeOrderPort acceptRechargeOrderPort,
            RechargeAcceptReliabilityPort reliabilityPort,
            PersistRechargeOrderMessagePublisher persistMessagePublisher,
            AcceptAnomalyRecordPort acceptAnomalyRecordPort,
            RechargeOrderIdGenerator orderIdGenerator,
            Clock clock,
            V1RechargeAcceptSettings settings,
            Executor tradeDbTaskExecutor) {
        this(
                precheckService,
                requestHashCanonicalizer,
                merchantProfilePort,
                productCandidateSelector,
                acceptRechargeOrderPort,
                reliabilityPort,
                persistMessagePublisher,
                acceptAnomalyRecordPort,
                RedisHaGuardPort.noop(),
                V1ProductConfigPreheatPort.noop(),
                orderIdGenerator,
                clock,
                settings,
                tradeDbTaskExecutor,
                V1CreateOrderStageObserver.noop());
    }

    public V1CreateRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            RequestHashCanonicalizer requestHashCanonicalizer,
            V1MerchantProfilePort merchantProfilePort,
            V1ProductCandidateSelector productCandidateSelector,
            AcceptRechargeOrderPort acceptRechargeOrderPort,
            RechargeAcceptReliabilityPort reliabilityPort,
            PersistRechargeOrderMessagePublisher persistMessagePublisher,
            AcceptAnomalyRecordPort acceptAnomalyRecordPort,
            RedisHaGuardPort redisHaGuardPort,
            RechargeOrderIdGenerator orderIdGenerator,
            Clock clock,
            V1RechargeAcceptSettings settings,
            Executor tradeDbTaskExecutor) {
        this(
                precheckService,
                requestHashCanonicalizer,
                merchantProfilePort,
                productCandidateSelector,
                acceptRechargeOrderPort,
                reliabilityPort,
                persistMessagePublisher,
                acceptAnomalyRecordPort,
                redisHaGuardPort,
                V1ProductConfigPreheatPort.noop(),
                orderIdGenerator,
                clock,
                settings,
                tradeDbTaskExecutor,
                V1CreateOrderStageObserver.noop());
    }

    public V1CreateRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            RequestHashCanonicalizer requestHashCanonicalizer,
            V1MerchantProfilePort merchantProfilePort,
            V1ProductCandidateSelector productCandidateSelector,
            AcceptRechargeOrderPort acceptRechargeOrderPort,
            RechargeAcceptReliabilityPort reliabilityPort,
            PersistRechargeOrderMessagePublisher persistMessagePublisher,
            AcceptAnomalyRecordPort acceptAnomalyRecordPort,
            RedisHaGuardPort redisHaGuardPort,
            V1ProductConfigPreheatPort productConfigPreheatPort,
            RechargeOrderIdGenerator orderIdGenerator,
            Clock clock,
            V1RechargeAcceptSettings settings,
            Executor tradeDbTaskExecutor) {
        this(
                precheckService,
                requestHashCanonicalizer,
                merchantProfilePort,
                productCandidateSelector,
                acceptRechargeOrderPort,
                reliabilityPort,
                persistMessagePublisher,
                acceptAnomalyRecordPort,
                redisHaGuardPort,
                productConfigPreheatPort,
                orderIdGenerator,
                clock,
                settings,
                tradeDbTaskExecutor,
                V1CreateOrderStageObserver.noop());
    }

    public V1CreateRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            RequestHashCanonicalizer requestHashCanonicalizer,
            V1MerchantProfilePort merchantProfilePort,
            V1ProductCandidateSelector productCandidateSelector,
            AcceptRechargeOrderPort acceptRechargeOrderPort,
            RechargeAcceptReliabilityPort reliabilityPort,
            PersistRechargeOrderMessagePublisher persistMessagePublisher,
            AcceptAnomalyRecordPort acceptAnomalyRecordPort,
            RedisHaGuardPort redisHaGuardPort,
            V1ProductConfigPreheatPort productConfigPreheatPort,
            RechargeOrderIdGenerator orderIdGenerator,
            Clock clock,
            V1RechargeAcceptSettings settings,
            Executor tradeDbTaskExecutor,
            V1CreateOrderStageObserver stageObserver) {
        this(
                precheckService,
                requestHashCanonicalizer,
                merchantProfilePort,
                productCandidateSelector,
                acceptRechargeOrderPort,
                reliabilityPort,
                persistMessagePublisher,
                acceptAnomalyRecordPort,
                redisHaGuardPort,
                productConfigPreheatPort,
                orderIdGenerator,
                clock,
                settings,
                tradeDbTaskExecutor,
                () -> stageObserver == null ? V1CreateOrderStageObserver.noop() : stageObserver);
    }

    public V1CreateRechargeOrderUseCaseService(
            V1ProtocolPrecheckService precheckService,
            RequestHashCanonicalizer requestHashCanonicalizer,
            V1MerchantProfilePort merchantProfilePort,
            V1ProductCandidateSelector productCandidateSelector,
            AcceptRechargeOrderPort acceptRechargeOrderPort,
            RechargeAcceptReliabilityPort reliabilityPort,
            PersistRechargeOrderMessagePublisher persistMessagePublisher,
            AcceptAnomalyRecordPort acceptAnomalyRecordPort,
            RedisHaGuardPort redisHaGuardPort,
            V1ProductConfigPreheatPort productConfigPreheatPort,
            RechargeOrderIdGenerator orderIdGenerator,
            Clock clock,
            V1RechargeAcceptSettings settings,
            Executor tradeDbTaskExecutor,
            Supplier<V1CreateOrderStageObserver> stageObserverSupplier) {
        this.precheckService = precheckService;
        this.requestHashCanonicalizer = requestHashCanonicalizer;
        this.merchantProfilePort = merchantProfilePort;
        this.productCandidateSelector = productCandidateSelector;
        this.productConfigPreheatPort = productConfigPreheatPort == null
                ? V1ProductConfigPreheatPort.noop()
                : productConfigPreheatPort;
        this.acceptRechargeOrderPort = acceptRechargeOrderPort;
        this.reliabilityPort = reliabilityPort;
        this.persistMessagePublisher = persistMessagePublisher;
        this.acceptAnomalyRecordPort = acceptAnomalyRecordPort == null
                ? AcceptAnomalyRecordPort.noop()
                : acceptAnomalyRecordPort;
        this.redisHaGuardPort = redisHaGuardPort == null
                ? RedisHaGuardPort.noop()
                : redisHaGuardPort;
        this.orderIdGenerator = orderIdGenerator;
        this.clock = clock;
        this.settings = settings;
        this.tradeDbTaskExecutor = tradeDbTaskExecutor;
        Objects.requireNonNull(stageObserverSupplier, "stageObserverSupplier must not be null");
        this.stageObserverSupplier = () -> {
            V1CreateOrderStageObserver observer = stageObserverSupplier.get();
            return observer == null ? V1CreateOrderStageObserver.noop() : observer;
        };
    }

    @Override
    public CompletionStage<V1CardApiResult> createRechargeOrder(V1MerchantCommand command) {
        long syncStartedNanos = System.nanoTime();
        return CompletableFuture
                .supplyAsync(() -> timedPrepare(command), tradeDbTaskExecutor)
                .thenCompose(preparation -> preparation.errorResult != null
                        ? CompletableFuture.completedFuture(preparation.errorResult)
                        : acceptAfterDependencyGate(preparation))
                .whenComplete((ignored, error) -> recordStage(
                        V1CreateOrderStageObserver.STAGE_SYNC_TOTAL,
                        Duration.ofNanos(Math.max(0L, System.nanoTime() - syncStartedNanos))));
    }

    private Preparation timedPrepare(V1MerchantCommand command) {
        long startedNanos = System.nanoTime();
        try {
            return prepare(command);
        } finally {
            recordStage(
                    V1CreateOrderStageObserver.STAGE_PREPARE,
                    Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNanos)));
        }
    }

    private void recordStage(String stage, Duration duration) {
        stageObserverSupplier.get().recordStage(stage, duration);
    }

    private <T> CompletionStage<T> recordStageAsync(String stage, long startedNanos, CompletionStage<T> stageFuture) {
        return stageFuture.whenComplete((ignored, error) -> recordStage(
                stage,
                Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNanos))));
    }

    /**
     * Redis HA Guard 和 RabbitMQ publisher breaker 必须在 Redis accept Lua 之前检查。
     *
     * <p>参见详细设计「4. Application 用例编排层」。如果 Redis 主从切换/修复窗口或 MQ 全局不可用时仍继续写
     * Redis，会制造大量只能依赖补偿任务追平的 pending 订单，因此这里 fail closed。
     */
    private CompletionStage<V1CardApiResult> acceptAfterDependencyGate(Preparation preparation) {
        if (!redisHaGuardPort.isWriteAllowed()) {
            log.warn("V1直充下单Redis HA Guard关闭写入，Redis接单前拒绝 appId={}, orgCode={}, outOrderSn={}, requestHash={}",
                    preparation.acceptCommand.appId(),
                    preparation.acceptCommand.orgCode(),
                    preparation.acceptCommand.outOrderSn(),
                    preparation.acceptCommand.requestHash());
            return CompletableFuture.completedFuture(V1CardApiResultStatus.SYSTEM_BUSY.result("系统繁忙"));
        }
        if (!persistMessagePublisher.isAvailable()) {
            log.warn("V1直充下单MQ发布不可用，Redis接单前拒绝 appId={}, orgCode={}, outOrderSn={}, requestHash={}",
                    preparation.acceptCommand.appId(),
                    preparation.acceptCommand.orgCode(),
                    preparation.acceptCommand.outOrderSn(),
                    preparation.acceptCommand.requestHash());
            return CompletableFuture.completedFuture(V1CardApiResultStatus.SYSTEM_BUSY.result("系统繁忙"));
        }
        long acceptStartedNanos = System.nanoTime();
        return recordStageAsync(
                V1CreateOrderStageObserver.STAGE_ACCEPT,
                acceptStartedNanos,
                acceptRechargeOrderPort.accept(preparation.acceptCommand))
                .thenCompose(acceptResult -> afterRedisAccept(preparation, acceptResult));
    }

    private CompletionStage<V1CardApiResult> afterRedisAccept(
            Preparation preparation,
            AcceptRechargeOrderResult acceptResult) {
        if (!acceptResult.orderWritten()) {
            if (acceptResult.status() == AcceptRechargeOrderStatus.CONFIG_CHANGED) {
                return publishMysqlOnlyFailedOrder(preparation, acceptResult);
            }
            return CompletableFuture.completedFuture(toV1Result(preparation.outOrderSn, acceptResult));
        }
        long waitStartedNanos = System.nanoTime();
        return recordStageAsync(
                V1CreateOrderStageObserver.STAGE_WAIT,
                waitStartedNanos,
                reliabilityPort.waitForReplication(preparation.acceptCommand, acceptResult))
                .thenCompose(waitResult -> waitResult.confirmed()
                        ? publishPersistMessage(preparation, acceptResult)
                        : cancelUnconfirmedAccept(preparation, acceptResult, waitResult));
    }

    private CompletionStage<V1CardApiResult> cancelUnconfirmedAccept(
            Preparation preparation,
            AcceptRechargeOrderResult acceptResult,
            ReplicationWaitResult waitResult) {
        return reliabilityPort
                .cancelUnconfirmedAccept(preparation.acceptCommand, acceptResult, waitResult.reason())
                .thenCompose(cancelResult -> toWaitFailureResult(preparation, acceptResult, waitResult, cancelResult));
    }

    private CompletionStage<V1CardApiResult> publishPersistMessage(
            Preparation preparation,
            AcceptRechargeOrderResult acceptResult) {
        PersistTradeOrderMessage message = buildPersistMessage(preparation.acceptCommand, acceptResult);
        long publishStartedNanos = System.nanoTime();
        return recordStageAsync(
                V1CreateOrderStageObserver.STAGE_MQ_CONFIRM,
                publishStartedNanos,
                persistMessagePublisher.publish(message))
                .thenCompose(publishResult -> publishResult.confirmed()
                        ? markPersistQueued(preparation, acceptResult, publishResult)
                        : CompletableFuture.completedFuture(processingResult(preparation.outOrderSn, acceptResult)));
    }

    private CompletionStage<V1CardApiResult> publishMysqlOnlyFailedOrder(
            Preparation preparation,
            AcceptRechargeOrderResult acceptResult) {
        /*
         * CONFIG_CHANGED 是 Lua 在写 Redis 订单前的 fail closed 结果，但旧卡管仍要求能在 mc_order 查到失败单。
         * 参见详细设计「6. Redis 受理与缓存层」和「8.3 异步落库 Consumer」：这里只发布携带完整
         * initial snapshot 的 MySQL 落库消息，并显式标记 redis_order_written=false；Consumer 落库后不能再
         * 调 markDbPersisted，因为 Redis 中没有对应订单、pending 或幂等快照。
         */
        PersistTradeOrderMessage message = buildPersistMessage(preparation.acceptCommand, acceptResult);
        long publishStartedNanos = System.nanoTime();
        return recordStageAsync(
                V1CreateOrderStageObserver.STAGE_MQ_CONFIRM,
                publishStartedNanos,
                persistMessagePublisher.publish(message))
                .thenApply(publishResult -> toV1Result(preparation.outOrderSn, acceptResult));
    }

    private CompletionStage<V1CardApiResult> markPersistQueued(
            Preparation preparation,
            AcceptRechargeOrderResult acceptResult,
            PersistMessagePublishResult publishResult) {
        long markStartedNanos = System.nanoTime();
        return recordStageAsync(
                V1CreateOrderStageObserver.STAGE_MARK_PERSIST_QUEUED,
                markStartedNanos,
                reliabilityPort.markPersistQueued(
                        preparation.acceptCommand,
                        acceptResult,
                        publishResult.messageId(),
                        publishResult.attemptCount()))
                .thenApply(markResult -> toMarkQueuedResult(preparation.outOrderSn, acceptResult, markResult));
    }

    private PersistTradeOrderMessage buildPersistMessage(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult) {
        String messageId = "persist-" + command.orderId() + "-" + UUID.randomUUID();
        String validationStatus = validationStatus(acceptResult);
        long orderVersion = Math.max(1L, acceptResult.orderVersion());
        Map<String, Object> snapshot = Map.ofEntries(
                Map.entry("order_id", acceptResult.orderId()),
                Map.entry("org_code", command.orgCode()),
                Map.entry("app_id", command.appId()),
                Map.entry("out_order_sn", command.outOrderSn()),
                Map.entry("product_id", command.productId()),
                Map.entry("selected_product_id", command.selectedProductId()),
                Map.entry("account", command.account()),
                Map.entry("notify_url", command.notifyUrl()),
                Map.entry("num", command.num()),
                Map.entry("order_price", command.orderAmountCent()),
                Map.entry("product_name", command.productName()),
                Map.entry("product_value", command.productValueCent()),
                Map.entry("product_type", command.productType()),
                Map.entry("coupon_type", command.couponType()),
                Map.entry("supplier_channel", command.supplierChannel()),
                Map.entry("supplier_batch_id", command.supplierBatchId()),
                Map.entry("order_status", orderStatus(validationStatus)),
                Map.entry("validation_status", validationStatus),
                Map.entry("fail_code", acceptResult.failCode()),
                Map.entry("fail_reason", acceptResult.failReason()),
                Map.entry("dispatch_allowed", acceptResult.dispatchAllowed()),
                Map.entry("balance_reserved", acceptResult.balanceReserved()),
                Map.entry("persist_status", PERSIST_STATUS_PENDING_MQ),
                Map.entry("order_version", orderVersion),
                Map.entry(REDIS_ORDER_WRITTEN_FIELD, acceptResult.orderWritten()),
                Map.entry("request_hash", command.requestHash()),
                Map.entry("request_hash_version", command.requestHashVersion()),
                Map.entry("request_hash_alg", command.requestHashAlg()));
        return new PersistTradeOrderMessage(
                messageId,
                command.orgCode(),
                acceptResult.orderId(),
                command.outOrderSn(),
                command.requestHash(),
                orderVersion,
                snapshot);
    }

    private static String validationStatus(AcceptRechargeOrderResult acceptResult) {
        if (StringUtils.hasText(acceptResult.validationStatus())) {
            return acceptResult.validationStatus();
        }
        return acceptResult.dispatchAllowed() ? ValidationStatus.PASSED.name() : ValidationStatus.FAILED.name();
    }

    private static String orderStatus(String validationStatus) {
        return ValidationStatus.PASSED.name().equals(validationStatus)
                ? ORDER_STATUS_ACCEPTED
                : ORDER_STATUS_VALIDATION_FAILED;
    }

    private Preparation prepare(V1MerchantCommand command) {
        Optional<V1MerchantProfile> maybeMerchant = merchantProfilePort.findByAppIdAndOrgCode(command.appId(), command.orgCode());
        if (maybeMerchant.isEmpty()) {
            log.warn("V1直充下单商户资料缺失 appId={}, orgCode={}", command.appId(), command.orgCode());
            return Preparation.error(V1CardApiResultStatus.AUTH_FAILED.result());
        }

        V1MerchantProfile merchant = maybeMerchant.get();
        /*
         * 历史X-Load-Test参数仅作调用兼容，V1.2在所有接口完整验签，不能跳过签名结果比对。
         * 参见详细设计「3. API/V1 接入层」。
         */
        V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> precheck = precheckService.precheckCreate(
                command.rawBody(),
                new V1MerchantSecurityProfile(merchant.appId(), merchant.orgCode(), merchant.aesKey(), command.signingCredential()),
                command.loadTestHeader());
        if (!precheck.valid()) {
            return Preparation.error(protocolFailure(precheck.reason()));
        }

        V1RechargeCreateOrderRequest request = precheck.payload();
        String syntaxError = validatePayload(request);
        if (syntaxError != null) {
            return Preparation.error(V1CardApiResultStatus.PARAM_ERROR.result(syntaxError));
        }
        String notifyUrl = normalizeNotifyUrl(request.notifyUrl());

        V1ProductSelection productSelection;
        try {
            productSelection = productCandidateSelector.select(
                    merchant.orgCode(),
                    request.productId(),
                    request.account());
            /*
             * 参见详细设计「6. Redis 受理与缓存层」：父商品选品快照已经由缓存选择器完成逻辑过期
             * 和单飞刷新。接单 Lua 前再执行 compare-and-repair，仅当 Redis 父快照仍匹配当前请求时
             * 修复实际子商品配置，避免旧请求把新价格覆盖回旧值。
             */
            if (productSelection.productSnapshotAvailable()) {
                productConfigPreheatPort.refreshIfMissingOrStale(merchant.orgCode(), productSelection);
            }
        } catch (V1ProductCacheUnavailableException ex) {
            logProductCacheUnavailable(merchant.orgCode(), request.productId(), ex);
            return Preparation.error(V1CardApiResultStatus.SYSTEM_BUSY.result("系统繁忙"));
        }
        BusinessFailure businessFailure = firstBusinessFailure(merchant, request, productSelection);
        ValidationStatus validationStatus = businessFailure == null ? productSelection.validationStatus() : ValidationStatus.FAILED;
        String candidateFailCode = businessFailure == null ? productSelection.failCode() : businessFailure.failCode();
        String candidateFailReason = businessFailure == null ? productSelection.failReason() : businessFailure.failReason();
        var requestHash = requestHashCanonicalizer.canonicalizeRechargeCreate(
                new RequestHashCanonicalizer.RechargeCreateFields(
                        merchant.orgCode(),
                        request.productId(),
                        request.outOrderSn(),
                        request.account(),
                        notifyUrl,
                        request.num()));
        long now = clock.instant().toEpochMilli();
        AcceptRechargeOrderCommand acceptCommand = new AcceptRechargeOrderCommand(
                merchant.orgCode(),
                merchant.appId(),
                request.outOrderSn(),
                orderIdGenerator.nextOrderId(),
                request.productId(),
                productSelection.selectedProductId(),
                request.account(),
                notifyUrl,
                request.nonceStr(),
                request.num(),
                productSelection.salePriceCent(),
                productSelection.productName(),
                productSelection.faceValueCent(),
                productSelection.productType(),
                productSelection.couponType(),
                productSelection.supplierChannel(),
                requestHash.value(),
                requestHash.version(),
                requestHash.algorithm(),
                validationStatus,
                candidateFailCode,
                candidateFailReason,
                merchant.configVersion(),
                productSelection.productConfigVersion(),
                productSelection.cacheFingerprint(),
                productSelection.productSnapshotAvailable(),
                now,
                now + settings.firstPersistRetryDelay().toMillis()).withSupplierBatchId(productSelection.supplierBatchId());
        return Preparation.ready(acceptCommand, request.outOrderSn());
    }

    private static void logProductCacheUnavailable(
            String orgCode,
            String productId,
            V1ProductCacheUnavailableException ex) {
        /*
         * 商品缓存故障位于下单热路径，硬过期期间并发请求可能同时失败。这里按 10 秒采样 error，
         * 保留首个异常栈和非敏感业务定位字段，同时避免 Redis/MySQL 故障放大成日志 I/O 风暴。
         * 参见详细设计「12. 可观测性」。
         */
        if (PRODUCT_CACHE_ERROR_LOG_GUARD.shouldLog(LOG_EVENT_PRODUCT_CACHE_UNAVAILABLE).shouldLog()) {
            log.error("V1商品缓存刷新或配置修复不可用，Redis接单前拒绝 orgCode={}, productId={}, sampleIntervalSeconds={}",
                    orgCode,
                    productId,
                    PRODUCT_CACHE_ERROR_LOG_INTERVAL.toSeconds(),
                    ex);
        }
    }

    private record Preparation(V1CardApiResult errorResult, AcceptRechargeOrderCommand acceptCommand, String outOrderSn) {
        static Preparation error(V1CardApiResult result) {
            return new Preparation(result, null, null);
        }
        static Preparation ready(AcceptRechargeOrderCommand command, String outOrderSn) {
            return new Preparation(null, command, outOrderSn);
        }
    }

    private static BusinessFailure firstBusinessFailure(
            V1MerchantProfile merchant,
            V1RechargeCreateOrderRequest request,
            V1ProductSelection productSelection) {
        if (!merchant.enabled()) {
            return new BusinessFailure(FAIL_CODE_MERCHANT_DISABLED, "商户已停用");
        }
        if (request.num() == null || request.num() != DIRECT_RECHARGE_ORDER_NUM) {
            return new BusinessFailure(FAIL_CODE_INVALID_NUM, "直充产品仅支持单数量下单！");
        }
        if (productSelection.validationStatus() == ValidationStatus.FAILED) {
            return new BusinessFailure(productSelection.failCode(), productSelection.failReason());
        }
        return null;
    }

    private static String validatePayload(V1RechargeCreateOrderRequest request) {
        if (!StringUtils.hasText(request.outOrderSn())
                || !StringUtils.hasText(request.productId())
                || !StringUtils.hasText(request.account())
                || !StringUtils.hasText(request.nonceStr())
                || request.num() == null) {
            return "参数错误";
        }
        if (request.nonceStr().length() != NONCE_STR_LENGTH) {
            return "nonce_str长度不正确";
        }
        if (StringUtils.hasText(request.notifyUrl()) && !isHttpUrl(request.notifyUrl())) {
            return "notify_url格式不正确";
        }
        return null;
    }

    private static String normalizeNotifyUrl(String value) {
        return StringUtils.hasText(value) ? value : "";
    }

    private static boolean isHttpUrl(String value) {
        try {
            URI uri = URI.create(value);
            return HTTP_SCHEME.equalsIgnoreCase(uri.getScheme()) || HTTPS_SCHEME.equalsIgnoreCase(uri.getScheme());
        } catch (IllegalArgumentException ex) {
            /*
             * 这是商户请求参数校验路径，非法 URL 应返回 PARAM_ERROR，不应进入系统故障告警。
             * 参见 docs/交行直充业务系统详细设计.md「3. API/V1 接入层」的协议预检边界。
             */
            log.debug("V1直充notify_url格式非法 notifyUrlLength={}", value == null ? 0 : value.length(), ex);
            return false;
        }
    }

    private static V1CardApiResult protocolFailure(String reason) {
        return switch (reason) {
            case V1ProtocolFailureReason.SIGN_FAILED -> V1CardApiResultStatus.SIGN_FAILED.result("验签不合规！");
            case V1ProtocolFailureReason.DECRYPT_FAILED -> V1CardApiResultStatus.SIGN_FAILED.result();
            case V1ProtocolFailureReason.TIMESTAMP_OUT_OF_RANGE ->
                    V1CardApiResultStatus.PARAM_ERROR.result("timestamp超出允许范围");
            default -> V1CardApiResultStatus.PARAM_ERROR.result();
        };
    }

    private static V1CardApiResult toV1Result(String outOrderSn, AcceptRechargeOrderResult result) {
        AcceptRechargeOrderStatus status = result.status();
        if (status == AcceptRechargeOrderStatus.ACCEPTED) {
            return V1CardApiResultStatus.SUCCESS.result(Map.of("order_sn", result.orderId(), "out_order_sn", outOrderSn));
        }
        if (status == AcceptRechargeOrderStatus.ACCEPTED_FAILED) {
            return businessStatus(result.failCode()).result(defaultText(result.failReason(), "订单失败"));
        }
        if (status == AcceptRechargeOrderStatus.DUPLICATE) {
            return V1CardApiResultStatus.ORDER_DUPLICATE.result(defaultText(result.failReason(), "订单号重复"));
        }
        if (status == AcceptRechargeOrderStatus.NONCE_REPLAY) {
            return V1CardApiResultStatus.SIGN_FAILED.result(defaultText(result.failReason(), "请求随机串重复"));
        }
        return V1CardApiResultStatus.SYSTEM_BUSY.result(defaultText(result.failReason(), "系统繁忙，请稍后重试"));
    }

    private CompletionStage<V1CardApiResult> toWaitFailureResult(
            Preparation preparation,
            AcceptRechargeOrderResult acceptResult,
            ReplicationWaitResult waitResult,
            CancelUnconfirmedAcceptResult cancelResult) {
        /*
         * cancel 成功代表 Redis 已确认释放余额并清理 pending，此时旧协议只能返回系统繁忙让调用方重试；
         * cancel 失败则不能假定订单未受理，必须进入“稍后查单 + anomaly 核查”保守语义。
         */
        if (cancelResult.rollbackConfirmed()) {
            return CompletableFuture.completedFuture(V1CardApiResultStatus.SYSTEM_BUSY.result());
        }
        /*
         * anomaly 写库是旁路证据留存，不能参与 HTTP 响应耗时；否则 tradeDbTaskExecutor 或 MySQL 抖动会把
         * “稍后查单”的保守响应拖到调用方超时。参见详细设计「6. Redis 受理与缓存层」。
         */
        closeRedisWriteForUnknownAccept(preparation.acceptCommand, acceptResult, waitResult, cancelResult);
        recordWaitCancelFailedAnomalyAsync(preparation.acceptCommand, acceptResult, waitResult, cancelResult);
        return CompletableFuture.completedFuture(processingResult(preparation.outOrderSn, acceptResult));
    }

    /**
     * WAIT 未达标且 cancel 失败时关闭 Redis 写入。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」和
     * 「4. Application 用例编排层」。此时不能确认 Redis 主从切换后订单、余额预扣和 pending 是否仍可见；
     * 继续接单会扩大待核查窗口，所以必须先 fail closed，再由 anomaly 自动核查或人工处理决定恢复。
     */
    private void closeRedisWriteForUnknownAccept(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            ReplicationWaitResult waitResult,
            CancelUnconfirmedAcceptResult cancelResult) {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("org_code", command.orgCode());
        context.put("out_order_sn", command.outOrderSn());
        context.put("order_id", acceptResult.orderId());
        context.put("request_hash", command.requestHash());
        context.put("wait_reason", defaultText(waitResult.reason(), "WAIT_NOT_CONFIRMED"));
        context.put("cancel_fail_code", cancelResult.failCode());
        context.put("cancel_fail_reason", cancelResult.failReason());
        try {
            /*
             * RedisHaGuardPort.closeWrite 当前必须是本机内存或等价的快速操作，不能在 HTTP 响应路径同步做
             * Nacos/Sentinel/DB 等网络 I/O；若后续实现需要远程持久化关闭状态，应在端口内部异步化或引入独立任务。
             * 参见 docs/交行直充业务系统详细设计.md「6.3 Redis HA Guard 与状态未知窗口」。
             */
            redisHaGuardPort.closeWrite("WAIT_FAILED_CANCEL_FAILED", context);
        } catch (RuntimeException ex) {
            /*
             * HA Guard 关闭失败属于严重可观测问题，但不能改变当前请求的保守响应；调用方仍会收到
             * “稍后查单”，后续 readiness/日志告警必须暴露该故障。
             */
            log.error("V1直充Redis HA Guard关闭写入失败 orgCode={}, outOrderSn={}, orderId={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    acceptResult.orderId(),
                    ex);
        }
    }

    /**
     * 将 MyBatis anomaly 写库隔离到 tradeDbTaskExecutor。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
     * Redis WAIT/cancel 的 CompletionStage 可能由 Lettuce/Redis 回调线程完成，不能在该线程上同步写 MySQL；
     * 因此这里显式切换到交易 DB executor，并把调度失败也降级为告警，保持接口响应语义不变。
     */
    private void recordWaitCancelFailedAnomalyAsync(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            ReplicationWaitResult waitResult,
            CancelUnconfirmedAcceptResult cancelResult) {
        try {
            CompletableFuture
                    .runAsync(
                            () -> recordWaitCancelFailedAnomaly(command, acceptResult, waitResult, cancelResult),
                            tradeDbTaskExecutor)
                    .exceptionally(ex -> {
                        log.error("V1直充接单异常登记任务执行失败 orgCode={}, outOrderSn={}, orderId={}",
                                command.orgCode(),
                                command.outOrderSn(),
                                acceptResult.orderId(),
                                ex);
                        return null;
                    });
        } catch (RuntimeException ex) {
            log.error("V1直充接单异常登记任务调度失败 orgCode={}, outOrderSn={}, orderId={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    acceptResult.orderId(),
                    ex);
        }
    }

    /**
     * WAIT 未达标且 cancel 也未确认时，Redis 中可能仍保留订单、余额预扣和 pending 索引。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
     * 这里登记 `recharge_accept_anomaly` 是为了给后续自动核查任务留下入口；登记失败不能改变接口响应，
     * 因为调用方已经只能得到“稍后查单”的保守语义。
     */
    private void recordWaitCancelFailedAnomaly(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            ReplicationWaitResult waitResult,
            CancelUnconfirmedAcceptResult cancelResult) {
        AcceptAnomalyRecord record = new AcceptAnomalyRecord(
                acceptAnomalyId(AcceptAnomalyType.WAIT_CANCEL_FAILED, command, acceptResult),
                AcceptAnomalyType.WAIT_CANCEL_FAILED,
                command.orgCode(),
                command.outOrderSn(),
                acceptResult.orderId(),
                command.requestHash(),
                "",
                acceptAnomalyContext(command, acceptResult, waitResult, cancelResult),
                defaultText(cancelResult.failReason(), cancelResult.failCode()));
        try {
            acceptAnomalyRecordPort.record(record);
        } catch (RuntimeException ex) {
            log.error("V1直充接单异常登记失败 orgCode={}, outOrderSn={}, orderId={}, anomalyId={}, failCode={}, failReason={}",
                    command.orgCode(),
                    command.outOrderSn(),
                    acceptResult.orderId(),
                    record.anomalyId(),
                    cancelResult.failCode(),
                    cancelResult.failReason(),
                    ex);
        }
    }

    private static String acceptAnomalyId(
            AcceptAnomalyType type,
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult) {
        String prefix = "accept-" + type.name() + "-";
        if (StringUtils.hasText(acceptResult.orderId())) {
            return prefix + acceptResult.orderId();
        }
        /*
         * Lua timeout、Redis BUSY 等状态未知分支可能拿不到 Lua 返回体，也就没有平台订单号。这里使用 request_hash
         * 作为稳定兜底：同一业务意图重复进入异常登记时仍会命中同一个 anomaly_id；同时避免多个订单共同退化成
         * `unknown`。字段长度受 `recharge_accept_anomaly.anomaly_id varchar(100)` 约束，需要做显式截断。
         */
        String requestHashSuffix = "request-" + command.requestHash();
        int maxSuffixLength = MAX_ACCEPT_ANOMALY_ID_LENGTH - prefix.length();
        return prefix + abbreviate(requestHashSuffix, maxSuffixLength);
    }

    private static Map<String, Object> acceptAnomalyContext(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult,
            ReplicationWaitResult waitResult,
            CancelUnconfirmedAcceptResult cancelResult) {
        Map<String, Object> context = new LinkedHashMap<>();
        // 业务定位维度：后续核查任务优先用这些字段回查 Redis、MySQL 和 MQ 消息轨迹。
        context.put("org_code", command.orgCode());
        context.put("out_order_sn", command.outOrderSn());
        context.put("order_id", acceptResult.orderId());
        context.put("request_hash", command.requestHash());
        // Redis accept Lua 结果：用于判断是否已经写入订单快照、是否已经预扣余额。
        context.put("accept_status", acceptResult.status().name());
        context.put("validation_status", acceptResult.validationStatus());
        context.put("accept_fail_code", acceptResult.failCode());
        context.put("accept_fail_reason", acceptResult.failReason());
        context.put("order_version", acceptResult.orderVersion());
        // JSON 中这里保留 boolean 类型；D5B 核查任务读取 raw_context 时不能把这些字段按字符串解析。
        context.put("balance_reserved", acceptResult.balanceReserved());
        context.put("dispatch_allowed", acceptResult.dispatchAllowed());
        // WAIT 与 cancel 结果：用于区分复制未达标、回滚失败、状态未知等恢复分支。
        context.put("wait_replica_count", waitResult.replicaCount());
        context.put("wait_reason", defaultText(waitResult.reason(), "WAIT_NOT_CONFIRMED"));
        context.put("cancel_fail_code", cancelResult.failCode());
        context.put("cancel_fail_reason", cancelResult.failReason());
        return context;
    }

    private static V1CardApiResult toMarkQueuedResult(
            String outOrderSn,
            AcceptRechargeOrderResult acceptResult,
            MarkPersistQueuedResult markResult) {
        return markResult.marked() ? toV1Result(outOrderSn, acceptResult) : processingResult(outOrderSn, acceptResult);
    }

    private static V1CardApiResult processingResult(String outOrderSn, AcceptRechargeOrderResult acceptResult) {
        return V1CardApiResultStatus.SYSTEM_BUSY.result(
                "系统繁忙，请稍后查询订单状态",
                Map.of(
                        "out_order_sn", outOrderSn,
                        "order_status", Integer.toString(LegacyOrderStatusCode.ACCEPTED.code())));
    }

    private static V1CardApiResultStatus businessStatus(String failCode) {
        return switch (defaultText(failCode, "")) {
            case FAIL_CODE_BALANCE_NOT_ENOUGH, FAIL_CODE_MERCHANT_DISABLED ->
                    V1CardApiResultStatus.BALANCE_OR_MERCHANT_FAILED;
            default -> V1CardApiResultStatus.BUSINESS_FAILED;
        };
    }

    private static String defaultText(String value, String fallback) {
        return StringUtils.hasText(value) ? value : fallback;
    }

    private static String abbreviate(String value, int maxLength) {
        if (maxLength <= 0) {
            return "";
        }
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }

    private record BusinessFailure(String failCode, String failReason) {
    }

    /**
     * 旧构造器的兼容默认值，仅用于测试或未装配 D4 可靠性端口时保持已有行为。
     * 生产 Bean 会通过完整构造器注入真实 WAIT/cancel/mark 实现。
     */
    private static final class NoopRechargeAcceptReliabilityPort implements RechargeAcceptReliabilityPort {

        @Override
        public CompletionStage<ReplicationWaitResult> waitForReplication(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult) {
            return CompletableFuture.completedFuture(ReplicationWaitResult.confirmed(1));
        }

        @Override
        public CompletionStage<CancelUnconfirmedAcceptResult> cancelUnconfirmedAccept(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String reason) {
            return CompletableFuture.completedFuture(CancelUnconfirmedAcceptResult.success());
        }

        @Override
        public CompletionStage<MarkPersistQueuedResult> markPersistQueued(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String persistMessageId,
                int publishCount) {
            return CompletableFuture.completedFuture(MarkPersistQueuedResult.success());
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

    private static final class NoopPersistRechargeOrderMessagePublisher implements PersistRechargeOrderMessagePublisher {

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public CompletionStage<PersistMessagePublishResult> publish(PersistTradeOrderMessage message) {
            return CompletableFuture.completedFuture(PersistMessagePublishResult.confirmed(message.messageId(), 1));
        }
    }
}
