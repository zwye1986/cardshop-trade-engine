package com.cardshop.trade.engine.api.v1.usecase;

import com.cardshop.trade.engine.api.v1.dto.V1RechargeCreateOrderRequest;
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
import com.cardshop.trade.engine.application.v1.CreateRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.RechargeOrderIdGenerator;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.application.v1.V1CardApiResultStatus;
import com.cardshop.trade.engine.application.v1.V1MerchantCommand;
import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.application.v1.V1ProductCandidateSelector;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.domain.service.RequestHashCanonicalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * V1 创建直充订单新链路用例。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」、
 * 「20.13.5 商品选择规则」和「20.13.6 Redis Lua 下单结果」。该用例完成协议预检、request_hash、
 * Java 商品候选校验、Redis-first 原子接单、Redis WAIT 和 RabbitMQ publisher confirm。
 * MySQL 落库仍由后续 Consumer 异步完成，本用例只保证接口返回前订单进入可恢复的 Redis/MQ 通道。
 */
public class V1CreateRechargeOrderUseCaseService implements CreateRechargeOrderUseCase {

    private static final Logger log = LoggerFactory.getLogger(V1CreateRechargeOrderUseCaseService.class);
    private static final String PERSIST_STATUS_PENDING_MQ = "PENDING_MQ";
    private static final int MAX_ACCEPT_ANOMALY_ID_LENGTH = 100;

    private final V1ProtocolPrecheckService precheckService;
    private final RequestHashCanonicalizer requestHashCanonicalizer;
    private final V1MerchantProfilePort merchantProfilePort;
    private final V1ProductCandidateSelector productCandidateSelector;
    private final AcceptRechargeOrderPort acceptRechargeOrderPort;
    private final RechargeAcceptReliabilityPort reliabilityPort;
    private final PersistRechargeOrderMessagePublisher persistMessagePublisher;
    private final AcceptAnomalyRecordPort acceptAnomalyRecordPort;
    private final RedisHaGuardPort redisHaGuardPort;
    private final RechargeOrderIdGenerator orderIdGenerator;
    private final Clock clock;
    private final V1RechargeAcceptSettings settings;
    private final Executor tradeDbTaskExecutor;

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
                orderIdGenerator,
                clock,
                settings,
                tradeDbTaskExecutor);
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
                orderIdGenerator,
                clock,
                settings,
                tradeDbTaskExecutor);
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
                orderIdGenerator,
                clock,
                settings,
                tradeDbTaskExecutor);
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
        this.precheckService = precheckService;
        this.requestHashCanonicalizer = requestHashCanonicalizer;
        this.merchantProfilePort = merchantProfilePort;
        this.productCandidateSelector = productCandidateSelector;
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
    }

    @Override
    public CompletionStage<V1CardApiResult> createRechargeOrder(V1MerchantCommand command) {
        return CompletableFuture
                .supplyAsync(() -> prepare(command), tradeDbTaskExecutor)
                .thenCompose(preparation -> preparation.errorResult != null
                        ? CompletableFuture.completedFuture(preparation.errorResult)
                        : acceptAfterDependencyGate(preparation));
    }

    /**
     * Redis HA Guard 和 RabbitMQ publisher breaker 必须在 Redis accept Lua 之前检查。
     *
     * <p>参见详细设计「20.5 下单服务实现细节」。如果 Redis 主从切换/修复窗口或 MQ 全局不可用时仍继续写
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
        return acceptRechargeOrderPort.accept(preparation.acceptCommand)
                .thenCompose(acceptResult -> afterRedisAccept(preparation, acceptResult));
    }

    private CompletionStage<V1CardApiResult> afterRedisAccept(
            Preparation preparation,
            AcceptRechargeOrderResult acceptResult) {
        if (!acceptResult.orderWritten()) {
            return CompletableFuture.completedFuture(toV1Result(preparation.outOrderSn, acceptResult));
        }
        return reliabilityPort.waitForReplication(preparation.acceptCommand, acceptResult)
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
        return persistMessagePublisher.publish(message)
                .thenCompose(publishResult -> publishResult.confirmed()
                        ? markPersistQueued(preparation, acceptResult, publishResult)
                        : CompletableFuture.completedFuture(processingResult(preparation.outOrderSn, acceptResult)));
    }

    private CompletionStage<V1CardApiResult> markPersistQueued(
            Preparation preparation,
            AcceptRechargeOrderResult acceptResult,
            PersistMessagePublishResult publishResult) {
        return reliabilityPort
                .markPersistQueued(
                        preparation.acceptCommand,
                        acceptResult,
                        publishResult.messageId(),
                        publishResult.attemptCount())
                .thenApply(markResult -> toMarkQueuedResult(preparation.outOrderSn, acceptResult, markResult));
    }

    private PersistTradeOrderMessage buildPersistMessage(
            AcceptRechargeOrderCommand command,
            AcceptRechargeOrderResult acceptResult) {
        String messageId = "persist-" + command.orderId() + "-" + UUID.randomUUID();
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
                Map.entry("validation_status", acceptResult.validationStatus()),
                Map.entry("fail_code", acceptResult.failCode()),
                Map.entry("fail_reason", acceptResult.failReason()),
                Map.entry("dispatch_allowed", acceptResult.dispatchAllowed()),
                Map.entry("balance_reserved", acceptResult.balanceReserved()),
                Map.entry("persist_status", PERSIST_STATUS_PENDING_MQ),
                Map.entry("order_version", acceptResult.orderVersion()),
                Map.entry("request_hash", command.requestHash()));
        return new PersistTradeOrderMessage(
                messageId,
                command.orgCode(),
                acceptResult.orderId(),
                command.outOrderSn(),
                command.requestHash(),
                Math.max(1L, acceptResult.orderVersion()),
                snapshot);
    }

    private Preparation prepare(V1MerchantCommand command) {
        Optional<V1MerchantProfile> maybeMerchant = merchantProfilePort.findByAppIdAndOrgCode(command.appId(), command.orgCode());
        if (maybeMerchant.isEmpty() || !StringUtils.hasText(maybeMerchant.get().aesKey())) {
            log.warn("V1直充下单商户资料缺失 appId={}, orgCode={}", command.appId(), command.orgCode());
            return Preparation.error(V1CardApiResultStatus.AUTH_FAILED.result());
        }

        V1MerchantProfile merchant = maybeMerchant.get();
        V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> precheck = precheckService.precheckCreate(
                command.rawBody(),
                new V1MerchantSecurityProfile(merchant.appId(), merchant.orgCode(), merchant.aesKey()));
        if (!precheck.valid()) {
            return Preparation.error(protocolFailure(precheck.reason()));
        }

        V1RechargeCreateOrderRequest request = precheck.payload();
        String syntaxError = validatePayload(request);
        if (syntaxError != null) {
            return Preparation.error(V1CardApiResultStatus.PARAM_ERROR.result(syntaxError));
        }

        V1ProductSelection productSelection = productCandidateSelector.select(
                merchant.orgCode(),
                request.productId(),
                request.account());
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
                        request.notifyUrl(),
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
                request.notifyUrl(),
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
                now,
                now + settings.firstPersistRetryDelay().toMillis());
        return Preparation.ready(acceptCommand, request.outOrderSn());
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
            return new BusinessFailure("MERCHANT_DISABLED", "商户已停用");
        }
        if (!Integer.valueOf(1).equals(request.num())) {
            return new BusinessFailure("INVALID_NUM", "直充产品仅支持单数量下单！");
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
        if (request.nonceStr().length() != 32) {
            return "nonce_str长度不正确";
        }
        if (!StringUtils.hasText(request.notifyUrl()) || !isHttpUrl(request.notifyUrl())) {
            return "notify_url格式不正确";
        }
        return null;
    }

    private static boolean isHttpUrl(String value) {
        try {
            URI uri = URI.create(value);
            return "http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme());
        } catch (IllegalArgumentException ex) {
            /*
             * 这是商户请求参数校验路径，非法 URL 应返回 PARAM_ERROR，不应进入系统故障告警。
             * 参见 docs/交行直充业务系统详细设计.md「20.13.3 SM4 解密与 SM3 验签」的协议预检边界。
             */
            log.debug("V1直充notify_url格式非法 notifyUrlLength={}", value == null ? 0 : value.length(), ex);
            return false;
        }
    }

    private static V1CardApiResult protocolFailure(String reason) {
        return switch (reason) {
            case "SIGN_FAILED" -> V1CardApiResultStatus.SIGN_FAILED.result("验签不合规！");
            case "DECRYPT_FAILED" -> V1CardApiResultStatus.SIGN_FAILED.result();
            case "TIMESTAMP_OUT_OF_RANGE" -> V1CardApiResultStatus.PARAM_ERROR.result("timestamp超出允许范围");
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
         * “稍后查单”的保守响应拖到调用方超时。参见详细设计「20.13.6 Redis Lua 下单结果」。
         */
        closeRedisWriteForUnknownAccept(preparation.acceptCommand, acceptResult, waitResult, cancelResult);
        recordWaitCancelFailedAnomalyAsync(preparation.acceptCommand, acceptResult, waitResult, cancelResult);
        return CompletableFuture.completedFuture(processingResult(preparation.outOrderSn, acceptResult));
    }

    /**
     * WAIT 未达标且 cancel 失败时关闭 Redis 写入。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 Redis HA Guard」和
     * 「20.5 下单服务实现细节」。此时不能确认 Redis 主从切换后订单、余额预扣和 pending 是否仍可见；
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
             * 参见 docs/交行直充业务系统详细设计.md「9.3 Redis HA Guard」。
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
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
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
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
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
                Map.of("out_order_sn", outOrderSn, "order_status", "2"));
    }

    private static V1CardApiResultStatus businessStatus(String failCode) {
        return switch (defaultText(failCode, "")) {
            case "BALANCE_NOT_ENOUGH", "MERCHANT_DISABLED" -> V1CardApiResultStatus.BALANCE_OR_MERCHANT_FAILED;
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
