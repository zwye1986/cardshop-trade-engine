package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL 派单 outbox 履约 Worker。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」、
 * 「9.3 作废设计」和「9.4 回调设计」。本服务只扫描履约事件
 * （{@code SUPPLIER_DISPATCH}/{@code SUPPLIER_QUERY}/{@code INVALID_ORDER}），不处理
 * {@code BALANCE_DELTA_FLUSH}；余额释放只通过 {@link BalanceDeltaFlushOutboxPort} 写入
 * {@code recharge_balance_delta_outbox}（详细设计「9.2 余额 Delta 刷账 Outbox」）。
 * 具体 MySQL、Redis 和 HTTP 实现都在基础设施层。
 */
public class RechargeOutboxWorkerService {

    private static final Logger log = LoggerFactory.getLogger(RechargeOutboxWorkerService.class);

    private static final int ORDER_STATUS_ACCEPTED = LegacyOrderStatusCode.ACCEPTED.code();
    private static final int ORDER_STATUS_FAILED = LegacyOrderStatusCode.FAILED.code();
    private static final int ORDER_STATUS_SUCCESS = LegacyOrderStatusCode.SUCCESS.code();
    private static final int ORDER_STATUS_SUPPLIER_ACCEPTED = LegacyOrderStatusCode.SUPPLIER_ACCEPTED.code();
    private static final String DATA_STATUS_PROCESSING = "PROCESSING";
    private static final String DATA_STATUS_PROCESSED = "PROCESSED";
    private static final String DATA_STATUS_FREQUENCY_WAIT = "FREQUENCY_WAIT";
    private static final int DEFAULT_NOTIFY_MAX_RETRY_COUNT = 10;
    private static final int MIN_PARALLELISM = 1;
    private static final int MAX_PARALLELISM = 32;
    private static final int MAX_SCAN_LIMIT = 1000;
    private static final char ACCOUNT_KEY_SEPARATOR = '\0';

    private final java.util.function.Supplier<MinisoWriteOffProtection> minisoWriteOffProtection;
    private final com.cardshop.trade.engine.application.logging.LogStormGuard errorLogs =
            com.cardshop.trade.engine.application.logging.LogStormGuard.defaultGuard();

    private final RechargeOutboxPort outboxPort;
    private final RechargeFulfillmentOrderPort orderPort;
    private final RechargeSupplierGatewayPort supplierGatewayPort;
    private final SupplierQueryOutboxPort supplierQueryOutboxPort;
    private final DirectRechargeFrequencyPort frequencyPort;
    private final BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort;
    private final BalanceReservationReleasePort balanceReservationReleasePort;
    private final RechargeNotifyTaskPort notifyTaskPort;
    private final MerchantNotifyPayloadSigner notifyPayloadSigner;
    private final RechargeDispatchTimerPort dispatchTimerPort;
    private final Clock clock;
    private final RechargeOutboxWorkerSettings settings;

    /**
     * 保留旧构造签名供已有 slice/unit test 使用；真实履约 Bean 必须注入查单 outbox 端口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」：如果该兼容构造器被用于真实
     * 运行时，供应商处理中会显式失败并进入派单事件重试，不会静默丢失查单事件。
     */
    public RechargeOutboxWorkerService(
            RechargeOutboxPort outboxPort,
            RechargeFulfillmentOrderPort orderPort,
            RechargeSupplierGatewayPort supplierGatewayPort,
            DirectRechargeFrequencyPort frequencyPort,
            BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort,
            BalanceReservationReleasePort balanceReservationReleasePort,
            RechargeNotifyTaskPort notifyTaskPort,
            MerchantNotifyPayloadSigner notifyPayloadSigner,
            Clock clock,
            RechargeOutboxWorkerSettings settings) {
        this(
                outboxPort,
                orderPort,
                supplierGatewayPort,
                command -> {
                    throw new IllegalStateException("SupplierQueryOutboxPort is required for processing orders");
                },
                frequencyPort,
                balanceDeltaFlushOutboxPort,
                balanceReservationReleasePort,
                notifyTaskPort,
                notifyPayloadSigner,
                RechargeDispatchTimerPort.noop(),
                clock,
                settings);
    }

    public RechargeOutboxWorkerService(
            RechargeOutboxPort outboxPort,
            RechargeFulfillmentOrderPort orderPort,
            RechargeSupplierGatewayPort supplierGatewayPort,
            DirectRechargeFrequencyPort frequencyPort,
            BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort,
            BalanceReservationReleasePort balanceReservationReleasePort,
            RechargeNotifyTaskPort notifyTaskPort,
            MerchantNotifyPayloadSigner notifyPayloadSigner,
            Clock clock,
            RechargeOutboxWorkerSettings settings,
            RechargeDispatchTimerPort dispatchTimerPort) {
        this(
                outboxPort,
                orderPort,
                supplierGatewayPort,
                command -> {
                    throw new IllegalStateException("SupplierQueryOutboxPort is required for processing orders");
                },
                frequencyPort,
                balanceDeltaFlushOutboxPort,
                balanceReservationReleasePort,
                notifyTaskPort,
                notifyPayloadSigner,
                dispatchTimerPort,
                clock,
                settings);
    }

    public RechargeOutboxWorkerService(
            RechargeOutboxPort outboxPort,
            RechargeFulfillmentOrderPort orderPort,
            RechargeSupplierGatewayPort supplierGatewayPort,
            SupplierQueryOutboxPort supplierQueryOutboxPort,
            DirectRechargeFrequencyPort frequencyPort,
            BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort,
            BalanceReservationReleasePort balanceReservationReleasePort,
            RechargeNotifyTaskPort notifyTaskPort,
            MerchantNotifyPayloadSigner notifyPayloadSigner,
            Clock clock,
            RechargeOutboxWorkerSettings settings) {
        this(
                outboxPort,
                orderPort,
                supplierGatewayPort,
                supplierQueryOutboxPort,
                frequencyPort,
                balanceDeltaFlushOutboxPort,
                balanceReservationReleasePort,
                notifyTaskPort,
                notifyPayloadSigner,
                RechargeDispatchTimerPort.noop(),
                clock,
                settings);
    }

    public RechargeOutboxWorkerService(
            RechargeOutboxPort outboxPort,
            RechargeFulfillmentOrderPort orderPort,
            RechargeSupplierGatewayPort supplierGatewayPort,
            SupplierQueryOutboxPort supplierQueryOutboxPort,
            DirectRechargeFrequencyPort frequencyPort,
            BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort,
            BalanceReservationReleasePort balanceReservationReleasePort,
            RechargeNotifyTaskPort notifyTaskPort,
            MerchantNotifyPayloadSigner notifyPayloadSigner,
            RechargeDispatchTimerPort dispatchTimerPort,
            Clock clock,
            RechargeOutboxWorkerSettings settings) {
        this(
                outboxPort,
                orderPort,
                supplierGatewayPort,
                supplierQueryOutboxPort,
                frequencyPort,
                balanceDeltaFlushOutboxPort,
                balanceReservationReleasePort,
                notifyTaskPort,
                notifyPayloadSigner,
                dispatchTimerPort,
                clock,
                settings,
                () -> null);
    }

    public RechargeOutboxWorkerService(
            RechargeOutboxPort outboxPort,
            RechargeFulfillmentOrderPort orderPort,
            RechargeSupplierGatewayPort supplierGatewayPort,
            SupplierQueryOutboxPort supplierQueryOutboxPort,
            DirectRechargeFrequencyPort frequencyPort,
            BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort,
            BalanceReservationReleasePort balanceReservationReleasePort,
            RechargeNotifyTaskPort notifyTaskPort,
            MerchantNotifyPayloadSigner notifyPayloadSigner,
            RechargeDispatchTimerPort dispatchTimerPort,
            Clock clock,
            RechargeOutboxWorkerSettings settings,
            java.util.function.Supplier<MinisoWriteOffProtection> minisoWriteOffProtection) {
        this.minisoWriteOffProtection = Objects.requireNonNull(minisoWriteOffProtection);
        this.outboxPort = Objects.requireNonNull(outboxPort, "outboxPort must not be null");
        this.orderPort = Objects.requireNonNull(orderPort, "orderPort must not be null");
        this.supplierGatewayPort = Objects.requireNonNull(supplierGatewayPort, "supplierGatewayPort must not be null");
        this.supplierQueryOutboxPort = Objects.requireNonNull(
                supplierQueryOutboxPort, "supplierQueryOutboxPort must not be null");
        this.frequencyPort = Objects.requireNonNull(frequencyPort, "frequencyPort must not be null");
        this.balanceDeltaFlushOutboxPort = Objects.requireNonNull(
                balanceDeltaFlushOutboxPort, "balanceDeltaFlushOutboxPort must not be null");
        this.balanceReservationReleasePort = Objects.requireNonNull(
                balanceReservationReleasePort, "balanceReservationReleasePort must not be null");
        this.notifyTaskPort = Objects.requireNonNull(notifyTaskPort, "notifyTaskPort must not be null");
        this.notifyPayloadSigner = Objects.requireNonNull(notifyPayloadSigner, "notifyPayloadSigner must not be null");
        this.dispatchTimerPort = Objects.requireNonNull(dispatchTimerPort, "dispatchTimerPort must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
    }

    public RechargeOutboxWorkerService(
            RechargeOutboxPort outboxPort,
            RechargeFulfillmentOrderPort orderPort,
            RechargeSupplierGatewayPort supplierGatewayPort,
            SupplierQueryOutboxPort supplierQueryOutboxPort,
            DirectRechargeFrequencyPort frequencyPort,
            BalanceDeltaFlushOutboxPort balanceDeltaFlushOutboxPort,
            BalanceReservationReleasePort balanceReservationReleasePort,
            RechargeNotifyTaskPort notifyTaskPort,
            MerchantNotifyPayloadSigner notifyPayloadSigner,
            Clock clock,
            RechargeOutboxWorkerSettings settings,
            RechargeDispatchTimerPort dispatchTimerPort) {
        this(
                outboxPort,
                orderPort,
                supplierGatewayPort,
                supplierQueryOutboxPort,
                frequencyPort,
                balanceDeltaFlushOutboxPort,
                balanceReservationReleasePort,
                notifyTaskPort,
                notifyPayloadSigner,
                dispatchTimerPort,
                clock,
                settings);
    }

    /**
     * 扫描并处理一个 outbox 分片（默认仅 {@link OutboxEventType#SUPPLIER_DISPATCH}）。
     *
     * <p>参见 {@code docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md} 与
     * {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。任何降级、重试或跳过都必须显式回写 outbox，
     * 避免事件长期停留在 PROCESSING。
     */
    public RechargeOutboxBatchResult processDue(int shardTotal, int shardIndex, int limit, String workerId) {
        return processDue(
                shardTotal,
                shardIndex,
                limit,
                workerId,
                List.of(OutboxEventType.SUPPLIER_DISPATCH),
                settings.outboxParallelism());
    }

    /**
     * 按事件类型过滤扫描 outbox，并在 claim 后按配置并行履约。
     *
     * <p>流水线：overfetch 扫描 → 公平/频控预检（仅 DISPATCH）→ 串行 CAS claim → 线程池执行（同账号互斥）。
     * 参见 {@code docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md}「4. 处理流水线」与
     * {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。
     */
    public RechargeOutboxBatchResult processDue(
            int shardTotal,
            int shardIndex,
            int limit,
            String workerId,
            List<OutboxEventType> eventTypes,
            int parallelism) {
        List<OutboxEventType> normalizedEventTypes = normalizeEventTypes(eventTypes);
        int normalizedParallelism = normalizeParallelism(parallelism);
        int scanLimit = Math.min(limit * settings.outboxScanOverfetchFactor(), MAX_SCAN_LIMIT);

        List<RechargeOutboxEvent> events =
                outboxPort.selectDueForShard(shardTotal, shardIndex, scanLimit, normalizedEventTypes);
        RechargeOutboxBatchResult result = RechargeOutboxBatchResult.empty().scanned(events.size());

        FairSelectResult fairSelectResult = fairSelect(events, limit);
        result = result.deferred(fairSelectResult.deferredCount());

        List<RechargeOutboxEvent> claimedEvents = new ArrayList<>();
        for (RechargeOutboxEvent event : fairSelectResult.selected()) {
            if (event.retryExhausted()) {
                outboxPort.markManualRequired(event, retryExhaustedMessage(event));
                result = result.add(RechargeOutboxBatchResult.Outcome.FAILED);
                continue;
            }
            Optional<RechargeOutboxEvent> claimed = outboxPort.claim(event, workerId, settings.claimLease());
            if (claimed.isEmpty()) {
                result = result.add(RechargeOutboxBatchResult.Outcome.SKIPPED);
                continue;
            }
            result = result.claimedOne();
            claimedEvents.add(claimed.get());
        }
        return executeClaimed(claimedEvents, normalizedParallelism, result);
    }

    /**
     * 处理 timer 直接唤醒的单个 outbox 事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。该入口不扫描
     * {@code recharge_dispatch_outbox}，只按稳定 {@code event_id} 查询 due 事件并复用同一套 CAS 抢占和履约
     * 状态机；多节点重复调度时只有抢占成功的一方会真实调用供应商。
     */
    public RechargeOutboxBatchResult.Outcome processEvent(
            String eventId,
            OutboxEventType eventType,
            String workerId) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        Optional<RechargeOutboxEvent> maybeEvent = outboxPort.findDueByEventId(eventId, eventType);
        if (maybeEvent.isEmpty()) {
            // 参见详细设计「9.1 派单 Outbox」：秒精度存储可能晚于首次唤醒，按持久 due 续排而非丢弃。
            Optional<Duration> retryDelay = outboxPort.findPendingRetryDelay(eventId, eventType);
            if (retryDelay.isPresent()) {
                scheduleOutboxTimer(eventId, eventType, retryDelay.get());
                log.debug("供应商履约timer提前唤醒后续排 eventId={}, eventType={}, delay={}",
                        eventId, eventType, retryDelay.get());
                return RechargeOutboxBatchResult.Outcome.DEFERRED;
            }
            return RechargeOutboxBatchResult.Outcome.SKIPPED;
        }
        RechargeOutboxEvent event = maybeEvent.get();
        if (event.retryExhausted()) {
            outboxPort.markManualRequired(event, retryExhaustedMessage(event));
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        RechargeFulfillmentOrder preloadedOrder = event.eventType() == OutboxEventType.SUPPLIER_DISPATCH
                ? loadOrder(event)
                : null;
        if (dispatchOwnedByAnotherSameAccount(event, preloadedOrder)) {
            /*
             * 参见 docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」。timer 单事件热路径必须保持
             * 与扫描路径相同的同账号限频语义：当前账号被其他订单持有时不抢占、不消耗 retry_count，
             * 只重新安排一次本地唤醒；进程中断时仍由 outbox due 扫描兜底。
             */
            scheduleOutboxTimer(event.eventId(), event.eventType(), settings.frequencyRetryDelay());
            return RechargeOutboxBatchResult.Outcome.DEFERRED;
        }
        Optional<RechargeOutboxEvent> claimed = outboxPort.claim(event, workerId, settings.claimLease());
        if (claimed.isEmpty()) {
            return RechargeOutboxBatchResult.Outcome.SKIPPED;
        }
        RechargeOutboxEvent claimedEvent = claimed.get();
        /*
         * 参见 docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」。DISPATCH 的预 claim 订单读取只用于
         * 同账号限频预检；claim 成功后必须重新读取订单事实，避免订单在预检和抢占之间已由查单、人工或重复
         * worker 推进终态时继续使用旧快照调用供应商。
         */
        return handleClaimed(claimedEvent, loadOrder(claimedEvent));
    }

    /**
     * 公平挑选本批可 claim 的事件：DISPATCH 做同账号去重与 Redis 频控预检；QUERY/INVALID 按扫描顺序直接选取。
     *
     * <p>参见 {@code docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md}「公平调度 F3」。
     */
    private FairSelectResult fairSelect(List<RechargeOutboxEvent> events, int limit) {
        List<RechargeOutboxEvent> selected = new ArrayList<>();
        Set<String> selectedAccountKeys = new HashSet<>();
        int deferred = 0;

        for (RechargeOutboxEvent event : events) {
            if (selected.size() >= limit) {
                break;
            }
            if (event.eventType() == OutboxEventType.SUPPLIER_DISPATCH) {
                RechargeFulfillmentOrder order = orderPort.findByOrderId(event.orderId()).orElse(null);
                String orgCode = order != null ? order.orgCode() : event.orgCode();
                String account = order != null ? order.account() : null;
                if (account != null && !account.isBlank()) {
                    String accountKey = accountKey(orgCode, account);
                    if (selectedAccountKeys.contains(accountKey)) {
                        deferred++;
                        continue;
                    }
                    Optional<String> frequencyOwner = frequencyPort.currentOwner(orgCode, account);
                    if (frequencyOwner.isPresent() && !frequencyOwner.get().equals(event.orderId())) {
                        deferred++;
                        continue;
                    }
                    selectedAccountKeys.add(accountKey);
                }
            }
            selected.add(event);
        }
        return new FairSelectResult(selected, deferred);
    }

    /**
     * 执行已 claim 事件：parallelism &gt; 1 时用固定线程池，同 {@code org+account} 在批内串行。
     *
     * <p>参见 {@code docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md}「7. 同账号并行语义」
     * 与详细设计「9.1 派单 Outbox」。
     */
    private RechargeOutboxBatchResult executeClaimed(
            List<RechargeOutboxEvent> claimedEvents, int parallelism, RechargeOutboxBatchResult result) {
        if (claimedEvents.isEmpty()) {
            return result;
        }
        if (parallelism <= MIN_PARALLELISM || claimedEvents.size() == 1) {
            for (RechargeOutboxEvent event : claimedEvents) {
                result = result.add(handleClaimed(event, loadOrder(event)));
            }
            return result;
        }

        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        ConcurrentHashMap<String, Object> accountLocks = new ConcurrentHashMap<>();
        Object noAccountLock = new Object();
        try {
            List<Future<RechargeOutboxBatchResult.Outcome>> futures = new ArrayList<>(claimedEvents.size());
            for (RechargeOutboxEvent event : claimedEvents) {
                futures.add(executor.submit(() -> {
                    RechargeFulfillmentOrder order = loadOrder(event);
                    Object lock = accountLock(order, accountLocks, noAccountLock);
                    synchronized (lock) {
                        return handleClaimed(event, order);
                    }
                }));
            }
            for (Future<RechargeOutboxBatchResult.Outcome> future : futures) {
                result = result.add(future.get());
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("outbox parallel execution interrupted", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause() == null ? ex : ex.getCause();
            throw new IllegalStateException("outbox parallel execution failed", cause);
        } finally {
            executor.shutdown();
        }
        return result;
    }

    /**
     * 加载已 claim 事件的订单快照，供账号锁键计算与履约决策共用，避免执行期重复查询。
     *
     * <p>参见 {@code docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md}「7. 同账号并行语义」。
     */
    private RechargeFulfillmentOrder loadOrder(RechargeOutboxEvent event) {
        return orderPort.findByOrderId(event.orderId()).orElse(null);
    }

    private Object accountLock(
            RechargeFulfillmentOrder order, ConcurrentHashMap<String, Object> accountLocks, Object noAccountLock) {
        if (order == null || order.account() == null || order.account().isBlank()) {
            return noAccountLock;
        }
        return accountLocks.computeIfAbsent(accountKey(order.orgCode(), order.account()), ignored -> new Object());
    }

    private boolean dispatchOwnedByAnotherSameAccount(RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        if (event.eventType() != OutboxEventType.SUPPLIER_DISPATCH
                || order == null
                || !order.dispatchable()
                || order.account() == null
                || order.account().isBlank()) {
            return false;
        }
        return frequencyPort.currentOwner(order.orgCode(), order.account())
                .filter(ownerOrderId -> !ownerOrderId.equals(order.orderId()))
                .isPresent();
    }

    private static String accountKey(String orgCode, String account) {
        return orgCode + ACCOUNT_KEY_SEPARATOR + account;
    }

    private static List<OutboxEventType> normalizeEventTypes(List<OutboxEventType> eventTypes) {
        if (eventTypes == null || eventTypes.isEmpty()) {
            return List.of(OutboxEventType.SUPPLIER_DISPATCH);
        }
        return List.copyOf(eventTypes);
    }

    private int normalizeParallelism(int parallelism) {
        if (parallelism <= 0) {
            return settings.outboxParallelism();
        }
        if (parallelism > MAX_PARALLELISM) {
            return MAX_PARALLELISM;
        }
        return parallelism;
    }

    private record FairSelectResult(List<RechargeOutboxEvent> selected, int deferredCount) {}

    private RechargeOutboxBatchResult.Outcome handleClaimed(RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        try {
            return switch (event.eventType()) {
                case SUPPLIER_DISPATCH -> handleSupplierDispatch(event, order);
                case SUPPLIER_QUERY -> handleSupplierQuery(event, order);
                case INVALID_ORDER -> handleInvalidOrder(event, order);
                default -> {
                    String reason = "unsupported outbox event type: " + event.eventType();
                    markFailedOrManualRequired(event, reason, settings.retryBackoff());
                    yield RechargeOutboxBatchResult.Outcome.FAILED;
                }
            };
        } catch (Exception ex) {
            // 高频重试按类别采样，保留安全分类栈；参见详细设计9.5及日志风暴防护。
            if (errorLogs.shouldLog("outbox.handle." + ex.getClass().getSimpleName()).shouldLog()) {
                log.error("outbox事件处理异常 eventId={}, eventType={}, orderId={}",
                    event.eventId(), event.eventType(), event.orderId(), ex);
            }
            if (ex instanceof MinisoWriteOffException conflict
                    && conflict.reason() == MinisoWriteOffException.Reason.REDEEM_VOID_CONFLICT) {
                blockedByMinisoRedemption(event, order);
                outboxPort.markManualRequired(event, conflict.getMessage());
                return RechargeOutboxBatchResult.Outcome.FAILED;
            }
            if (event.eventType() == OutboxEventType.SUPPLIER_DISPATCH
                    && recoverDispatchExceptionByQuery(event, order, ex)) {
                return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
            }
            markFailedOrManualRequired(event, ex.getMessage(), settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
    }

    private RechargeOutboxBatchResult.Outcome handleSupplierDispatch(
            RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        if (order == null) {
            markFailedOrManualRequired(event, "order not found for dispatch", settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        // 作废属于券终态，任何旧派单事件恢复均不能重派或误发订单失败通知；参见详细设计9.6。
        if (order.cancelled()) return handleInvalidOrder(event, order);
        if (!order.dispatchable()) {
            if (order.terminalBalanceReleasePending()) {
                return completeTerminalBalanceRelease(event, order, "供应商失败终态补偿释放预扣");
            }
            if (order.orderStatus() == ORDER_STATUS_SUCCESS) {
                return completeSuccessfulOrder(event, order);
            }
            if (order.orderStatus() == ORDER_STATUS_SUPPLIER_ACCEPTED) {
                /*
                 * 派单返回处理中后，如果应用在“查单事件插入”和“派单事件成功回写”之间宕机，
                 * 恢复时订单已是供应商受理态。这里再次按稳定 event_id 补写查单事件，避免该窗口丢失后续终态推进。
                 */
                enqueueSupplierQuery(event, order);
            }
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        Optional<SupplierFlowSnapshot> latestFlow = orderPort.findLatestSupplierFlowByOrderId(order.orderId());
        if (latestFlow.isPresent()) {
            /*
             * 参见 docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」。
             * DISPATCH 恢复时只要已经存在同订单供应商流水，就说明请求已经可能发出；不能继续调用 dispatch
             * 制造重复履约，必须先写 supplier-query-{orderId} 让查单链路用同一订单号确认终态。
             */
            return recoverDispatchUnknownByQuery(
                    event,
                    order,
                    latestFlow.get(),
                    "供应商派单恢复发现已有调用流水，转查单确认");
        }
        if (!frequencyPort.acquire(order.orgCode(), order.account(), order.orderId(), settings.frequencyLockTtl())) {
            orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                    order.orderId(),
                    FulfillmentDispatchStatus.FREQUENCY_WAIT,
                    ORDER_STATUS_ACCEPTED,
                    DATA_STATUS_FREQUENCY_WAIT,
                    null,
                    null,
                    null,
                    "同账号存在直充处理中订单，请稍后重试"));
            markFailedOrManualRequired(event, "direct recharge frequency wait", settings.frequencyRetryDelay());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }

        orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                order.orderId(),
                FulfillmentDispatchStatus.DISPATCHING,
                ORDER_STATUS_ACCEPTED,
                DATA_STATUS_PROCESSING,
                null,
                null,
                null,
                "供应商派单中"));
        /*
         * 参见详细设计「9.1 派单 Outbox」：新链路严格使用受理阶段从请求父产品候选中冻结的
         * `selected_product_id/supplier_channel`。供应商业务失败不能切到同父商品的其它供应商，否则会让
         * 订单金额、退款金额、商品类型校验和实际履约商品再次错位。
         */
        SupplierDispatchGatewayRequest request = dispatchRequest(event, order);
        long flowId = orderPort.insertSupplierFlowStart(new SupplierFlowStartCommand(
                order.orderId(),
                order.outOrderSn(),
                order.supplierChannel(),
                "new",
                "准备下单",
                event.eventId(),
                requestPayload(request)));
        SupplierGatewayResponse response = supplierGatewayPort.dispatch(request);
        orderPort.updateSupplierFlow(flowUpdate(flowId, response));
        return finishDispatch(event, order, flowId, response);
    }

    private boolean recoverDispatchExceptionByQuery(
            RechargeOutboxEvent event,
            RechargeFulfillmentOrder order,
            Exception originalException) {
        if (order == null) {
            return false;
        }
        try {
            Optional<SupplierFlowSnapshot> latestFlow = orderPort.findLatestSupplierFlowByOrderId(order.orderId());
            if (latestFlow.isEmpty()) {
                return false;
            }
            recoverDispatchUnknownByQuery(
                    event,
                    order,
                    latestFlow.get(),
                    "供应商派单异常后发现已有调用流水，转查单确认");
            return true;
        } catch (Exception recoveryException) {
            log.error(
                    "派单异常后转查单恢复判断失败 eventId={}, orderId={}, originalError={}",
                    event.eventId(),
                    order.orderId(),
                    originalException.getMessage(),
                    recoveryException);
            return false;
        }
    }

    private RechargeOutboxBatchResult.Outcome recoverDispatchUnknownByQuery(
            RechargeOutboxEvent event,
            RechargeFulfillmentOrder order,
            SupplierFlowSnapshot latestFlow,
            String remark) {
        orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                order.orderId(),
                FulfillmentDispatchStatus.SUPPLIER_ACCEPTED,
                ORDER_STATUS_SUPPLIER_ACCEPTED,
                DATA_STATUS_PROCESSING,
                latestFlow.flowId(),
                null,
                null,
                remark));
        enqueueSupplierQuery(event, order);
        outboxPort.markSuccess(event);
        return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
    }

    private RechargeOutboxBatchResult.Outcome finishDispatch(
            RechargeOutboxEvent event,
            RechargeFulfillmentOrder order,
            long flowId,
            SupplierGatewayResponse response) {
        switch (response.status()) {
            case SUCCESS -> {
                orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                        order.orderId(),
                        FulfillmentDispatchStatus.SUCCESS,
                        ORDER_STATUS_SUCCESS,
                        DATA_STATUS_PROCESSED,
                        flowId,
                        null,
                        null,
                        "供应商充值成功").withMinisoReceipt(response.minisoReceipt()));
                return completeSuccessfulOrder(event, order);
            }
            case PROCESSING -> {
                orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                        order.orderId(),
                        FulfillmentDispatchStatus.SUPPLIER_ACCEPTED,
                        ORDER_STATUS_SUPPLIER_ACCEPTED,
                        DATA_STATUS_PROCESSING,
                        flowId,
                        null,
                        null,
                        "供应商已受理，等待最终结果"));
                enqueueSupplierQuery(event, order);
                outboxPort.markSuccess(event);
                return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
            }
            case FAILED -> {
                return finishDispatchFailure(
                        event,
                        order,
                        flowId,
                        response.responseCode(),
                        response.responseMessage());
            }
            case RETRYABLE_FAILED -> {
                /*
                 * 参见 docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」。HTTP 5xx、超时、连接异常属于
                 * “派单结果不可确认”，不能按供应商明确失败释放预扣，也不能盲目重派同一订单；对 MINISO 这类可
                 * 按本地幂等号查单的渠道，下一步必须转 SUPPLIER_QUERY，用同一订单事实确认终态。
                 */
                orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                        order.orderId(),
                        FulfillmentDispatchStatus.SUPPLIER_ACCEPTED,
                        ORDER_STATUS_SUPPLIER_ACCEPTED,
                        DATA_STATUS_PROCESSING,
                        flowId,
                        response.responseCode(),
                        response.responseMessage(),
                        "供应商派单结果不可确认，转查单确认"));
                enqueueSupplierQuery(event, order);
                outboxPort.markSuccess(event);
                return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
            }
            default -> throw new IllegalStateException("unsupported supplier status: " + response.status());
        }
    }

    private RechargeOutboxBatchResult.Outcome finishDispatchFailure(
            RechargeOutboxEvent event,
            RechargeFulfillmentOrder order,
            Long flowId,
            String failCode,
            String failReason) {
        orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                order.orderId(),
                FulfillmentDispatchStatus.FAILED,
                ORDER_STATUS_FAILED,
                DATA_STATUS_PROCESSED,
                flowId,
                failCode,
                failReason,
                failReason));
        createOrderStatusNotifyTask(order, LegacyOrderStatusCode.FAILED);
        if (!releaseBalanceReservationAndEnqueueDelta(event, order, failReason)) {
            frequencyPort.releaseIfOwned(
                    order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        frequencyPort.releaseIfOwned(
                order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
        outboxPort.markSuccess(event);
        return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
    }

    /**
     * 处理供应商处理中订单的异步查单事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和
     * {@code docs/交行直充业务系统高可用方案设计.md}「17.1 任务清单」。查单仍返回处理中或传输层不可确认
     * 结果时只退避当前查单事件；只有供应商明确给出成功/失败，才复用派单终态副作用，包括回调、频控释放和余额释放。
     */
    private RechargeOutboxBatchResult.Outcome handleSupplierQuery(
            RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        if (order == null) {
            markFailedOrManualRequired(event, "order not found for supplier query", settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        // CANCELLED订单仍为12，必须先于失败资金补偿分支识别；参见详细设计9.6。
        if (order.cancelled()) return handleInvalidOrder(event, order);
        if (order.terminalBalanceReleasePending()) {
            createOrderStatusNotifyTask(order, LegacyOrderStatusCode.FAILED);
            return completeTerminalBalanceRelease(event, order, "供应商查单终态补偿释放预扣");
        }
        if (order.orderStatus() == ORDER_STATUS_FAILED) {
            createOrderStatusNotifyTask(order, LegacyOrderStatusCode.FAILED);
            frequencyPort.releaseIfOwned(
                    order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        if (order.orderStatus() == ORDER_STATUS_SUCCESS) {
            // 终态已写入但回调或 outbox 成功回写可能尚未完成，依赖 notify task/event_id 幂等重放副作用。
            return completeSuccessfulOrder(event, order);
        }
        if (order.orderStatus() != ORDER_STATUS_SUPPLIER_ACCEPTED) {
            // 订单已经由其他重试或人工流程推进终态，当前查单事件只需关闭，不能重复调用供应商。
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }

        SupplierQueryGatewayRequest request = queryRequest(event, order);
        long flowId = orderPort.insertSupplierFlowStart(new SupplierFlowStartCommand(
                order.orderId(),
                order.outOrderSn(),
                order.supplierChannel(),
                "new",
                "供应商查单中",
                request.supplierRequestId(),
                queryRequestPayload(request)));
        SupplierGatewayResponse response = supplierGatewayPort.query(request);
        orderPort.updateSupplierFlow(flowUpdate(flowId, response));
        return finishSupplierQuery(event, order, flowId, response);
    }

    private RechargeOutboxBatchResult.Outcome finishSupplierQuery(
            RechargeOutboxEvent event,
            RechargeFulfillmentOrder order,
            long flowId,
            SupplierGatewayResponse response) {
        switch (response.status()) {
            case SUCCESS -> {
                orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                        order.orderId(),
                        FulfillmentDispatchStatus.SUCCESS,
                        ORDER_STATUS_SUCCESS,
                        DATA_STATUS_PROCESSED,
                        flowId,
                        response.responseCode(),
                        null,
                        "供应商查单确认成功").withMinisoReceipt(response.minisoReceipt()));
                return completeSuccessfulOrder(event, order);
            }
            case FAILED -> {
                orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                        order.orderId(),
                        FulfillmentDispatchStatus.FAILED,
                        ORDER_STATUS_FAILED,
                        DATA_STATUS_PROCESSED,
                        flowId,
                        response.responseCode(),
                        response.responseMessage(),
                        response.responseMessage()));
                createOrderStatusNotifyTask(order, LegacyOrderStatusCode.FAILED);
                if (!releaseBalanceReservationAndEnqueueDelta(event, order, response.responseMessage())) {
                    frequencyPort.releaseIfOwned(
                            order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
                    return RechargeOutboxBatchResult.Outcome.FAILED;
                }
                frequencyPort.releaseIfOwned(
                        order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
                outboxPort.markSuccess(event);
                return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
            }
            case PROCESSING -> {
                orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                        order.orderId(),
                        FulfillmentDispatchStatus.SUPPLIER_ACCEPTED,
                        ORDER_STATUS_SUPPLIER_ACCEPTED,
                        DATA_STATUS_PROCESSING,
                        flowId,
                        response.responseCode(),
                        response.responseMessage(),
                        "供应商仍在处理中"));
                markSupplierQueryFailedOrManualRequired(event, response.responseMessage());
                return RechargeOutboxBatchResult.Outcome.FAILED;
            }
            case RETRYABLE_FAILED -> {
                markSupplierQueryFailedOrManualRequired(event, response.responseMessage());
                return RechargeOutboxBatchResult.Outcome.FAILED;
            }
            default -> throw new IllegalStateException("unsupported supplier query status: " + response.status());
        }
    }

    private RechargeOutboxBatchResult.Outcome handleInvalidOrder(
            RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        if (order == null) {
            markFailedOrManualRequired(event, "order not found for invalid", settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        if (blockedByMinisoRedemption(event, order))
            return RechargeOutboxBatchResult.Outcome.FAILED;
        // 作废完成仍保留订单12，独立终态及原时间用于恢复；参见详细设计9.6。
        if (order.cancelled()) {
            if (order.invalidTime() == null) {
                outboxPort.markManualRequired(event, "CANCELLED_TIME_MISSING");
                return RechargeOutboxBatchResult.Outcome.FAILED;
            }
            createWriteOffNotifyTask(order, order.invalidTime());
            if (order.balanceReserved())
                return completeTerminalBalanceRelease(event, order, "供应商作废终态补偿释放预扣");
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        if (order.terminalBalanceReleasePending()) {
            return completeTerminalBalanceRelease(event, order, "供应商作废终态补偿释放预扣");
        }
        if (order.orderStatus() != ORDER_STATUS_SUCCESS) {
            markFailedOrManualRequired(event, "订单尚未发券成功，不能确认券作废", settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                order.orderId(),
                FulfillmentDispatchStatus.CANCEL_REQUESTED,
                ORDER_STATUS_SUCCESS,
                DATA_STATUS_PROCESSING,
                null,
                null,
                null,
                "供应商作废中"));
        SupplierGatewayResponse response = supplierGatewayPort.invalid(invalidRequest(event, order));
        if (response.status() == SupplierGatewayStatus.SUCCESS) {
            if (blockedByMinisoRedemption(event, order))
                return RechargeOutboxBatchResult.Outcome.FAILED;
            var invalidTime = clock.instant();
            orderPort.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                    order.orderId(),
                    FulfillmentDispatchStatus.CANCELLED,
                    ORDER_STATUS_SUCCESS,
                    DATA_STATUS_PROCESSED,
                    null,
                    null,
                    null,
                    "供应商作废成功").withMinisoReceipt(response.minisoReceipt()).withInvalidTime(invalidTime));
            createWriteOffNotifyTask(order, invalidTime);
            if (!releaseBalanceReservationAndEnqueueDelta(event, order, "供应商作废成功")) {
                frequencyPort.releaseIfOwned(
                        order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
                return RechargeOutboxBatchResult.Outcome.FAILED;
            }
            frequencyPort.releaseIfOwned(
                    order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
            outboxPort.markSuccess(event);
            return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
        }
        if (response.status() == SupplierGatewayStatus.PROCESSING) {
            // 作废受理不是作废完成，保留同一任务恢复且不释放资金；参见详细设计9.6。
            markFailedOrManualRequired(event, "供应商作废处理中", settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        markFailedOrManualRequired(event, response.responseMessage(), settings.retryBackoff());
        return RechargeOutboxBatchResult.Outcome.FAILED;
    }

    /** 作废前、远端成功后与退款补偿统一复核持久核销证据；参见详细设计9.5。 */
    private boolean blockedByMinisoRedemption(
            RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        if (order == null || !"MINISO".equals(order.supplierChannel())) return false;
        var protection = minisoWriteOffProtection.get();
        if (protection != null && protection.blockInvalidation(event.eventId(), order.orderId())) {
            outboxPort.markManualRequired(event, "REDEEM_VOID_CONFLICT");
            return true;
        }
        return false;
    }

    private RechargeOutboxBatchResult.Outcome completeTerminalBalanceRelease(
            RechargeOutboxEvent event,
            RechargeFulfillmentOrder order,
            String reason) {
        if (blockedByMinisoRedemption(event, order))
            return RechargeOutboxBatchResult.Outcome.FAILED;
        if (!releaseBalanceReservationAndEnqueueDelta(event, order, reason)) {
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        frequencyPort.releaseIfOwned(
                order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
        outboxPort.markSuccess(event);
        return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
    }

    /**
     * 成功业务事实已提交后补齐消费确认；确认失败保留同一Outbox恢复，不再次请求供应商。
     * 参见详细设计「9.2 成功预扣收尾最小修订」。确认不退款、不新增扣款delta，后续成功副作用保持原语义。
     */
    private RechargeOutboxBatchResult.Outcome completeSuccessfulOrder(
            RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        if (order.balanceReserved() && !balanceReservationReleasePort.confirm(order)) {
            markFailedOrManualRequired(event, "successful order balance consumption confirmation failed", settings.retryBackoff());
            return RechargeOutboxBatchResult.Outcome.FAILED;
        }
        createOrderStatusNotifyTask(order, LegacyOrderStatusCode.SUCCESS);
        frequencyPort.releaseIfOwned(
                order.orgCode(), order.account(), order.orderId(), settings.frequencySuccessMinimumHold());
        outboxPort.markSuccess(event);
        return RechargeOutboxBatchResult.Outcome.SUCCEEDED;
    }

    private boolean releaseBalanceReservationAndEnqueueDelta(
            RechargeOutboxEvent event,
            RechargeFulfillmentOrder order,
            String reason) {
        /*
         * docs/交行直充业务系统高可用方案设计.md「14.6 订单最终失败/作废退款协议」
         * 要求终态失败后释放 Redis reservation，并生成正向 BALANCE_DELTA_FLUSH 沉淀回旧表余额。
         * releaseEventId 使用当前 outbox 事件 ID 保证 Redis Lua 幂等；若 Redis 回补失败，不能先写
         * MySQL 余额反向事件，否则 Redis 与旧表会继续扩大偏差，因此保留当前 outbox 重试入口。
         */
        BalanceReservationReleaseResult releaseResult = balanceReservationReleasePort.release(
                new BalanceReservationReleaseCommand(
                        order.orgCode(),
                        order.orderId(),
                        order.outOrderSn(),
                        order.orderPrice(),
                        "RELEASE",
                        event.eventId(),
                        reason == null ? "" : reason));
        if (!releaseResult.succeeded()) {
            String failReason = releaseResult.failReason().isBlank()
                    ? releaseResult.failCode()
                    : releaseResult.failReason();
            markFailedOrManualRequired(event, "release balance reservation failed: " + failReason, settings.retryBackoff());
            return false;
        }
        enqueueReleaseBalanceDelta(order, reason);
        orderPort.markBalanceReleased(order.orderId());
        return true;
    }

    private void markFailedOrManualRequired(
            RechargeOutboxEvent event,
            String errorMessage,
            java.time.Duration nextRetryDelay) {
        if (event.nextFailureReachesMaxRetry()) {
            outboxPort.markManualRequired(event, errorMessage);
            return;
        }
        outboxPort.markFailed(event, errorMessage, nextRetryDelay);
    }

    /**
     * 按供应商查单专用退避序列处理非终态查单结果。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。查单处理中、HTTP 超时、
     * 5xx 或不可识别响应都不能释放预扣或回写失败终态，只能推进同一个 {@code SUPPLIER_QUERY} 事件的
     * retry_count/next_retry_time，并重新安排 timer 热唤醒；耗尽序列后转人工。
     */
    private void markSupplierQueryFailedOrManualRequired(RechargeOutboxEvent event, String errorMessage) {
        if (event.nextFailureReachesMaxRetry()) {
            outboxPort.markManualRequired(event, errorMessage);
            return;
        }
        Duration nextRetryDelay = nextSupplierQueryRetryDelay(event);
        outboxPort.markFailed(event, errorMessage, nextRetryDelay);
        scheduleOutboxTimer(event.eventId(), OutboxEventType.SUPPLIER_QUERY, nextRetryDelay);
    }

    /**
     * 计算下一次查单退避时间。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。event 当前 retry_count
     * 代表已失败次数，本次失败写库后会 +1，因此按 {@code retryCount + 1} 取下一档；超过配置序列时复用最后一档，
     * 但默认 max_retry_count 等于序列长度，正常路径会先进入人工。
     */
    private Duration nextSupplierQueryRetryDelay(RechargeOutboxEvent event) {
        List<Duration> delays = settings.supplierQueryRetryDelays();
        int retryCountAfterCurrentFailure = event.retryCount() + 1;
        int index = Math.min(retryCountAfterCurrentFailure, delays.size() - 1);
        return delays.get(index);
    }

    private static String retryExhaustedMessage(RechargeOutboxEvent event) {
        if (event.lastError().isBlank()) {
            return "outbox retry exhausted";
        }
        return "outbox retry exhausted: " + event.lastError();
    }

    private void enqueueReleaseBalanceDelta(RechargeFulfillmentOrder order, String reason) {
        /*
         * docs/交行直充业务系统详细设计.md「9.2 余额 Delta 刷账 Outbox」要求作废/失败完成后
         * 写入 delta_type=RELEASE 的刷账事件（event_id=balance-flush-release-{orderId}）。
         * 这里仅写可靠事件，不在 Worker 线程直接改 mc_ext_depart.balance，
         * 保持余额刷账的 CAS 抢占、按 org 聚合和重试语义（高可用「14.7」）。
         */
        balanceDeltaFlushOutboxPort.enqueueBalanceDeltaFlush(new BalanceDeltaFlushOutboxCommand(
                order.orgCode(),
                order.orderId(),
                order.outOrderSn(),
                order.orderPrice(),
                "RELEASE",
                reason == null ? "" : reason));
    }

    private void createOrderStatusNotifyTask(RechargeFulfillmentOrder order, LegacyOrderStatusCode orderStatus) {
        if (!hasNotifyUrl(order)) {
            return;
        }
        String orderStatusCode = Integer.toString(orderStatus.code());
        Map<String, String> payload = notifyPayloadSigner.signOrderStatus(order, orderStatusCode, clock.instant());
        notifyTaskPort.insertIfAbsent(new RechargeNotifyTaskDraft(
                "notify-order-" + order.orderId() + "-" + orderStatusCode,
                "ORDER_STATUS",
                order.orgCode(),
                order.orderId(),
                order.outOrderSn(),
                order.notifyUrl(),
                orderStatusCode,
                payload,
                DEFAULT_NOTIFY_MAX_RETRY_COUNT,
                LocalDateTime.now(clock)));
    }

    private void createWriteOffNotifyTask(RechargeFulfillmentOrder order, java.time.Instant invalidTime) {
        if (!hasNotifyUrl(order)) {
            return;
        }
        Map<String, String> payload = notifyPayloadSigner.signWriteOff(order, invalidTime);
        notifyTaskPort.insertIfAbsent(new RechargeNotifyTaskDraft(
                "notify-writeoff-" + order.orderId() + "-cancelled",
                "VOUCHER_STATE",
                order.orgCode(),
                order.orderId(),
                order.outOrderSn(),
                order.notifyUrl(),
                "CANCELLED",
                payload,
                DEFAULT_NOTIFY_MAX_RETRY_COUNT,
                LocalDateTime.now(clock)));
    }

    private static boolean hasNotifyUrl(RechargeFulfillmentOrder order) {
        return order.notifyUrl() != null && !order.notifyUrl().isBlank();
    }

    private static SupplierDispatchGatewayRequest dispatchRequest(RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        return new SupplierDispatchGatewayRequest(
                event.eventId(),
                order.orderId(),
                order.outOrderSn(),
                order.orgCode(),
                order.account(),
                order.productId(),
                order.selectedProductId(),
                order.supplierChannel(),
                order.orderPrice(),
                Map.of("outbox_payload", event.payload()));
    }

    private static SupplierInvalidGatewayRequest invalidRequest(RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        return new SupplierInvalidGatewayRequest(
                event.eventId(),
                order.orderId(),
                order.outOrderSn(),
                order.orgCode(),
                order.account(),
                order.supplierChannel(),
                Map.of("outbox_payload", event.payload()));
    }

    private static SupplierQueryGatewayRequest queryRequest(RechargeOutboxEvent event, RechargeFulfillmentOrder order) {
        return new SupplierQueryGatewayRequest(
                event.eventId(),
                order.orderId(),
                order.outOrderSn(),
                order.orgCode(),
                order.account(),
                order.supplierChannel(),
                Map.of("outbox_payload", event.payload()));
    }

    private void enqueueSupplierQuery(RechargeOutboxEvent dispatchEvent, RechargeFulfillmentOrder order) {
        String eventId = RechargeDispatchEventIds.supplierQueryEventId(order.orderId());
        Duration initialDelay = settings.supplierQueryRetryDelays().getFirst();
        supplierQueryOutboxPort.enqueueSupplierQuery(new SupplierQueryOutboxCommand(
                eventId,
                order.orgCode(),
                order.orderId(),
                order.outOrderSn(),
                order.account(),
                order.supplierChannel(),
                dispatchEvent.payload(),
                initialDelay,
                settings.supplierQueryRetryDelays().size()));
        scheduleOutboxTimer(eventId, OutboxEventType.SUPPLIER_QUERY, initialDelay);
    }

    private void scheduleOutboxTimer(String eventId, OutboxEventType eventType, Duration delay) {
        try {
            dispatchTimerPort.schedule(eventId, eventType, delay);
        } catch (RuntimeException ex) {
            /*
             * 参见 docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」。
             * timer 只是热路径唤醒，失败时保留 outbox due 时间交给低频 XXL-Job 恢复；该分支位于高频履约路径，
             * 使用 warn 并携带堆栈，避免 timer 后端异常时把日志放大成新的故障源。
             */
            log.warn("供应商履约timer调度失败 eventId={}, eventType={}, delay={}", eventId, eventType, delay, ex);
        }
    }

    private static Map<String, Object> requestPayload(SupplierDispatchGatewayRequest request) {
        return Map.ofEntries(
                Map.entry("supplier_request_id", request.supplierRequestId()),
                Map.entry("order_id", request.orderId()),
                Map.entry("out_order_sn", request.outOrderSn()),
                Map.entry("org_code", request.orgCode()),
                Map.entry("account", request.account()),
                Map.entry("product_id", request.productId()),
                Map.entry("selected_product_id", request.selectedProductId()),
                Map.entry("supplier_channel", request.supplierChannel()),
                Map.entry("order_price", request.orderPrice()));
    }

    private static Map<String, Object> queryRequestPayload(SupplierQueryGatewayRequest request) {
        return Map.ofEntries(
                Map.entry("supplier_request_id", request.supplierRequestId()),
                Map.entry("order_id", request.orderId()),
                Map.entry("out_order_sn", request.outOrderSn()),
                Map.entry("org_code", request.orgCode()),
                Map.entry("account", request.account()),
                Map.entry("supplier_channel", request.supplierChannel()));
    }

    private static SupplierFlowUpdateCommand flowUpdate(long flowId, SupplierGatewayResponse response) {
        String flowStatus = switch (response.status()) {
            case SUCCESS -> "success";
            case PROCESSING -> "processing";
            case FAILED -> "fail";
            /*
             * mc_order_supplier_flow.status 是旧卡管 varchar(10)，DDL 注释只约定 new/success/fail/processing。
             * docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」中的可重试失败表示供应商结果未确认，
             * 不是业务终态失败；写旧表时保持 processing，详细错误原因放到 response_result/remark 和 outbox
             * last_error 中，避免因状态值超长导致 SUP-002 的退避重试被数据库兼容问题打断。
             */
            case RETRYABLE_FAILED -> "processing";
        };
        return new SupplierFlowUpdateCommand(
                flowId,
                flowStatus,
                response.responseMessage(),
                response.responseCode(),
                response.responseMessage(),
                response.supplierOrderId(),
                response.rawResponse());
    }
}
