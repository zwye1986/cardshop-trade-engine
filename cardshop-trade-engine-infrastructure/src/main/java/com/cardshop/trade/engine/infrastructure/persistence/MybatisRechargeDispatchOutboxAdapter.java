package com.cardshop.trade.engine.infrastructure.persistence;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxEvent;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.application.recharge.SupplierQueryOutboxCommand;
import com.cardshop.trade.engine.application.recharge.SupplierQueryOutboxPort;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxCommand;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxPort;
import com.cardshop.trade.engine.application.recharge.MinisoWriteOffProtection;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.application.recharge.RechargeOrderInvalidationState;
import com.cardshop.trade.engine.application.recharge.InvalidationAttempt;
import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.application.recharge.SupplierFlowStartCommand;
import com.cardshop.trade.engine.application.recharge.MinisoWriteOffException;
import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
import com.cardshop.trade.engine.domain.order.VoucherUseState;
import com.cardshop.trade.engine.application.recharge.FulfillmentDispatchStatus;
import static com.cardshop.trade.engine.application.recharge.RechargeOrderInvalidationState.Status.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import java.util.function.Supplier;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeDispatchOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 基于 {@code recharge_dispatch_outbox} 的派单履约 outbox 适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」「8.3 异步落库 Consumer」以及
 * 「9.3 作废设计」中 {@code INVALID_ORDER} 入队路径。本适配器只服务供应商履约生命周期
 * （{@code SUPPLIER_DISPATCH}/{@code SUPPLIER_QUERY}/{@code INVALID_ORDER}），不写入余额刷账事件；
 * 余额 delta 入队见 {@link MybatisBalanceDeltaOutboxWriteAdapter}
 * （详细设计「9.2 余额 Delta 刷账 Outbox」）。
 *
 * <p>分片键固定使用 {@code event_id}，与高可用方案 {@code docs/交行直充业务系统高可用方案设计.md}
 * 「17.2 分片规则」一致，避免不同任务混用分片语义。
 */
public class MybatisRechargeDispatchOutboxAdapter
        implements RechargeOutboxPort, SupplierQueryOutboxPort, InvalidRechargeOrderOutboxPort {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(MybatisRechargeDispatchOutboxAdapter.class);
    private final LogStormGuard invalidationLogs = LogStormGuard.defaultGuard();
    private static final int DEFAULT_MAX_RETRY_COUNT = 10;
    private static final int ADMISSION_TRANSACTION_TIMEOUT_SECONDS = 10;
    private static final String MINISO_CHANNEL = "MINISO";
    private static final String DB_PERSISTED = "DB_PERSISTED";
    private static final int MINISO_COUPON_UNUSED = 0;
    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");

    private final RechargeDispatchOutboxMapper mapper;
    private final RechargeDispatchOutboxPersistWriter writer;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private McOrderMapper orderMapper;
    private RechargeOrderExtMapper extMapper;
    private MinisoWriteOffOrderLocks orderLocks;
    private Supplier<MinisoWriteOffProtection> protection;
    private Supplier<RechargeFulfillmentOrderPort> orders;
    private MinisoReceiptMapper receipts;
    private TransactionTemplate admission;
    private TransactionTemplate observation;

    /** 作废受理使用主SqlSession及短事务，依赖延迟解析以避免Worker/资金适配器循环装配。参见详细设计9.3.2。 */
    public MybatisRechargeDispatchOutboxAdapter(RechargeDispatchOutboxMapper mapper, ObjectMapper objectMapper,
            Clock clock, McOrderMapper orderMapper, RechargeOrderExtMapper extMapper,
            MinisoWriteOffEventMapper events, MinisoReceiptMapper receipts, MinisoCouponGuardMapper guards,
            Supplier<MinisoWriteOffProtection> protection, Supplier<RechargeFulfillmentOrderPort> orders,
            PlatformTransactionManager tm) {
        this(mapper, objectMapper, clock);
        this.orderMapper = orderMapper;
        this.extMapper = extMapper;
        this.orderLocks = new MinisoWriteOffOrderLocks(events, receipts, guards);
        this.protection = protection;
        this.orders = orders;
        this.receipts = receipts;
        admission = new TransactionTemplate(tm);
        admission.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        admission.setTimeout(ADMISSION_TRANSACTION_TIMEOUT_SECONDS);
        observation = new TransactionTemplate(tm);
        observation.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        observation.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        observation.setReadOnly(true);
        observation.setTimeout(ADMISSION_TRANSACTION_TIMEOUT_SECONDS);
    }

    @Override
    public RechargeOrderInvalidationState prepareInvalidOrder(InvalidRechargeOrderOutboxCommand command) {
        return Objects.requireNonNull(admission, "persistent admission dependencies required").execute(tx -> {
            var order = orderMapper.selectInvalidationOrder(command.orgCode(), command.outOrderSn(), true);
            if (order == null) return new RechargeOrderInvalidationState(null, null, NOT_ACCEPTED, "订单尚未落库，作废未受理");
            var ext = extMapper.selectInvalidationOrder(order.getOrderId(), true);
            var event = mapper.selectInvalidation(invalidEventId(order.getOrderId()), order.getOrgCode(),
                    order.getOrderId(), order.getOutOrderId(), true);
            var state = invalidationState(order, ext, event, 0);
            boolean newRound = state.attempt() != null && state.attempt().result() == InvalidationAttempt.Result.REJECTED;
            if (state.status() != NOT_ACCEPTED && !newRound) return state;
            if (!owned(order, ext) || !Objects.equals(command.orderId(), order.getOrderId())) return state;
            if (!Objects.equals(order.getOrderStatus(), LegacyOrderStatusCode.SUCCESS.code())
                    || !FulfillmentDispatchStatus.SUCCESS.name().equals(ext.getDispatchStatus())
                    || !VoucherUseState.UNUSED.code().equals(ext.getVoucherUseState())
                    || ext.getVoucherUseTime() != null) {
                return new RechargeOrderInvalidationState(state.order(), null, NOT_ACCEPTED, "订单未发券成功或券不可作废，作废未受理");
            }
            if (ext.getBalanceReleaseAuthorizedAt() != null || ext.getBalanceReleaseEventId() != null
                    || ext.getBalanceReleaseAmount() != null || ext.getBalanceNoReleaseDecidedAt() != null
                    || ext.getBalanceNoReleaseEventId() != null) {
                return new RechargeOrderInvalidationState(state.order(), null, NOT_ACCEPTED, "订单存在资金裁决，作废未受理");
            }
            if (MINISO_CHANNEL.equals(order.getSupplierChannel())) {
                var local = orderLocks.lock(order.getOrderId(), now());
                if (local == null || local.receipt().couponCode() == null || local.receipt().couponCode().isBlank()
                        || local.receipt().phone() == null || local.receipt().phone().isBlank()
                        || local.receipt().couponStatus() != MINISO_COUPON_UNUSED) {
                    return new RechargeOrderInvalidationState(state.order(), null, NOT_ACCEPTED, "本地券凭据不完整或券不可作废，作废未受理");
                }
                try {
                    Objects.requireNonNull(protection.get(), "MINISO protection required").assertVoidAllowed(order.getOrderId());
                } catch (MinisoWriteOffException ex) {
                    if (ex.reason() != MinisoWriteOffException.Reason.REDEEM_VOID_CONFLICT) throw ex;
                    if (invalidationLogs.shouldLog("invalidation.redemption").shouldLog()) {
                        log.warn("作废首次受理遇已认证核销证据 orderId={}", order.getOrderId(), ex);
                    }
                    return new RechargeOrderInvalidationState(state.order(), null, NOT_ACCEPTED, "券已存在核销证据，作废未受理");
                }
            }
            try {
                if (newRound) {
                    var payload = readPayload(event.getPayload());
                    long number = state.attempt().number() + 1;
                    payload.withArray(InvalidationAttempt.PAYLOAD_KEY).add(objectMapper.valueToTree(
                            pendingAttempt(event.getEventId(), number, command.deadlineEpochMs())));
                    int changed = mapper.update(null, new LambdaUpdateWrapper<RechargeDispatchOutboxDO>()
                            .eq(RechargeDispatchOutboxDO::getEventId, event.getEventId())
                            .eq(RechargeDispatchOutboxDO::getVersion, event.getVersion())
                            .set(RechargeDispatchOutboxDO::getPayload, payload.toString())
                            .set(RechargeDispatchOutboxDO::getStatus, TaskStatus.INIT.name())
                            .set(RechargeDispatchOutboxDO::getRetryCount, 0)
                            .set(RechargeDispatchOutboxDO::getNextRetryTime, now().withNano(0))
                            .set(RechargeDispatchOutboxDO::getLockedBy, null)
                            .set(RechargeDispatchOutboxDO::getLockedUntil, null)
                            .set(RechargeDispatchOutboxDO::getLastError, "")
                            .set(RechargeDispatchOutboxDO::getUpdatedAt, now())
                            .setSql("version = version + 1"));
                    if (changed != 1) throw new IllegalStateException("invalidation round changed during admission");
                } else enqueueInvalidOrder(command);
                if (mapper.selectInvalidation(invalidEventId(order.getOrderId()), order.getOrgCode(),
                        order.getOrderId(), order.getOutOrderId()) == null) {
                    throw new IllegalStateException("invalidation event identity mismatch");
                }
            } catch (RuntimeException ex) {
                // TransactionTemplate回滚成功才透出该标记；回滚失败覆盖为事务异常，仍按未知处理。见详细设计9.3.2。
                throw new AdmissionRolledBackException(ex);
            }
            return invalidationState(order, ext, mapper.selectInvalidation(invalidEventId(order.getOrderId()),
                    order.getOrgCode(), order.getOrderId(), order.getOutOrderId()), 0);
        });
    }

    @Override
    public Optional<RechargeOrderInvalidationState> findInvalidOrder(String orgCode, String outOrderSn) {
        return findInvalidOrder(orgCode, outOrderSn, 0);
    }

    @Override
    public Optional<RechargeOrderInvalidationState> findInvalidOrder(String orgCode, String outOrderSn, long attemptNo) {
        return Objects.requireNonNull(observation, "persistent observation dependencies required").execute(tx -> {
            var order = orderMapper.selectInvalidationOrder(orgCode, outOrderSn, false);
            return order == null ? Optional.empty()
                    : Optional.of(invalidationState(order, extMapper.selectInvalidationOrder(order.getOrderId(), false),
                            mapper.selectInvalidation(invalidEventId(order.getOrderId()), order.getOrgCode(),
                                    order.getOrderId(), order.getOutOrderId()), attemptNo));
        });
    }

    private RechargeOrderInvalidationState invalidationState(McOrderDO order, RechargeOrderExtDO ext,
            RechargeDispatchOutboxDO event, long attemptNo) {
        boolean owned = owned(order, ext);
        var snapshot = MybatisRechargeOrderQueryAdapter.toSnapshot(order, owned ? ext : null, order.getOutOrderId());
        String eventId = event == null ? null : event.getEventId();
        var attempt = event == null ? null : attempt(event.getPayload(), attemptNo);
        if (attempt != null) {
            if (!owned) return new RechargeOrderInvalidationState(snapshot, eventId, MANUAL_REQUIRED, "作废归属证据不完整");
            // 加入轮次不能重新获得五分钟；只读观察按原截止失败，不在这里取写锁或发起恢复。
            if (attempt.result() == InvalidationAttempt.Result.PENDING
                    && (clock.millis() >= attempt.deadlineEpochMs() || TaskStatus.MANUAL_REQUIRED.name().equals(event.getStatus()))) {
                return new RechargeOrderInvalidationState(snapshot, eventId, RechargeOrderInvalidationState.Status.FAILED,
                        "作废失败", attempt);
            }
            return new RechargeOrderInvalidationState(snapshot, eventId, PENDING, "").withAttempt(attempt);
        }
        if (owned && FulfillmentDispatchStatus.CANCELLED.name().equals(ext.getDispatchStatus())) {
            boolean complete = Objects.equals(order.getOrderStatus(), LegacyOrderStatusCode.SUCCESS.code())
                    && VoucherUseState.CANCELLED.code().equals(ext.getVoucherUseState())
                    && ext.getVoucherInvalidTime() != null && ext.getVoucherUseTime() == null;
            if (complete && MINISO_CHANNEL.equals(order.getSupplierChannel())) {
                try {
                    complete = Objects.requireNonNull(orders.get(), "funds decision reader required")
                            .hasMinisoTerminalFundsDecision(order.getOrgCode(), order.getOutOrderId());
                } catch (MinisoWriteOffException ex) {
                    if (invalidationLogs.shouldLog("invalidation.decision").shouldLog()) {
                        log.error("作废资金裁决不完整，保留人工核查 orderId={}", order.getOrderId(), ex);
                    }
                    complete = false;
                }
            }
            return new RechargeOrderInvalidationState(snapshot, eventId, complete ? COMPLETED : MANUAL_REQUIRED,
                    complete ? "" : "作废历史事实或资金裁决需人工核查");
        }
        if (event != null) return new RechargeOrderInvalidationState(snapshot, eventId,
                TaskStatus.MANUAL_REQUIRED.name().equals(event.getStatus()) ? MANUAL_REQUIRED : PENDING, "");
        return new RechargeOrderInvalidationState(snapshot, null, NOT_ACCEPTED,
                owned ? "尚无作废操作" : "订单缺少新引擎持久归属，作废未受理");
    }

    /** JSON是持久边界，仅在这里转换；无数组表示旧合同，损坏数组不得按无操作重新发送。 */
    private com.fasterxml.jackson.databind.node.ObjectNode readPayload(String payload) {
        try {
            var tree = objectMapper.readTree(payload);
            if (tree == null || !tree.isObject()) throw new IllegalStateException("invalidation payload must be an object");
            return (com.fasterxml.jackson.databind.node.ObjectNode) tree;
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("invalid invalidation payload");
        }
    }

    private InvalidationAttempt attempt(String payload, long number) {
        var root = readPayload(payload);
        var attempts = root.get(InvalidationAttempt.PAYLOAD_KEY);
        if (attempts == null) return null;
        if (!attempts.isArray() || attempts.isEmpty()) throw new IllegalStateException("invalidation attempts missing");
        var index = number == 0 ? attempts.size() - 1 : Math.toIntExact(number - 1);
        if (index < 0 || index >= attempts.size()) throw new IllegalStateException("invalidation round not found");
        try {
            var value = objectMapper.treeToValue(attempts.get(index), InvalidationAttempt.class);
            if (value.number() != index + 1L || value.supplierRequestId() == null || value.supplierRequestId().isBlank()
                    || value.deadlineEpochMs() <= 0 || value.result() == null
                    || (value.result() != InvalidationAttempt.Result.PENDING && value.confirmedEpochMs() <= 0)) {
                throw new IllegalStateException("invalidation round evidence invalid");
            }
            return value;
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("invalid invalidation round");
        }
    }

    private InvalidationAttempt pendingAttempt(String eventId, long number, long deadlineEpochMs) {
        return new InvalidationAttempt(number, number == 1 ? eventId : eventId + "-" + number,
                deadlineEpochMs == 0 ? clock.millis() + java.time.Duration.ofMinutes(5).toMillis() : deadlineEpochMs,
                InvalidationAttempt.Result.PENDING, 0, null);
    }

    @Override
    public Optional<Long> startInvalidationCall(RechargeOutboxEvent event, SupplierFlowStartCommand flow) {
        return admission.execute(tx -> {
            var row = lockCurrentInvalidation(event);
            if (row == null || attempt(row.getPayload(), 0).result() != InvalidationAttempt.Result.PENDING
                    || clock.millis() >= event.invalidationAttempt().deadlineEpochMs()
                    || orders.get().hasSupplierInvalidationAttempt(event.orderId(), flow.supplierRequestId())) return Optional.empty();
            // 受理后可能收到核销，必须在发送登记事务内按原锁序重新裁决，HTTP仍在事务外（详细设计9.3.5）。
            if (MINISO_CHANNEL.equals(flow.channel())) {
                var local = orderLocks.lock(event.orderId(), now());
                if (local == null || local.receipt().couponStatus() != MINISO_COUPON_UNUSED) return Optional.empty();
                Objects.requireNonNull(protection.get(), "MINISO protection required").assertVoidAllowed(event.orderId());
            }
            return Optional.of(orders.get().insertSupplierFlowStart(flow));
        });
    }

    @Override
    public boolean completeInvalidation(RechargeOutboxEvent event, InvalidationAttempt result) {
        return Boolean.TRUE.equals(admission.execute(tx -> {
            var row = lockCurrentInvalidation(event);
            if (row == null) return false;
            var previous = attempt(row.getPayload(), 0);
            if (previous.result() != InvalidationAttempt.Result.PENDING) return previous.equals(result);
            var payload = readPayload(row.getPayload());
            ((com.fasterxml.jackson.databind.node.ArrayNode) payload.get(InvalidationAttempt.PAYLOAD_KEY))
                    .set(Math.toIntExact(result.number() - 1), objectMapper.valueToTree(result));
            var update = new LambdaUpdateWrapper<RechargeDispatchOutboxDO>()
                    .eq(RechargeDispatchOutboxDO::getEventId, event.eventId())
                    .eq(RechargeDispatchOutboxDO::getVersion, event.version())
                    .set(RechargeDispatchOutboxDO::getPayload, payload.toString())
                    .set(RechargeDispatchOutboxDO::getUpdatedAt, now());
            if (result.result() == InvalidationAttempt.Result.REJECTED || result.result() == InvalidationAttempt.Result.UNKNOWN) {
                update.set(RechargeDispatchOutboxDO::getStatus, result.result() == InvalidationAttempt.Result.REJECTED
                                ? TaskStatus.SUCCESS.name() : TaskStatus.MANUAL_REQUIRED.name())
                        .set(RechargeDispatchOutboxDO::getLockedUntil, null)
                        .set(RechargeDispatchOutboxDO::getLastError, result.result().name());
            }
            return mapper.update(null, update) == 1;
        }));
    }

    private RechargeDispatchOutboxDO lockCurrentInvalidation(RechargeOutboxEvent event) {
        var order = orderMapper.selectInvalidationOrder(event.orgCode(), event.outOrderSn(), true);
        if (order == null || !event.orderId().equals(order.getOrderId())) return null;
        var ext = extMapper.selectInvalidationOrder(event.orderId(), true);
        if (!owned(order, ext)) return null;
        var row = mapper.selectInvalidation(event.eventId(), event.orgCode(), event.orderId(), event.outOrderSn(), true);
        if (row == null || !Objects.equals(row.getVersion(), event.version())
                || !TaskStatus.PROCESSING.name().equals(row.getStatus()) || row.getLockedUntil() == null
                || !row.getLockedUntil().isAfter(now())
                || attempt(row.getPayload(), 0).number() != event.invalidationAttempt().number()) return null;
        return row;
    }

    @Override
    public Optional<MinisoFulfillmentReceipt> findInvalidationReceipt(String orgCode, String orderId) {
        var snapshot = receipts.selectSnapshot(orgCode, orderId);
        var receipt = receipts.selectReceipt(orderId);
        if (snapshot == null || receipt == null || !orgCode.equals(receipt.orgCode())
                || !snapshot.account().equals(receipt.phone())
                || Long.parseLong(snapshot.batchId()) != receipt.batchId()) return Optional.empty();
        return Optional.of(new MinisoFulfillmentReceipt(receipt.orderId(), receipt.orgCode(), receipt.batchId(),
                receipt.couponCode(), receipt.phone(), receipt.beginTime(), receipt.endTime(), 3));
    }

    private static boolean owned(McOrderDO order, RechargeOrderExtDO ext) {
        return ext != null && DB_PERSISTED.equals(ext.getPersistStatus())
                && Objects.equals(order.getOrderId(), ext.getOrderId())
                && Objects.equals(order.getOrgCode(), ext.getOrgCode())
                && Objects.equals(order.getOutOrderId(), ext.getOutOrderSn());
    }

    private static String invalidEventId(String orderId) {
        return "invalid-" + Objects.requireNonNull(orderId, "platform order id required");
    }

    public MybatisRechargeDispatchOutboxAdapter(
            RechargeDispatchOutboxMapper mapper,
            ObjectMapper objectMapper,
            Clock clock) {
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.writer = mapper::insertIfAbsent;
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * 测试用构造：只注入写入桩，扫描/抢占路径仍要求真实 Mapper。
     */
    MybatisRechargeDispatchOutboxAdapter(
            RechargeDispatchOutboxPersistWriter writer,
            ObjectMapper objectMapper,
            Clock clock) {
        this.mapper = null;
        this.writer = Objects.requireNonNull(writer, "writer must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public List<RechargeOutboxEvent> selectDueForShard(int shardTotal, int shardIndex, int limit) {
        return selectDueForShard(shardTotal, shardIndex, limit, List.of(OutboxEventType.SUPPLIER_DISPATCH));
    }

    @Override
    public List<RechargeOutboxEvent> selectDueForShard(
            int shardTotal, int shardIndex, int limit, List<OutboxEventType> eventTypes) {
        List<String> types = eventTypes == null || eventTypes.isEmpty()
                ? List.of(OutboxEventType.SUPPLIER_DISPATCH.name())
                : eventTypes.stream().map(Enum::name).toList();
        return requireMapper().selectDueForShardByEventTypes(shardTotal, shardIndex, limit, types, now())
                .stream()
                .map(this::toEvent)
                .toList();
    }

    @Override
    public Optional<RechargeOutboxEvent> findDueByEventId(String eventId, OutboxEventType eventType) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        return Optional.ofNullable(requireMapper().selectDueByEventId(eventId, eventType.name(), now()))
                .map(this::toEvent);
    }

    @Override
    public Optional<Duration> findPendingRetryDelay(String eventId, OutboxEventType eventType) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        return Optional.ofNullable(requireMapper().selectPendingRetryTime(eventId, eventType.name()))
                .map(due -> {
                    Duration remaining = Duration.between(now(), due);
                    return remaining.isNegative() ? Duration.ZERO : remaining;
                });
    }

    @Override
    public Optional<RechargeOutboxEvent> claim(RechargeOutboxEvent event, String workerId, Duration lease) {
        // 参见详细设计「9.1 派单 Outbox」：due、租约回收与截止统一使用注入 Clock 的上海时间。
        LocalDateTime now = now();
        LocalDateTime lockedUntil = now.plusNanos(lease.toNanos());
        int updated = requireMapper().claimProcessing(event.eventId(), event.version(), workerId, lockedUntil, now);
        return updated == 1 ? Optional.of(event.claimedCopy()) : Optional.empty();
    }

    @Override
    public void markSuccess(RechargeOutboxEvent event) {
        // 独立资金协议：收尾提交后也不能由过期/被接管Worker结束新任务，CAS miss必须显式失败。
        LocalDateTime now = now();
        int updated = requireMapper().update(null, new LambdaUpdateWrapper<RechargeDispatchOutboxDO>()
                .eq(RechargeDispatchOutboxDO::getEventId, event.eventId())
                .eq(RechargeDispatchOutboxDO::getVersion, event.version())
                .eq(RechargeDispatchOutboxDO::getStatus, TaskStatus.PROCESSING.name())
                .ge(RechargeDispatchOutboxDO::getLockedUntil, now)
                .set(RechargeDispatchOutboxDO::getStatus, TaskStatus.SUCCESS.name())
                .set(RechargeDispatchOutboxDO::getLockedUntil, null)
                .set(RechargeDispatchOutboxDO::getLastError, "")
                .set(RechargeDispatchOutboxDO::getUpdatedAt, now)
                .setSql("version = version + 1"));
        if (updated != 1) throw new IllegalStateException("outbox claim lost before success: " + event.eventId());
    }

    @Override
    public void markFailed(RechargeOutboxEvent event, String errorMessage, Duration nextRetryDelay) {
        if (event.nextFailureReachesMaxRetry()) {
            markManualRequired(event, errorMessage);
            return;
        }
        requireMapper().update(null, new LambdaUpdateWrapper<RechargeDispatchOutboxDO>()
                .eq(RechargeDispatchOutboxDO::getEventId, event.eventId())
                .eq(RechargeDispatchOutboxDO::getVersion, event.version())
                .set(RechargeDispatchOutboxDO::getStatus, TaskStatus.FAILED.name())
                .set(RechargeDispatchOutboxDO::getNextRetryTime, now().plusNanos(nextRetryDelay.toNanos()))
                .set(RechargeDispatchOutboxDO::getLockedUntil, null)
                .set(RechargeDispatchOutboxDO::getLastError, truncate(errorMessage, 1000))
                .set(RechargeDispatchOutboxDO::getUpdatedAt, now())
                .setSql("retry_count = retry_count + 1")
                .setSql("version = version + 1"));
    }

    @Override
    public void markManualRequired(RechargeOutboxEvent event, String errorMessage) {
        LambdaUpdateWrapper<RechargeDispatchOutboxDO> wrapper = new LambdaUpdateWrapper<RechargeDispatchOutboxDO>()
                .eq(RechargeDispatchOutboxDO::getEventId, event.eventId())
                .eq(RechargeDispatchOutboxDO::getVersion, event.version())
                .set(RechargeDispatchOutboxDO::getStatus, TaskStatus.MANUAL_REQUIRED.name())
                .set(RechargeDispatchOutboxDO::getLockedUntil, null)
                .set(RechargeDispatchOutboxDO::getLastError, truncate(errorMessage, 1000))
                .set(RechargeDispatchOutboxDO::getUpdatedAt, now())
                .setSql("version = version + 1");
        if (!event.retryExhausted()) {
            wrapper.setSql("retry_count = retry_count + 1");
        }
        requireMapper().update(null, wrapper);
    }

    /**
     * 仅供已通过受理事务的内部路径写原事件，提交后由请求或恢复Worker推进。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 作废设计」「9.1 派单 Outbox」。
     * {@code event_id} 固定为 {@code invalid-{orderId}}，{@code shard_key} 与之相同以保证分片稳定。
     */
    void enqueueInvalidOrder(InvalidRechargeOrderOutboxCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        LocalDateTime now = now();
        String eventId = invalidEventId(command.orderId());
        RechargeDispatchOutboxDO outbox = new RechargeDispatchOutboxDO();
        outbox.setEventId(eventId);
        outbox.setEventType(OutboxEventType.INVALID_ORDER.name());
        outbox.setOrgCode(command.orgCode());
        outbox.setOrderId(command.orderId());
        outbox.setOutOrderSn(command.outOrderSn());
        outbox.setPayload(invalidPayload(command));
        outbox.setStatus(TaskStatus.INIT.name());
        outbox.setRetryCount(0);
        outbox.setMaxRetryCount(DEFAULT_MAX_RETRY_COUNT);
        // 参见详细设计9.3.2：首次事件应立即可执行，避免datetime舍入到下一秒使唯一即时唤醒落空。
        outbox.setNextRetryTime(now.withNano(0));
        outbox.setLockedBy(null);
        outbox.setLockedUntil(null);
        outbox.setVersion(0L);
        outbox.setLastError("");
        outbox.setShardKey(eventId);
        outbox.setCreatedAt(now);
        outbox.setUpdatedAt(now);
        writer.insertIfAbsent(outbox);
    }

    /**
     * 写入 {@code SUPPLIER_QUERY} 可靠事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。
     * {@code event_id} 唯一键保证 dispatch 重试或 Worker 宕机恢复时不会重复入查单事件。
     */
    @Override
    public void enqueueSupplierQuery(SupplierQueryOutboxCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        LocalDateTime now = now();
        RechargeDispatchOutboxDO outbox = new RechargeDispatchOutboxDO();
        outbox.setEventId(command.eventId());
        outbox.setEventType(OutboxEventType.SUPPLIER_QUERY.name());
        outbox.setOrgCode(command.orgCode());
        outbox.setOrderId(command.orderId());
        outbox.setOutOrderSn(command.outOrderSn());
        outbox.setPayload(supplierQueryPayload(command));
        outbox.setStatus(TaskStatus.INIT.name());
        outbox.setRetryCount(0);
        outbox.setMaxRetryCount(command.maxRetryCount());
        outbox.setNextRetryTime(now.plusNanos(command.initialDelay().toNanos()));
        outbox.setLockedBy(null);
        outbox.setLockedUntil(null);
        outbox.setVersion(0L);
        outbox.setLastError("");
        outbox.setShardKey(command.eventId());
        outbox.setCreatedAt(now);
        outbox.setUpdatedAt(now);
        writer.insertIfAbsent(outbox);
    }

    private RechargeOutboxEvent toEvent(RechargeDispatchOutboxDO outbox) {
        return new RechargeOutboxEvent(
                outbox.getEventId(),
                OutboxEventType.valueOf(outbox.getEventType()),
                outbox.getOrgCode(),
                outbox.getOrderId(),
                outbox.getOutOrderSn(),
                outbox.getPayload(),
                valueOrZero(outbox.getRetryCount()),
                outbox.getMaxRetryCount() == null ? DEFAULT_MAX_RETRY_COUNT : outbox.getMaxRetryCount(),
                outbox.getVersion() == null ? 0L : outbox.getVersion(),
                outbox.getNextRetryTime(),
                outbox.getLastError(),
                OutboxEventType.INVALID_ORDER.name().equals(outbox.getEventType()) ? attempt(outbox.getPayload(), 0) : null);
    }

    private String invalidPayload(InvalidRechargeOrderOutboxCommand command) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "org_code", command.orgCode(),
                    "order_id", command.orderId(),
                    "out_order_sn", command.outOrderSn(),
                    "reason", command.reason() == null ? "" : command.reason(),
                    InvalidationAttempt.PAYLOAD_KEY, List.of(pendingAttempt(invalidEventId(command.orderId()), 1,
                            command.deadlineEpochMs()))));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("serialize invalid order outbox payload failed", ex);
        }
    }

    private String supplierQueryPayload(SupplierQueryOutboxCommand command) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "org_code", command.orgCode(),
                    "order_id", command.orderId(),
                    "out_order_sn", command.outOrderSn(),
                    "account", command.account(),
                    "supplier_channel", command.supplierChannel(),
                    "source_payload", command.payload() == null ? "{}" : command.payload()));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("serialize supplier query outbox payload failed", ex);
        }
    }

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(clock.instant(), LEGACY_CARDSHOP_DB_ZONE);
    }

    private RechargeDispatchOutboxMapper requireMapper() {
        if (mapper == null) {
            throw new IllegalStateException("RechargeDispatchOutboxMapper is required for this operation");
        }
        return mapper;
    }

    private static int valueOrZero(Integer value) {
        return value == null ? 0 : value;
    }

    private static String truncate(String value, int maxLength) {
        if (value == null) {
            return "";
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
