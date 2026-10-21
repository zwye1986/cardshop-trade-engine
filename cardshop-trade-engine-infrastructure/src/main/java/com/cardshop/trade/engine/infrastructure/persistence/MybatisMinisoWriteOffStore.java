package com.cardshop.trade.engine.infrastructure.persistence;

import static com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.Reason.*;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.persistence.MinisoWriteOffOrderLocks.Owned;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoWriteOffEventMapper.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.*;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.transaction.*;
import org.springframework.transaction.support.*;

import java.time.*;
import java.time.format.*;
import java.util.*;

/** 名创持久接收T1、原子处理T2及独立退避T3；参见docs/交行直充业务系统详细设计.md「9.8」。
 * 已知归属按主单、扩展、凭据、guard、事件、任务加锁；未知接收仅guard及事件，不再升级主单锁。
 */
public final class MybatisMinisoWriteOffStore implements MinisoWriteOffStore {
    private static final Logger LOG = LoggerFactory.getLogger(MybatisMinisoWriteOffStore.class);
    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
                    .withResolverStyle(ResolverStyle.STRICT);
    private static final String INIT = "INIT",
            RETRY = "RETRY",
            PROCESSED = "PROCESSED",
            MANUAL = "MANUAL_REQUIRED";
    private static final int VOID = 3, SUCCESS = 12, MAX_NOTIFY_RETRIES = 100;
    private static final ObjectMapper JSON = new ObjectMapper();
    private final MinisoWriteOffOrderLocks orderLocks;
    private final MinisoWriteOffObserver observer;
    private final MinisoWriteOffEventMapper events;
    private final MinisoReceiptMapper receipts;
    private final MinisoCouponGuardMapper guards;
    private final RechargeNotifyTaskPort tasks;
    private final V12RedemptionNotifyPayloadSigner signer;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final int maxRetries;
    private final Duration backoff;
    private final LogStormGuard logs = LogStormGuard.defaultGuard();

    public MybatisMinisoWriteOffStore(
            MinisoWriteOffEventMapper events,
            MinisoReceiptMapper receipts,
            MinisoCouponGuardMapper guards,
            RechargeNotifyTaskPort tasks,
            V12RedemptionNotifyPayloadSigner signer,
            PlatformTransactionManager tm,
            Clock clock,
            int maxRetries,
            Duration backoff,
            int timeoutSeconds) {
        this(
                events,
                receipts,
                guards,
                tasks,
                signer,
                tm,
                clock,
                maxRetries,
                backoff,
                timeoutSeconds,
                MinisoWriteOffObserver.NOOP);
    }

    public MybatisMinisoWriteOffStore(
            MinisoWriteOffEventMapper events,
            MinisoReceiptMapper receipts,
            MinisoCouponGuardMapper guards,
            RechargeNotifyTaskPort tasks,
            V12RedemptionNotifyPayloadSigner signer,
            PlatformTransactionManager tm,
            Clock clock,
            int maxRetries,
            Duration backoff,
            int timeoutSeconds,
            MinisoWriteOffObserver observer) {
        this.observer = Objects.requireNonNull(observer);
        this.events = Objects.requireNonNull(events);
        this.guards = Objects.requireNonNull(guards);
        this.orderLocks = new MinisoWriteOffOrderLocks(events, receipts, guards);
        this.receipts = Objects.requireNonNull(receipts);
        this.tasks = Objects.requireNonNull(tasks);
        this.signer = Objects.requireNonNull(signer);
        this.clock = Objects.requireNonNull(clock);
        this.maxRetries = maxRetries;
        this.backoff = Objects.requireNonNull(backoff);
        if (maxRetries < 1 || backoff.isNegative() || backoff.isZero() || timeoutSeconds < 1)
            throw new IllegalArgumentException("Invalid MINISO retry settings");
        tx = new TransactionTemplate(tm);
        tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        tx.setTimeout(timeoutSeconds);
    }

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(clock.instant(), ZONE);
    }

    /** T1只保存已认证事件；接收方法返回时独立事务已经提交，重复事实不重置处理进度。 */
    @Override
    public void accept(MinisoWriteOffNotification notice) {
        try {
            boolean duplicate =
                    Boolean.TRUE.equals(
                            tx.execute(
                                    s -> {
                                        var candidate =
                                                receipts.selectByCoupon(notice.couponCode());
                                        Owned owned =
                                                candidate == null
                                                        ? null
                                                        : lockOwned(candidate.orderId());
                                        // 未知归属只能锁同券guard及事件；凭据即使随后出现也留待下一事务定位。
                                        if (owned == null) guards.lockInTransaction(notice.couponCode(), now());
                                        var previous = events.lock(notice.eventId());
                                        events.insertIfAbsent(
                                                notice.eventId(),
                                                notice.couponCode(),
                                                notice.useTime(),
                                                owned == null ? null : owned.order().orderId(),
                                                owned == null ? null : owned.order().orgCode(),
                                                now());
                                        var saved = events.lock(notice.eventId());
                                        if (saved == null
                                                || !saved.couponCode().equals(notice.couponCode())
                                                || !saved.useTime().equals(notice.useTime()))
                                            throw new MinisoWriteOffException(CONFLICT);
                                        return previous != null;
                                    }));
            observer.event(duplicate ? "duplicate" : "accepted");
        } catch (MinisoWriteOffException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw MinisoWriteOffFailures.storage(ex);
        }
    }

    /** 到期事件逐笔T2执行，异常回滚后T3按version登记，不能覆盖其它节点已提交结果。 */
    @Override
    public MinisoWriteOffBatchResult processBatch(int shards, int shard, int limit, String worker) {
        if (shards < 1
                || shards > 1024
                || shard < 0
                || shard >= shards
                || limit < 1
                || limit > 1000
                || worker == null
                || worker.isBlank())
            throw new IllegalArgumentException("Invalid MINISO job parameters");
        List<Event> due;
        try {
            due = events.due(now(), shards, shard, limit);
        } catch (RuntimeException ex) {
            throw MinisoWriteOffFailures.storage(ex);
        }
        int processed = 0, retried = 0, manual = 0, deferred = 0, failed = 0;
        for (var candidate : due) {
            try {
                boolean completed = Boolean.TRUE.equals(tx.execute(s -> process(candidate)));
                if (completed) processed++;
                else deferred++;
            } catch (ConcurrencyFailureException ex) {
                // 多节点抢同单属于预期竞争，采样warn保留安全栈，不消耗业务重试次数。
                report(
                        "LOCK_BUSY",
                        candidate.eventId(),
                        new MinisoWriteOffException(ORDER_PENDING),
                        true);
                deferred++;
            } catch (RuntimeException ex) {
                var safe =
                        ex instanceof MinisoWriteOffException e
                                ? e
                                : MinisoWriteOffFailures.storage(ex);
                var reason = safe.reason();
                boolean isManual = !reason.retryable() || candidate.retryCount() + 1 >= maxRetries;
                report(
                        reason.name(),
                        candidate.eventId(),
                        safe,
                        reason.retryable() && reason != DB_UNAVAILABLE);
                try {
                    int rows =
                            Objects.requireNonNull(
                                    tx.execute(
                                            s ->
                                                    events.reschedule(
                                                            candidate.eventId(),
                                                            candidate.version(),
                                                            candidate.retryCount() + 1,
                                                            isManual ? MANUAL : RETRY,
                                                            now().plus(backoff),
                                                            reason.name(),
                                                            now())));
                    if (rows == 0) deferred++;
                    else if (isManual) manual++;
                    else retried++;
                } catch (RuntimeException retryFailure) {
                    report(
                            "RETRY_WRITE_FAILED",
                            candidate.eventId(),
                            MinisoWriteOffFailures.storage(retryFailure),
                            false);
                    failed++;
                }
            }
        }
        var result =
                new MinisoWriteOffBatchResult(
                        due.size(), processed, retried, manual, deferred, failed);
        observer.batch(result);
        return result;
    }

    /** T2只做本地事实/任务原子提交；任何失败先回滚，再由外层T3按version退避。 */
    private boolean process(Event candidate) {
        var initial = receipts.selectByCoupon(candidate.couponCode());
        Owned owned = initial == null ? null : lockOwned(initial.orderId());
        if (owned == null) guards.lockInTransaction(candidate.couponCode(), now());
        var event = events.lock(candidate.eventId());
        if (event == null
                || event.version() != candidate.version()
                || !List.of(INIT, RETRY).contains(event.status())
                || event.nextRetryTime().isAfter(now())) return false;
        if (initial == null) throw new MinisoWriteOffException(RECEIPT_PENDING);
        if (owned == null || !owned.receipt().couponCode().equals(event.couponCode()))
            throw new MinisoWriteOffException(OWNERSHIP_CONFLICT);
        var order = owned.order();
        var receipt = owned.receipt();
        if (receipt.couponStatus() == VOID) throw new MinisoWriteOffException(REDEEM_VOID_CONFLICT);
        if (order.orderStatus()
                        == com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode.FAILED
                                .code()
                || order.orderStatus()
                        == com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode.REFUNDED
                                .code()) throw new MinisoWriteOffException(REDEEM_VOID_CONFLICT);
        if (order.orderStatus() != SUCCESS) throw new MinisoWriteOffException(ORDER_PENDING);
        var used = receipts.useTime(order.orderId());
        if (used != null && !used.equals(event.useTime()))
            throw new MinisoWriteOffException(USE_TIME_CONFLICT);
        try {
            if (event.useTime().isBefore(LocalDateTime.parse(receipt.beginTime(), TIME))
                    || event.useTime().isAfter(LocalDateTime.parse(receipt.endTime(), TIME)))
                throw new MinisoWriteOffException(USE_TIME_OUT_OF_RANGE);
        } catch (DateTimeParseException ex) {
            throw new MinisoWriteOffException(USE_TIME_OUT_OF_RANGE);
        }
        if (order.notifyUrl() != null && !order.notifyUrl().isBlank()) createTask(order, event);
        if (receipts.redeem(order.orderId(), event.useTime()) != 1)
            throw new MinisoWriteOffException(DB_UNAVAILABLE);
        if (events.redeemVoucher(order.orderId(), order.orgCode(), event.useTime(), now()) != 1)
            throw new MinisoWriteOffException(DB_UNAVAILABLE);
        if (events.complete(event.eventId(), order.orderId(), order.orgCode(), now()) != 1)
            throw new MinisoWriteOffException(DB_UNAVAILABLE);
        return true;
    }

    /** 已有任务保留历史密钥和原文，不能用当前版本重新签名；参见docs/交行直充业务系统详细设计.md「9.5」。 */
    private void createTask(OrderFacts order, Event event) {
        String taskId =
                "notify-miniso-redeemed-"
                        + MinisoWriteOffNotification.digest(
                                order.orderId() + ":" + event.eventId());
        var existing = events.task(taskId);
        if (existing != null) {
            try {
                var payload = JSON.readTree(existing.payload());
                if (!order.orgCode().equals(existing.orgCode())
                        || !order.orderId().equals(existing.orderId())
                        || !order.outOrderSn().equals(existing.outOrderSn())
                        || !"REDEEMED".equals(existing.bizStatus())
                        || !TIME.format(event.useTime()).equals(payload.path("use_time").asText())
                        || !V12RedemptionNotifyPayloadSigner.STATE_TYPE_GENERAL.equals(
                                payload.path("state_type").asText())
                        || !V12RedemptionNotifyPayloadSigner.USE_STATE_REDEEMED.equals(
                                payload.path("use_state").asText())
                        || !order.orderId().equals(payload.path("order_id").asText())
                        || !order.outOrderSn().equals(payload.path("out_order_sn").asText())
                        || !order.account().equals(payload.path("account").asText())
                        || !payload.has("invalid_time")
                        || !payload.path("invalid_time").asText().isEmpty())
                    throw new MinisoWriteOffException(NOTIFY_TASK_CONFLICT);
            } catch (JsonProcessingException ex) {
                throw new MinisoWriteOffException(NOTIFY_TASK_CONFLICT);
            }
            return;
        }
        var facts =
                new RechargeFulfillmentOrder(
                        order.orderId(),
                        order.outOrderSn(),
                        order.orgCode(),
                        order.account(),
                        "",
                        "",
                        "MINISO",
                        0,
                        order.notifyUrl(),
                        order.orderStatus(),
                        "DB_PERSISTED",
                        "PASSED",
                        true,
                        true);
        tasks.insertIfAbsent(
                new RechargeNotifyTaskDraft(
                        taskId,
                        "VOUCHER_STATE",
                        order.orgCode(),
                        order.orderId(),
                        order.outOrderSn(),
                        order.notifyUrl(),
                        "REDEEMED",
                        signer.sign(facts, event.useTime(), clock.instant()),
                        MAX_NOTIFY_RETRIES,
                        now()));
    }

    /** 定位后重新核验，顺序与履约事务一致；参见docs/交行直充业务系统详细设计.md「9.5」。 */
    private Owned lockOwned(String id) {
        return orderLocks.lock(id, now());
    }

    private void report(String category, String event, RuntimeException safe, boolean expected) {
        var decision = logs.shouldLog("miniso.writeoff." + category);
        if (decision.shouldLog()) {
            if (expected)
                LOG.warn(
                        "名创核销暂缓 eventId={}, category={}, occurrence={}",
                        event,
                        category,
                        decision.occurrence(),
                        safe);
            else
                LOG.error(
                        "名创核销处理失败 eventId={}, category={}, occurrence={}",
                        event,
                        category,
                        decision.occurrence(),
                        safe);
        }
    }
}
