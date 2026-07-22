package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyBatchResult;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerService;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxBatchResult;
import com.cardshop.trade.engine.job.recharge.AcceptAnomalyAutoCheckJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceDeltaFlushJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceReconcileJob;
import com.cardshop.trade.engine.job.recharge.RechargeCacheRepairJob;
import com.cardshop.trade.engine.job.recharge.RechargeOutboxPublishJob;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import com.cardshop.trade.engine.job.recharge.RechargeSupplierDispatchJob;
import com.cardshop.trade.engine.job.recharge.RechargeSupplierQueryJob;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.util.StringUtils;

/**
 * 交易引擎全部 XXL-Job Handler 的统一注册入口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「17. 分片补偿与对账任务」。本类只做调度框架适配：
 * 解析 XXL-Job 的分片、批量和 worker 参数，然后委托已有的 framework-free job facade 或 worker service。
 * Redis pending 扫描、MySQL CAS 抢占、余额刷账、缓存修复和异常核查等业务逻辑仍保留在 application/job 层。
 *
 * <p>这里故意用 {@link ObjectProvider} 晚绑定每个任务，而不是用 {@code @ConditionalOnBean} 决定是否注册
 * handler。test188 曾出现 executor 已上线但 {@code rechargePersistMqRepublishJob} 报 {@code handler not found}：
 * 根因是 job facade 由后置 auto-configuration 创建，普通配置类判断条件过早。统一先注册 handler，可以让调度中心
 * 始终找到任务名；若某个任务依赖确实缺失，触发时返回明确失败，而不是表现为 handler 未注册。
 */
public class TradeEngineXxlJobHandlers {

    static final String PERSIST_MQ_REPUBLISH_HANDLER = "rechargePersistMqRepublishJob";
    static final String OUTBOX_PUBLISH_HANDLER = "rechargeOutboxPublishJob";
    static final String SUPPLIER_DISPATCH_HANDLER = "rechargeSupplierDispatchJob";
    static final String SUPPLIER_QUERY_HANDLER = "rechargeSupplierQueryJob";
    static final String NOTIFY_RETRY_HANDLER = "rechargeNotifyRetryJob";
    static final String BALANCE_DELTA_FLUSH_HANDLER = "rechargeBalanceDeltaFlushJob";
    static final String BALANCE_RECONCILE_HANDLER = "rechargeBalanceReconcileJob";
    static final String CACHE_REPAIR_HANDLER = "rechargeCacheRepairJob";
    static final String ACCEPT_ANOMALY_AUTO_CHECK_HANDLER = "acceptAnomalyAutoCheckJob";

    private static final Logger log = LoggerFactory.getLogger(TradeEngineXxlJobHandlers.class);
    private static final Pattern PARAM_SEPARATOR = Pattern.compile("[,;\\s]+");
    private static final Pattern WORKER_ID_PATTERN = Pattern.compile("[A-Za-z0-9._:-]{1,120}");
    private static final Pattern WORKER_ID_SAFE_CHARS = Pattern.compile("[^A-Za-z0-9._:-]");
    private static final int DEFAULT_HANDLER_LIMIT = 0;
    private static final int MAX_WORKER_ID_LENGTH = 120;

    private static final List<OutboxEventType> ALL_FULFILLMENT_EVENT_TYPES = List.of(
            OutboxEventType.SUPPLIER_DISPATCH,
            OutboxEventType.SUPPLIER_QUERY,
            OutboxEventType.INVALID_ORDER);
    private static final List<OutboxEventType> DEFAULT_PUBLISH_EVENT_TYPES =
            List.of(OutboxEventType.SUPPLIER_DISPATCH);
    private static final List<OutboxEventType> QUERY_JOB_EVENT_TYPES = List.of(
            OutboxEventType.SUPPLIER_QUERY,
            OutboxEventType.INVALID_ORDER);

    private final Supplier<RechargePersistMqRepublishJob> persistMqRepublishJob;
    private final Supplier<RechargeOutboxPublishJob> outboxPublishJob;
    private final Supplier<RechargeSupplierDispatchJob> supplierDispatchJob;
    private final Supplier<RechargeSupplierQueryJob> supplierQueryJob;
    private final Supplier<RechargeNotifyWorkerService> notifyWorkerService;
    private final Supplier<RechargeBalanceDeltaFlushJob> balanceDeltaFlushJob;
    private final Supplier<RechargeBalanceReconcileJob> balanceReconcileJob;
    private final Supplier<RechargeCacheRepairJob> cacheRepairJob;
    private final Supplier<AcceptAnomalyAutoCheckJob> acceptAnomalyAutoCheckJob;

    public TradeEngineXxlJobHandlers(
            ObjectProvider<RechargePersistMqRepublishJob> persistMqRepublishJobProvider,
            ObjectProvider<RechargeOutboxPublishJob> outboxPublishJobProvider,
            ObjectProvider<RechargeSupplierDispatchJob> supplierDispatchJobProvider,
            ObjectProvider<RechargeSupplierQueryJob> supplierQueryJobProvider,
            ObjectProvider<RechargeNotifyWorkerService> notifyWorkerServiceProvider,
            ObjectProvider<RechargeBalanceDeltaFlushJob> balanceDeltaFlushJobProvider,
            ObjectProvider<RechargeBalanceReconcileJob> balanceReconcileJobProvider,
            ObjectProvider<RechargeCacheRepairJob> cacheRepairJobProvider,
            ObjectProvider<AcceptAnomalyAutoCheckJob> acceptAnomalyAutoCheckJobProvider) {
        this(
                persistMqRepublishJobProvider::getIfAvailable,
                outboxPublishJobProvider::getIfAvailable,
                supplierDispatchJobProvider::getIfAvailable,
                supplierQueryJobProvider::getIfAvailable,
                notifyWorkerServiceProvider::getIfAvailable,
                balanceDeltaFlushJobProvider::getIfAvailable,
                balanceReconcileJobProvider::getIfAvailable,
                cacheRepairJobProvider::getIfAvailable,
                acceptAnomalyAutoCheckJobProvider::getIfAvailable);
    }

    TradeEngineXxlJobHandlers(
            Supplier<RechargePersistMqRepublishJob> persistMqRepublishJob,
            Supplier<RechargeOutboxPublishJob> outboxPublishJob,
            Supplier<RechargeSupplierDispatchJob> supplierDispatchJob,
            Supplier<RechargeSupplierQueryJob> supplierQueryJob,
            Supplier<RechargeNotifyWorkerService> notifyWorkerService,
            Supplier<RechargeBalanceDeltaFlushJob> balanceDeltaFlushJob,
            Supplier<RechargeBalanceReconcileJob> balanceReconcileJob,
            Supplier<RechargeCacheRepairJob> cacheRepairJob,
            Supplier<AcceptAnomalyAutoCheckJob> acceptAnomalyAutoCheckJob) {
        this.persistMqRepublishJob = Objects.requireNonNull(
                persistMqRepublishJob,
                "persistMqRepublishJob must not be null");
        this.outboxPublishJob = Objects.requireNonNull(outboxPublishJob, "outboxPublishJob must not be null");
        this.supplierDispatchJob = Objects.requireNonNull(supplierDispatchJob, "supplierDispatchJob must not be null");
        this.supplierQueryJob = Objects.requireNonNull(supplierQueryJob, "supplierQueryJob must not be null");
        this.notifyWorkerService = Objects.requireNonNull(
                notifyWorkerService,
                "notifyWorkerService must not be null");
        this.balanceDeltaFlushJob = Objects.requireNonNull(
                balanceDeltaFlushJob,
                "balanceDeltaFlushJob must not be null");
        this.balanceReconcileJob = Objects.requireNonNull(
                balanceReconcileJob,
                "balanceReconcileJob must not be null");
        this.cacheRepairJob = Objects.requireNonNull(cacheRepairJob, "cacheRepairJob must not be null");
        this.acceptAnomalyAutoCheckJob = Objects.requireNonNull(
                acceptAnomalyAutoCheckJob,
                "acceptAnomalyAutoCheckJob must not be null");
    }

    /**
     * Redis pending MQ 补投调度入口。
     *
     * <p>参见详细设计「10.1 XXL-Job Handler 契约」的 {@code rechargePersistMqRepublishJob}。
     */
    @XxlJob(PERSIST_MQ_REPUBLISH_HANDLER)
    public void executePersistMqRepublish() {
        execute(PERSIST_MQ_REPUBLISH_HANDLER, context -> compensationMessage(
                PERSIST_MQ_REPUBLISH_HANDLER,
                context,
                runPersistMqRepublishFromXxlContext(
                        context.shardTotal(),
                        context.shardIndex(),
                        context.jobId(),
                        context.jobParam())));
    }

    /**
     * 通用 outbox 推进调度入口。
     *
     * <p>参见详细设计「10.1 XXL-Job Handler 契约」的 {@code rechargeOutboxPublishJob}。
     * 默认仅 {@link OutboxEventType#SUPPLIER_DISPATCH}；参数 {@code eventTypes=ALL} 可恢复旧混扫。
     */
    @XxlJob(OUTBOX_PUBLISH_HANDLER)
    public void executeOutboxPublish() {
        execute(OUTBOX_PUBLISH_HANDLER, context -> outboxMessage(
                OUTBOX_PUBLISH_HANDLER,
                context,
                runOutboxPublishFromXxlContext(
                        context.shardTotal(),
                        context.shardIndex(),
                        context.jobId(),
                        context.jobParam())));
    }

    /**
     * 供应商派单 outbox 调度入口。
     *
     * <p>参见 {@code docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md}。
     */
    @XxlJob(SUPPLIER_DISPATCH_HANDLER)
    public void executeSupplierDispatch() {
        execute(SUPPLIER_DISPATCH_HANDLER, context -> outboxMessage(
                SUPPLIER_DISPATCH_HANDLER,
                context,
                runSupplierDispatchFromXxlContext(
                        context.shardTotal(),
                        context.shardIndex(),
                        context.jobId(),
                        context.jobParam())));
    }

    /**
     * 供应商查单与作废 outbox 调度入口。
     *
     * <p>参见 {@code docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md}。
     */
    @XxlJob(SUPPLIER_QUERY_HANDLER)
    public void executeSupplierQuery() {
        execute(SUPPLIER_QUERY_HANDLER, context -> outboxMessage(
                SUPPLIER_QUERY_HANDLER,
                context,
                runSupplierQueryFromXxlContext(
                        context.shardTotal(),
                        context.shardIndex(),
                        context.jobId(),
                        context.jobParam())));
    }

    /**
     * 商户回调重试调度入口。
     *
     * <p>参见高可用设计「17.2 分片规则」的 {@code rechargeNotifyRetryJob}。当前 worker 首版按到期任务抢占，
     * 不消费 shard 参数；handler 仍记录 shardTotal/shardIndex，便于后续把 task_id 分片下推到 repository。
     */
    @XxlJob(NOTIFY_RETRY_HANDLER)
    public void executeNotifyRetry() {
        execute(NOTIFY_RETRY_HANDLER, context -> notifyMessage(
                NOTIFY_RETRY_HANDLER,
                context,
                runNotifyRetryFromXxlContext(
                        context.shardTotal(),
                        context.shardIndex(),
                        context.jobId(),
                        context.jobParam())));
    }

    /**
     * 余额 delta 刷账调度入口。
     *
     * <p>参见详细设计「10.1 XXL-Job Handler 契约」的 {@code rechargeBalanceDeltaFlushJob}。
     */
    @XxlJob(BALANCE_DELTA_FLUSH_HANDLER)
    public void executeBalanceDeltaFlush() {
        execute(BALANCE_DELTA_FLUSH_HANDLER, context -> compensationMessage(
                BALANCE_DELTA_FLUSH_HANDLER,
                context,
                runBalanceDeltaFlushFromXxlContext(
                        context.shardTotal(),
                        context.shardIndex(),
                        context.jobId(),
                        context.jobParam())));
    }

    /**
     * 余额对账调度入口。
     *
     * <p>参见详细设计「10.1 XXL-Job Handler 契约」的 {@code rechargeBalanceReconcileJob}。
     */
    @XxlJob(BALANCE_RECONCILE_HANDLER)
    public void executeBalanceReconcile() {
        execute(BALANCE_RECONCILE_HANDLER, context -> compensationMessage(
                BALANCE_RECONCILE_HANDLER,
                context,
                runBalanceReconcileFromXxlContext(
                        context.shardTotal(),
                        context.shardIndex(),
                        context.jobId(),
                        context.jobParam())));
    }

    /**
     * Redis 缓存修复调度入口。
     *
     * <p>参见详细设计「10.1 XXL-Job Handler 契约」的 {@code rechargeCacheRepairJob}。
     */
    @XxlJob(CACHE_REPAIR_HANDLER)
    public void executeCacheRepair() {
        execute(CACHE_REPAIR_HANDLER, context -> compensationMessage(
                CACHE_REPAIR_HANDLER,
                context,
                runCacheRepairFromXxlContext(
                        context.shardTotal(),
                        context.shardIndex(),
                        context.jobId(),
                        context.jobParam())));
    }

    /**
     * Redis 接单异常自动核查调度入口。
     *
     * <p>参见详细设计「10.1 XXL-Job Handler 契约」的 {@code acceptAnomalyAutoCheckJob}。该任务当前按异常记录
     * 到期顺序处理，不消费 shard 参数；handler 保留 shard 日志上下文，避免后续分片下推时改变调度参数格式。
     */
    @XxlJob(ACCEPT_ANOMALY_AUTO_CHECK_HANDLER)
    public void executeAcceptAnomalyAutoCheck() {
        execute(ACCEPT_ANOMALY_AUTO_CHECK_HANDLER, context -> anomalyMessage(
                ACCEPT_ANOMALY_AUTO_CHECK_HANDLER,
                context,
                runAcceptAnomalyAutoCheckFromXxlContext(
                        context.shardTotal(),
                        context.shardIndex(),
                        context.jobId(),
                        context.jobParam())));
    }

    RechargeCompensationBatchResult runPersistMqRepublishFromXxlContext(
            int shardTotal,
            int shardIndex,
            long jobId,
            String jobParam) {
        XxlRunOptions options = XxlRunOptions.parse(jobId, jobParam);
        logStart(PERSIST_MQ_REPUBLISH_HANDLER, shardTotal, shardIndex, options);
        RechargeCompensationBatchResult result = requireJob(
                persistMqRepublishJob,
                "RechargePersistMqRepublishJob").runOnce(
                        shardTotal,
                        shardIndex,
                        options.limit(),
                        options.workerId());
        logCompensationComplete(PERSIST_MQ_REPUBLISH_HANDLER, shardTotal, shardIndex, options, result);
        return result;
    }

    RechargeOutboxBatchResult runOutboxPublishFromXxlContext(
            int shardTotal,
            int shardIndex,
            long jobId,
            String jobParam) {
        OutboxXxlRunOptions options = OutboxXxlRunOptions.parse(jobId, jobParam, DEFAULT_PUBLISH_EVENT_TYPES);
        return runOutboxJob(
                OUTBOX_PUBLISH_HANDLER,
                outboxPublishJob,
                "RechargeOutboxPublishJob",
                shardTotal,
                shardIndex,
                options);
    }

    RechargeOutboxBatchResult runSupplierDispatchFromXxlContext(
            int shardTotal,
            int shardIndex,
            long jobId,
            String jobParam) {
        OutboxXxlRunOptions options = OutboxXxlRunOptions.parse(jobId, jobParam, DEFAULT_PUBLISH_EVENT_TYPES)
                .withEventTypes(DEFAULT_PUBLISH_EVENT_TYPES);
        logOutboxStart(SUPPLIER_DISPATCH_HANDLER, shardTotal, shardIndex, options);
        RechargeOutboxBatchResult result = requireJob(supplierDispatchJob, "RechargeSupplierDispatchJob")
                .runOnce(shardTotal, shardIndex, options.limit(), options.workerId(), options.parallelism());
        logOutboxComplete(SUPPLIER_DISPATCH_HANDLER, shardTotal, shardIndex, options, result);
        return result;
    }

    RechargeOutboxBatchResult runSupplierQueryFromXxlContext(
            int shardTotal,
            int shardIndex,
            long jobId,
            String jobParam) {
        OutboxXxlRunOptions options = OutboxXxlRunOptions.parse(jobId, jobParam, QUERY_JOB_EVENT_TYPES)
                .withEventTypes(QUERY_JOB_EVENT_TYPES);
        logOutboxStart(SUPPLIER_QUERY_HANDLER, shardTotal, shardIndex, options);
        RechargeOutboxBatchResult result = requireJob(supplierQueryJob, "RechargeSupplierQueryJob")
                .runOnce(shardTotal, shardIndex, options.limit(), options.workerId(), options.parallelism());
        logOutboxComplete(SUPPLIER_QUERY_HANDLER, shardTotal, shardIndex, options, result);
        return result;
    }

    private RechargeOutboxBatchResult runOutboxJob(
            String handlerName,
            Supplier<RechargeOutboxPublishJob> jobSupplier,
            String jobName,
            int shardTotal,
            int shardIndex,
            OutboxXxlRunOptions options) {
        logOutboxStart(handlerName, shardTotal, shardIndex, options);
        RechargeOutboxBatchResult result = requireJob(jobSupplier, jobName).runOnce(
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId(),
                options.eventTypes(),
                options.parallelism());
        logOutboxComplete(handlerName, shardTotal, shardIndex, options, result);
        return result;
    }

    RechargeNotifyBatchResult runNotifyRetryFromXxlContext(
            int shardTotal,
            int shardIndex,
            long jobId,
            String jobParam) {
        XxlRunOptions options = XxlRunOptions.parse(jobId, jobParam);
        logStart(NOTIFY_RETRY_HANDLER, shardTotal, shardIndex, options);
        RechargeNotifyBatchResult result = requireJob(notifyWorkerService, "RechargeNotifyWorkerService").processDue(
                options.limit(),
                options.workerId());
        log.info("XXL-Job触发完成 handler={}, shardTotal={}, shardIndex={}, limit={}, workerId={}, batchId={}, "
                        + "scanned={}, claimed={}, succeeded={}, failed={}, skipped={}",
                NOTIFY_RETRY_HANDLER,
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId(),
                options.batchId(),
                result.scanned(),
                result.claimed(),
                result.succeeded(),
                result.failed(),
                result.skipped());
        return result;
    }

    RechargeCompensationBatchResult runBalanceDeltaFlushFromXxlContext(
            int shardTotal,
            int shardIndex,
            long jobId,
            String jobParam) {
        XxlRunOptions options = XxlRunOptions.parse(jobId, jobParam);
        logStart(BALANCE_DELTA_FLUSH_HANDLER, shardTotal, shardIndex, options);
        RechargeCompensationBatchResult result = requireJob(
                balanceDeltaFlushJob,
                "RechargeBalanceDeltaFlushJob").runOnce(
                        shardTotal,
                        shardIndex,
                        options.limit(),
                        options.workerId());
        logCompensationComplete(BALANCE_DELTA_FLUSH_HANDLER, shardTotal, shardIndex, options, result);
        return result;
    }

    RechargeCompensationBatchResult runBalanceReconcileFromXxlContext(
            int shardTotal,
            int shardIndex,
            long jobId,
            String jobParam) {
        XxlRunOptions options = XxlRunOptions.parse(jobId, jobParam);
        logStart(BALANCE_RECONCILE_HANDLER, shardTotal, shardIndex, options);
        RechargeCompensationBatchResult result = requireJob(balanceReconcileJob, "RechargeBalanceReconcileJob").runOnce(
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId());
        logCompensationComplete(BALANCE_RECONCILE_HANDLER, shardTotal, shardIndex, options, result);
        return result;
    }

    RechargeCompensationBatchResult runCacheRepairFromXxlContext(
            int shardTotal,
            int shardIndex,
            long jobId,
            String jobParam) {
        XxlRunOptions options = XxlRunOptions.parse(jobId, jobParam);
        logStart(CACHE_REPAIR_HANDLER, shardTotal, shardIndex, options);
        RechargeCompensationBatchResult result = requireJob(cacheRepairJob, "RechargeCacheRepairJob").runOnce(
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId());
        logCompensationComplete(CACHE_REPAIR_HANDLER, shardTotal, shardIndex, options, result);
        return result;
    }

    AcceptAnomalyCheckBatchResult runAcceptAnomalyAutoCheckFromXxlContext(
            int shardTotal,
            int shardIndex,
            long jobId,
            String jobParam) {
        XxlRunOptions options = XxlRunOptions.parse(jobId, jobParam);
        logStart(ACCEPT_ANOMALY_AUTO_CHECK_HANDLER, shardTotal, shardIndex, options);
        AcceptAnomalyCheckBatchResult result = requireJob(
                acceptAnomalyAutoCheckJob,
                "AcceptAnomalyAutoCheckJob").runOnce(
                        options.limit(),
                        options.workerId());
        log.info("XXL-Job触发完成 handler={}, shardTotal={}, shardIndex={}, limit={}, workerId={}, batchId={}, "
                        + "checked={}, resolvedAccepted={}, resolvedNotAccepted={}, manualRequired={}, failed={}",
                ACCEPT_ANOMALY_AUTO_CHECK_HANDLER,
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId(),
                options.batchId(),
                result.checked(),
                result.resolvedAccepted(),
                result.resolvedNotAccepted(),
                result.manualRequired(),
                result.failed());
        return result;
    }

    private static void execute(String handlerName, XxlAction action) {
        XxlInvocationContext context = XxlInvocationContext.current();
        try {
            String message = action.run(context);
            XxlJobHelper.log(message);
            XxlJobHelper.handleSuccess(message);
        } catch (RuntimeException ex) {
            log.error("XXL-Job触发失败 handler={}, shardTotal={}, shardIndex={}, jobId={}, jobParam={}",
                    handlerName,
                    context.shardTotal(),
                    context.shardIndex(),
                    context.jobId(),
                    context.jobParam(),
                    ex);
            XxlJobHelper.log(ex);
            XxlJobHelper.handleFail(safeMessage(ex));
            throw ex;
        }
    }

    private static void logStart(String handlerName, int shardTotal, int shardIndex, XxlRunOptions options) {
        log.info("XXL-Job触发 handler={}, shardTotal={}, shardIndex={}, limit={}, workerId={}, batchId={}",
                handlerName,
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId(),
                options.batchId());
    }

    private static void logOutboxStart(String handlerName, int shardTotal, int shardIndex, OutboxXxlRunOptions options) {
        log.info(
                "XXL-Job触发 handler={}, shardTotal={}, shardIndex={}, limit={}, workerId={}, batchId={}, "
                        + "parallelism={}, eventTypes={}",
                handlerName,
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId(),
                options.batchId(),
                options.parallelism(),
                formatEventTypes(options.eventTypes()));
    }

    private static void logOutboxComplete(
            String handlerName,
            int shardTotal,
            int shardIndex,
            OutboxXxlRunOptions options,
            RechargeOutboxBatchResult result) {
        log.info("XXL-Job触发完成 handler={}, shardTotal={}, shardIndex={}, limit={}, workerId={}, batchId={}, "
                        + "parallelism={}, eventTypes={}, scanned={}, claimed={}, succeeded={}, failed={}, skipped={}, "
                        + "deferred={}",
                handlerName,
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId(),
                options.batchId(),
                options.parallelism(),
                formatEventTypes(options.eventTypes()),
                result.scanned(),
                result.claimed(),
                result.succeeded(),
                result.failed(),
                result.skipped(),
                result.deferred());
    }

    private static String formatEventTypes(List<OutboxEventType> eventTypes) {
        return eventTypes.stream().map(OutboxEventType::name).reduce((left, right) -> left + "," + right).orElse("");
    }

    private static void logCompensationComplete(
            String handlerName,
            int shardTotal,
            int shardIndex,
            XxlRunOptions options,
            RechargeCompensationBatchResult result) {
        log.info("XXL-Job触发完成 handler={}, shardTotal={}, shardIndex={}, limit={}, workerId={}, batchId={}, "
                        + "scanned={}, succeeded={}, failed={}, skipped={}",
                handlerName,
                shardTotal,
                shardIndex,
                options.limit(),
                options.workerId(),
                options.batchId(),
                result.scanned(),
                result.succeeded(),
                result.failed(),
                result.skipped());
    }

    private static String compensationMessage(
            String handlerName,
            XxlInvocationContext context,
            RechargeCompensationBatchResult result) {
        XxlRunOptions options = XxlRunOptions.parse(context.jobId(), context.jobParam());
        return "%s finished shardTotal=%d, shardIndex=%d, limit=%d, workerId=%s, batchId=%s, scanned=%d, succeeded=%d, failed=%d, skipped=%d"
                .formatted(
                        handlerName,
                        context.shardTotal(),
                        context.shardIndex(),
                        options.limit(),
                        options.workerId(),
                        options.batchId(),
                        result.scanned(),
                        result.succeeded(),
                        result.failed(),
                        result.skipped());
    }

    private static String outboxMessage(String handlerName, XxlInvocationContext context, RechargeOutboxBatchResult result) {
        List<OutboxEventType> defaultEventTypes = defaultEventTypesForHandler(handlerName);
        OutboxXxlRunOptions options = OutboxXxlRunOptions.parse(
                context.jobId(), context.jobParam(), defaultEventTypes);
        return "%s finished shardTotal=%d, shardIndex=%d, limit=%d, workerId=%s, batchId=%s, parallelism=%d, "
                        + "eventTypes=%s, scanned=%d, claimed=%d, succeeded=%d, failed=%d, skipped=%d, deferred=%d"
                .formatted(
                        handlerName,
                        context.shardTotal(),
                        context.shardIndex(),
                        options.limit(),
                        options.workerId(),
                        options.batchId(),
                        options.parallelism(),
                        formatEventTypes(options.eventTypes()),
                        result.scanned(),
                        result.claimed(),
                        result.succeeded(),
                        result.failed(),
                        result.skipped(),
                        result.deferred());
    }

    private static List<OutboxEventType> defaultEventTypesForHandler(String handlerName) {
        if (SUPPLIER_QUERY_HANDLER.equals(handlerName)) {
            return QUERY_JOB_EVENT_TYPES;
        }
        return DEFAULT_PUBLISH_EVENT_TYPES;
    }

    private static String notifyMessage(String handlerName, XxlInvocationContext context, RechargeNotifyBatchResult result) {
        XxlRunOptions options = XxlRunOptions.parse(context.jobId(), context.jobParam());
        return "%s finished shardTotal=%d, shardIndex=%d, limit=%d, workerId=%s, batchId=%s, scanned=%d, claimed=%d, succeeded=%d, failed=%d, skipped=%d"
                .formatted(
                        handlerName,
                        context.shardTotal(),
                        context.shardIndex(),
                        options.limit(),
                        options.workerId(),
                        options.batchId(),
                        result.scanned(),
                        result.claimed(),
                        result.succeeded(),
                        result.failed(),
                        result.skipped());
    }

    private static String anomalyMessage(
            String handlerName,
            XxlInvocationContext context,
            AcceptAnomalyCheckBatchResult result) {
        XxlRunOptions options = XxlRunOptions.parse(context.jobId(), context.jobParam());
        return "%s finished shardTotal=%d, shardIndex=%d, limit=%d, workerId=%s, batchId=%s, checked=%d, resolvedAccepted=%d, resolvedNotAccepted=%d, manualRequired=%d, failed=%d"
                .formatted(
                        handlerName,
                        context.shardTotal(),
                        context.shardIndex(),
                        options.limit(),
                        options.workerId(),
                        options.batchId(),
                        result.checked(),
                        result.resolvedAccepted(),
                        result.resolvedNotAccepted(),
                        result.manualRequired(),
                        result.failed());
    }

    private static <T> T requireJob(Supplier<T> supplier, String jobName) {
        T job = supplier.get();
        if (job == null) {
            throw new IllegalStateException(jobName + " is not available for XXL-Job trigger");
        }
        return job;
    }

    private static String safeMessage(RuntimeException ex) {
        String message = ex.getMessage();
        return StringUtils.hasText(message) ? message : ex.getClass().getSimpleName();
    }

    @FunctionalInterface
    private interface XxlAction {
        String run(XxlInvocationContext context);
    }

    private record XxlInvocationContext(int shardTotal, int shardIndex, long jobId, String jobParam) {

        static XxlInvocationContext current() {
            return new XxlInvocationContext(
                    XxlJobHelper.getShardTotal(),
                    XxlJobHelper.getShardIndex(),
                    XxlJobHelper.getJobId(),
                    XxlJobHelper.getJobParam());
        }
    }

    record XxlRunOptions(int limit, String workerId, String batchId) {

        static XxlRunOptions parse(long jobId, String jobParam) {
            return parseParams(jobId, jobParam);
        }

        private static XxlRunOptions parseParams(long jobId, String jobParam) {
            Map<String, String> params = parseParamMap(jobParam);
            String batchId = valueOrDefault(params.get("batchid"), "job-" + jobId);
            int limit = parseLimit(params.get("limit"));
            String workerId = params.get("workerid");
            if (!StringUtils.hasText(workerId)) {
                workerId = defaultWorkerId(batchId);
            }
            workerId = validateWorkerId(workerId);
            return new XxlRunOptions(limit, workerId, batchId);
        }

        private static Map<String, String> parseParamMap(String jobParam) {
            Map<String, String> params = new LinkedHashMap<>();
            if (!StringUtils.hasText(jobParam)) {
                return params;
            }
            for (String token : PARAM_SEPARATOR.split(jobParam.trim())) {
                if (!StringUtils.hasText(token)) {
                    continue;
                }
                int equalsIndex = token.indexOf('=');
                if (equalsIndex <= 0 || equalsIndex == token.length() - 1) {
                    throw new IllegalArgumentException("XXL-Job参数必须使用 key=value 格式: " + token);
                }
                String key = token.substring(0, equalsIndex).trim().toLowerCase(Locale.ROOT);
                String value = token.substring(equalsIndex + 1).trim();
                params.put(key, value);
            }
            return params;
        }

        private static int parseLimit(String value) {
            if (!StringUtils.hasText(value)) {
                return DEFAULT_HANDLER_LIMIT;
            }
            try {
                int parsed = Integer.parseInt(value);
                if (parsed < 0) {
                    throw new IllegalArgumentException("limit must be non-negative");
                }
                return parsed;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("limit must be an integer", ex);
            }
        }

        private static String validateWorkerId(String workerId) {
            String value = workerId.trim();
            if (!WORKER_ID_PATTERN.matcher(value).matches()) {
                throw new IllegalArgumentException("workerId must match " + WORKER_ID_PATTERN.pattern());
            }
            return value;
        }

        private static String defaultWorkerId(String batchId) {
            String raw = "xxl:" + batchId + ":" + localIdentity();
            String sanitized = WORKER_ID_SAFE_CHARS.matcher(raw).replaceAll("-");
            if (sanitized.length() <= MAX_WORKER_ID_LENGTH) {
                return sanitized;
            }
            return sanitized.substring(0, MAX_WORKER_ID_LENGTH);
        }

        private static String valueOrDefault(String value, String defaultValue) {
            return StringUtils.hasText(value) ? value.trim() : defaultValue;
        }

        private static String localIdentity() {
            String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
            try {
                return InetAddress.getLocalHost().getHostName() + ":" + runtimeName;
            } catch (UnknownHostException ex) {
                return "unknown-host:" + runtimeName;
            }
        }
    }

    record OutboxXxlRunOptions(
            int limit, String workerId, String batchId, int parallelism, List<OutboxEventType> eventTypes) {

        OutboxXxlRunOptions withEventTypes(List<OutboxEventType> fixedEventTypes) {
            return new OutboxXxlRunOptions(limit, workerId, batchId, parallelism, List.copyOf(fixedEventTypes));
        }

        static OutboxXxlRunOptions parse(long jobId, String jobParam, List<OutboxEventType> defaultEventTypes) {
            Map<String, String> params = XxlRunOptions.parseParamMap(jobParam);
            XxlRunOptions base = XxlRunOptions.parseParams(jobId, jobParam);
            return new OutboxXxlRunOptions(
                    base.limit(),
                    base.workerId(),
                    base.batchId(),
                    parseParallelism(params.get("parallelism")),
                    parseEventTypes(params.get("eventtypes"), defaultEventTypes));
        }

        private static int parseParallelism(String value) {
            if (!StringUtils.hasText(value)) {
                return -1;
            }
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("parallelism must be an integer", ex);
            }
        }

        private static List<OutboxEventType> parseEventTypes(String value, List<OutboxEventType> defaultEventTypes) {
            if (!StringUtils.hasText(value)) {
                return List.copyOf(defaultEventTypes);
            }
            List<OutboxEventType> parsed = new ArrayList<>();
            for (String token : value.split(",")) {
                if (!StringUtils.hasText(token)) {
                    continue;
                }
                String normalized = token.trim().toUpperCase(Locale.ROOT);
                if ("ALL".equals(normalized)) {
                    return ALL_FULFILLMENT_EVENT_TYPES;
                }
                parsed.add(parseEventTypeToken(normalized));
            }
            if (parsed.isEmpty()) {
                return List.copyOf(defaultEventTypes);
            }
            return List.copyOf(parsed);
        }

        private static OutboxEventType parseEventTypeToken(String token) {
            return switch (token) {
                case "DISPATCH" -> OutboxEventType.SUPPLIER_DISPATCH;
                case "QUERY" -> OutboxEventType.SUPPLIER_QUERY;
                case "INVALID", "INVALID_ORDER" -> OutboxEventType.INVALID_ORDER;
                default -> OutboxEventType.valueOf(token);
            };
        }
    }
}
