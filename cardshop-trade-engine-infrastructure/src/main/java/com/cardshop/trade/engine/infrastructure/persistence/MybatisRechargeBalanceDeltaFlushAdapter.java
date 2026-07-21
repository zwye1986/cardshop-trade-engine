package com.cardshop.trade.engine.infrastructure.persistence;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushPort;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeBalanceDeltaOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeBalanceDeltaOutboxMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StringUtils;

/**
 * 基于 {@code recharge_balance_delta_outbox} 的商户最终余额刷账适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」：
 * 扫描到期/租约失效事件 → CAS 抢占 → 按 {@code org_code} 聚合 → 同一事务内
 * {@code mc_ext_depart.balance += sum(delta_amount)} 并 CAS 标记 SUCCESS。
 * 任一 {@code markSuccess} CAS miss 必须回滚整个商户组事务，不能留下余额已更新但 delta 未 SUCCESS 的状态。
 *
 * <p>刷账协议、租约与退避见 {@code docs/交行直充业务系统高可用方案设计.md}
 * 「14.7 {@code mc_ext_depart.balance} 刷账协议」「17.2 分片规则」。
 * 下单实时余额以 Redis 为准；本适配器只把已落库订单的 delta 沉淀到旧表 {@code mc_ext_depart.balance}。
 *
 * <p>运行参数来自 {@code trade.engine.outbox.*}：批大小、claim 租约、退避序列
 * {@code 5,30,120,600}（秒）。job 传入 {@code limit <= 0} 时回退到配置批大小。
 */
public class MybatisRechargeBalanceDeltaFlushAdapter implements RechargeBalanceDeltaFlushPort {

    private static final Logger log = LoggerFactory.getLogger(MybatisRechargeBalanceDeltaFlushAdapter.class);
    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");
    private static final int DEFAULT_MAX_RETRY_COUNT = 10;

    private final BalanceDeltaFlushOperations operations;
    private final Clock clock;
    private final Duration claimLease;
    private final List<Duration> retryBackoffSequence;
    private final int defaultBatchSize;
    private final TransactionRunner transactionRunner;

    public MybatisRechargeBalanceDeltaFlushAdapter(
            RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper,
            McExtDepartMapper extDepartMapper,
            Clock clock,
            Duration claimLease,
            List<Duration> retryBackoffSequence,
            int defaultBatchSize,
            PlatformTransactionManager transactionManager) {
        this(
                new MybatisBalanceDeltaFlushOperations(balanceDeltaOutboxMapper, extDepartMapper),
                clock,
                claimLease,
                retryBackoffSequence,
                defaultBatchSize,
                transactional(transactionManager));
    }

    MybatisRechargeBalanceDeltaFlushAdapter(
            BalanceDeltaFlushOperations operations,
            Clock clock,
            Duration claimLease,
            List<Duration> retryBackoffSequence,
            int defaultBatchSize) {
        this(operations, clock, claimLease, retryBackoffSequence, defaultBatchSize, Runnable::run);
    }

    MybatisRechargeBalanceDeltaFlushAdapter(
            BalanceDeltaFlushOperations operations,
            Clock clock,
            Duration claimLease,
            List<Duration> retryBackoffSequence,
            int defaultBatchSize,
            TransactionRunner transactionRunner) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.claimLease = Objects.requireNonNull(claimLease, "claimLease must not be null");
        this.retryBackoffSequence = List.copyOf(requireNonEmptyBackoffs(retryBackoffSequence));
        this.defaultBatchSize = Math.max(1, defaultBatchSize);
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
    }

    /**
     * 扫描并按商户聚合刷入当前分片的余额 delta 事件。
     *
     * <p>参见详细设计「9.2 余额 Delta 刷账 Outbox」。商户组事务失败后在事务外逐条标记
     * {@code FAILED}/{@code MANUAL_REQUIRED}；若失败标记也因数据库抖动失败，事件保留
     * {@code PROCESSING}，由 {@code idx_balance_delta_lease} + {@code locked_until} 回收。
     */
    @Override
    public RechargeCompensationBatchResult flushDue(int shardTotal, int shardIndex, int limit, String workerId) {
        LocalDateTime now = now();
        int effectiveLimit = limit <= 0 ? defaultBatchSize : limit;
        List<RechargeBalanceDeltaOutboxDO> events =
                operations.selectDueForFlush(shardTotal, shardIndex, effectiveLimit, now);
        int succeeded = 0;
        int failed = 0;
        int skipped = 0;

        List<ClaimedBalanceDelta> claimed = new ArrayList<>();
        for (RechargeBalanceDeltaOutboxDO event : events) {
            if (retryExhausted(event)) {
                failed++;
                tryMarkManualRequired(
                        event.getEventId(),
                        version(event),
                        retryExhaustedMessage(event),
                        now,
                        false,
                        workerId,
                        event.getOrgCode(),
                        event.getOrderId());
                continue;
            }
            LocalDateTime lockedUntil = now.plusNanos(claimLease.toNanos());
            int claimedRows = operations.claimProcessing(
                    event.getEventId(), version(event), workerId, lockedUntil, now);
            if (claimedRows != 1) {
                skipped++;
                continue;
            }
            claimed.add(new ClaimedBalanceDelta(event, version(event) + 1));
        }

        Map<String, List<ClaimedBalanceDelta>> byOrg = groupByOrg(claimed);
        for (Map.Entry<String, List<ClaimedBalanceDelta>> entry : byOrg.entrySet()) {
            String orgCode = entry.getKey();
            List<ClaimedBalanceDelta> group = entry.getValue();
            try {
                transactionRunner.run(() -> flushOrgGroup(orgCode, group));
                succeeded += group.size();
            } catch (RuntimeException ex) {
                failed += markGroupFailed(group, workerId, ex);
            }
        }
        return new RechargeCompensationBatchResult(events.size(), succeeded, failed, skipped);
    }

    /**
     * 同一商户组事务：一次余额增量 + 逐条 SUCCESS CAS。
     *
     * <p>参见详细设计「9.2」与高可用「14.7」：{@code markSuccess} 任一返回 0 必须抛错回滚，
     * 防止余额已加而 delta 仍停留在 {@code PROCESSING}。
     */
    private void flushOrgGroup(String orgCode, List<ClaimedBalanceDelta> group) {
        long sum = 0L;
        for (ClaimedBalanceDelta claimed : group) {
            sum += deltaAmount(claimed.event());
        }
        int updated = operations.incrementBalance(orgCode, sum);
        if (updated != 1) {
            throw new IllegalStateException("mc_ext_depart balance row not found for orgCode=" + orgCode);
        }
        LocalDateTime updatedAt = now();
        for (ClaimedBalanceDelta claimed : group) {
            int success = operations.markSuccess(claimed.event().getEventId(), claimed.claimedVersion(), updatedAt);
            if (success != 1) {
                throw new IllegalStateException(
                        "mark balance flush outbox success failed eventId=" + claimed.event().getEventId());
            }
        }
    }

    /**
     * 商户组事务失败后，在事务外按退避序列标记 FAILED，或转 MANUAL_REQUIRED。
     *
     * <p>退避取 {@code backoffs[min(retry_count, last)]}（详细设计「9.2」/ 配置
     * {@code trade.engine.outbox.balance-delta-retry-backoff-seconds}）。标记失败时吞掉异常并留下
     * {@code PROCESSING}，依赖租约回收。
     *
     * @return 计入 failed 的事件数（等于组大小）
     */
    private int markGroupFailed(List<ClaimedBalanceDelta> group, String workerId, RuntimeException cause) {
        LocalDateTime now = now();
        for (ClaimedBalanceDelta claimed : group) {
            RechargeBalanceDeltaOutboxDO event = claimed.event();
            try {
                if (nextRetryExhausted(event)) {
                    log.warn(
                            "余额delta刷账商户组达到最大重试，转人工处理 eventId={}, orgCode={}, orderId={}, workerId={}, retryCount={}, maxRetryCount={}, error={}",
                            event.getEventId(),
                            event.getOrgCode(),
                            event.getOrderId(),
                            workerId,
                            event.getRetryCount(),
                            event.getMaxRetryCount(),
                            cause.getMessage(),
                            cause);
                    operations.markManualRequired(
                            event.getEventId(),
                            claimed.claimedVersion(),
                            cause.getMessage(),
                            now,
                            true);
                } else {
                    Duration backoff = backoffFor(event);
                    log.error(
                            "余额delta刷账商户组处理失败 eventId={}, orgCode={}, orderId={}, workerId={}, nextRetrySeconds={}",
                            event.getEventId(),
                            event.getOrgCode(),
                            event.getOrderId(),
                            workerId,
                            backoff.toSeconds(),
                            cause);
                    operations.markFailed(
                            event.getEventId(),
                            claimed.claimedVersion(),
                            cause.getMessage(),
                            now.plusNanos(backoff.toNanos()),
                            now);
                }
            } catch (RuntimeException markEx) {
                /*
                 * 失败标记本身也失败时保留 PROCESSING，由 idx_balance_delta_lease 回收。
                 * 该路径可能在数据库抖动时批量出现，用 warn 避免与主失败 error 叠加成日志风暴。
                 */
                log.warn(
                        "余额delta刷账失败状态回写失败，事件保留PROCESSING待租约回收 eventId={}, orgCode={}, orderId={}, workerId={}",
                        event.getEventId(),
                        event.getOrgCode(),
                        event.getOrderId(),
                        workerId,
                        markEx);
            }
        }
        return group.size();
    }

    private void tryMarkManualRequired(
            String eventId,
            long currentVersion,
            String errorMessage,
            LocalDateTime updatedAt,
            boolean incrementRetryCount,
            String workerId,
            String orgCode,
            String orderId) {
        try {
            operations.markManualRequired(eventId, currentVersion, errorMessage, updatedAt, incrementRetryCount);
        } catch (RuntimeException markEx) {
            log.warn(
                    "余额delta刷账MANUAL_REQUIRED回写失败 eventId={}, orgCode={}, orderId={}, workerId={}",
                    eventId,
                    orgCode,
                    orderId,
                    workerId,
                    markEx);
        }
    }

    private Duration backoffFor(RechargeBalanceDeltaOutboxDO event) {
        int retryCount = event.getRetryCount() == null ? 0 : event.getRetryCount();
        int index = Math.min(retryCount, retryBackoffSequence.size() - 1);
        return retryBackoffSequence.get(index);
    }

    private static Map<String, List<ClaimedBalanceDelta>> groupByOrg(List<ClaimedBalanceDelta> claimed) {
        Map<String, List<ClaimedBalanceDelta>> byOrg = new LinkedHashMap<>();
        for (ClaimedBalanceDelta item : claimed) {
            String orgCode = item.event().getOrgCode();
            byOrg.computeIfAbsent(orgCode, ignored -> new ArrayList<>()).add(item);
        }
        return byOrg;
    }

    private static long deltaAmount(RechargeBalanceDeltaOutboxDO event) {
        if (event.getDeltaAmount() == null) {
            throw new IllegalArgumentException("delta_amount missing for eventId=" + event.getEventId());
        }
        return event.getDeltaAmount();
    }

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(clock.instant(), LEGACY_CARDSHOP_DB_ZONE);
    }

    private static long version(RechargeBalanceDeltaOutboxDO event) {
        return event.getVersion() == null ? 0L : event.getVersion();
    }

    private static List<Duration> requireNonEmptyBackoffs(List<Duration> retryBackoffSequence) {
        Objects.requireNonNull(retryBackoffSequence, "retryBackoffSequence must not be null");
        if (retryBackoffSequence.isEmpty()) {
            throw new IllegalArgumentException("retryBackoffSequence must not be empty");
        }
        for (Duration duration : retryBackoffSequence) {
            if (duration == null || duration.isZero() || duration.isNegative()) {
                throw new IllegalArgumentException("retryBackoffSequence entries must be positive");
            }
        }
        return retryBackoffSequence;
    }

    private static TransactionRunner transactional(PlatformTransactionManager transactionManager) {
        TransactionTemplate template = new TransactionTemplate(
                Objects.requireNonNull(transactionManager, "transactionManager must not be null"));
        return action -> template.executeWithoutResult(ignored -> action.run());
    }

    @FunctionalInterface
    interface TransactionRunner {

        void run(Runnable action);
    }

    /**
     * 已 CAS 抢占成功的余额 delta，携带抢占后的 version 供 SUCCESS/FAILED 回写。
     */
    private record ClaimedBalanceDelta(RechargeBalanceDeltaOutboxDO event, long claimedVersion) {
    }

    interface BalanceDeltaFlushOperations {

        List<RechargeBalanceDeltaOutboxDO> selectDueForFlush(
                int shardTotal, int shardIndex, int limit, LocalDateTime now);

        int claimProcessing(
                String eventId,
                long currentVersion,
                String workerId,
                LocalDateTime lockedUntil,
                LocalDateTime now);

        int incrementBalance(String orgCode, long deltaAmount);

        int markSuccess(String eventId, long currentVersion, LocalDateTime updatedAt);

        int markFailed(
                String eventId,
                long currentVersion,
                String errorMessage,
                LocalDateTime nextRetryTime,
                LocalDateTime updatedAt);

        int markManualRequired(
                String eventId,
                long currentVersion,
                String errorMessage,
                LocalDateTime updatedAt,
                boolean incrementRetryCount);
    }

    private static final class MybatisBalanceDeltaFlushOperations implements BalanceDeltaFlushOperations {

        private final RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper;
        private final McExtDepartMapper extDepartMapper;

        private MybatisBalanceDeltaFlushOperations(
                RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper,
                McExtDepartMapper extDepartMapper) {
            this.balanceDeltaOutboxMapper =
                    Objects.requireNonNull(balanceDeltaOutboxMapper, "balanceDeltaOutboxMapper must not be null");
            this.extDepartMapper = Objects.requireNonNull(extDepartMapper, "extDepartMapper must not be null");
        }

        @Override
        public List<RechargeBalanceDeltaOutboxDO> selectDueForFlush(
                int shardTotal, int shardIndex, int limit, LocalDateTime now) {
            return balanceDeltaOutboxMapper.selectDueForFlush(shardTotal, shardIndex, limit, now);
        }

        @Override
        public int claimProcessing(
                String eventId,
                long currentVersion,
                String workerId,
                LocalDateTime lockedUntil,
                LocalDateTime now) {
            return balanceDeltaOutboxMapper.claimProcessing(eventId, currentVersion, workerId, lockedUntil, now);
        }

        @Override
        public int incrementBalance(String orgCode, long deltaAmount) {
            return extDepartMapper.incrementBalance(orgCode, deltaAmount);
        }

        @Override
        public int markSuccess(String eventId, long currentVersion, LocalDateTime updatedAt) {
            return balanceDeltaOutboxMapper.update(null, new LambdaUpdateWrapper<RechargeBalanceDeltaOutboxDO>()
                    .eq(RechargeBalanceDeltaOutboxDO::getEventId, eventId)
                    .eq(RechargeBalanceDeltaOutboxDO::getVersion, currentVersion)
                    .set(RechargeBalanceDeltaOutboxDO::getStatus, TaskStatus.SUCCESS.name())
                    .set(RechargeBalanceDeltaOutboxDO::getLockedUntil, null)
                    .set(RechargeBalanceDeltaOutboxDO::getLastError, "")
                    .set(RechargeBalanceDeltaOutboxDO::getUpdatedAt, updatedAt)
                    .setSql("version = version + 1"));
        }

        @Override
        public int markFailed(
                String eventId,
                long currentVersion,
                String errorMessage,
                LocalDateTime nextRetryTime,
                LocalDateTime updatedAt) {
            return balanceDeltaOutboxMapper.update(null, new LambdaUpdateWrapper<RechargeBalanceDeltaOutboxDO>()
                    .eq(RechargeBalanceDeltaOutboxDO::getEventId, eventId)
                    .eq(RechargeBalanceDeltaOutboxDO::getVersion, currentVersion)
                    .set(RechargeBalanceDeltaOutboxDO::getStatus, TaskStatus.FAILED.name())
                    .set(RechargeBalanceDeltaOutboxDO::getNextRetryTime, nextRetryTime)
                    .set(RechargeBalanceDeltaOutboxDO::getLockedUntil, null)
                    .set(RechargeBalanceDeltaOutboxDO::getLastError, truncate(errorMessage, 1000))
                    .set(RechargeBalanceDeltaOutboxDO::getUpdatedAt, updatedAt)
                    .setSql("retry_count = retry_count + 1")
                    .setSql("version = version + 1"));
        }

        @Override
        public int markManualRequired(
                String eventId,
                long currentVersion,
                String errorMessage,
                LocalDateTime updatedAt,
                boolean incrementRetryCount) {
            LambdaUpdateWrapper<RechargeBalanceDeltaOutboxDO> wrapper =
                    new LambdaUpdateWrapper<RechargeBalanceDeltaOutboxDO>()
                            .eq(RechargeBalanceDeltaOutboxDO::getEventId, eventId)
                            .eq(RechargeBalanceDeltaOutboxDO::getVersion, currentVersion)
                            .set(RechargeBalanceDeltaOutboxDO::getStatus, TaskStatus.MANUAL_REQUIRED.name())
                            .set(RechargeBalanceDeltaOutboxDO::getLockedUntil, null)
                            .set(RechargeBalanceDeltaOutboxDO::getLastError, truncate(errorMessage, 1000))
                            .set(RechargeBalanceDeltaOutboxDO::getUpdatedAt, updatedAt)
                            .setSql("version = version + 1");
            if (incrementRetryCount) {
                wrapper.setSql("retry_count = retry_count + 1");
            }
            return balanceDeltaOutboxMapper.update(null, wrapper);
        }

        private static String truncate(String value, int maxLength) {
            if (value == null) {
                return "";
            }
            return value.length() <= maxLength ? value : value.substring(0, maxLength);
        }
    }

    private static boolean nextRetryExhausted(RechargeBalanceDeltaOutboxDO event) {
        int retryCount = event.getRetryCount() == null ? 0 : event.getRetryCount();
        int maxRetryCount = event.getMaxRetryCount() == null ? DEFAULT_MAX_RETRY_COUNT : event.getMaxRetryCount();
        return retryCount + 1 >= maxRetryCount;
    }

    private static boolean retryExhausted(RechargeBalanceDeltaOutboxDO event) {
        int retryCount = event.getRetryCount() == null ? 0 : event.getRetryCount();
        int maxRetryCount = event.getMaxRetryCount() == null ? DEFAULT_MAX_RETRY_COUNT : event.getMaxRetryCount();
        return retryCount >= maxRetryCount;
    }

    private static String retryExhaustedMessage(RechargeBalanceDeltaOutboxDO event) {
        String lastError = event.getLastError();
        if (!StringUtils.hasText(lastError)) {
            return "balance delta flush retry exhausted";
        }
        return "balance delta flush retry exhausted: " + lastError;
    }
}
