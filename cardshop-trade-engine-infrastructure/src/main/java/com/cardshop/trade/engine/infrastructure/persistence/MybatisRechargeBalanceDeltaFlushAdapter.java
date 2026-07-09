package com.cardshop.trade.engine.infrastructure.persistence;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.cardshop.trade.engine.application.recharge.RechargeBalanceDeltaFlushPort;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StringUtils;

/**
 * 基于 `recharge_outbox(BALANCE_DELTA_FLUSH)` 的商户最终余额刷账适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「14.7 `mc_ext_depart.balance` 刷账协议」和
 * 「17.2 分片规则」。下单实时余额以 Redis 为准；该适配器只把已落库订单产生的 delta 按可靠 outbox
 * 沉淀到旧表 `mc_ext_depart.balance`，供后台展示和财务沉淀使用。
 */
public class MybatisRechargeBalanceDeltaFlushAdapter implements RechargeBalanceDeltaFlushPort {

    private static final Logger log = LoggerFactory.getLogger(MybatisRechargeBalanceDeltaFlushAdapter.class);
    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");
    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };

    private final BalanceDeltaFlushOperations operations;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final Duration claimLease;
    private final Duration retryBackoff;
    private final TransactionRunner transactionRunner;

    public MybatisRechargeBalanceDeltaFlushAdapter(
            RechargeOutboxMapper outboxMapper,
            McExtDepartMapper extDepartMapper,
            ObjectMapper objectMapper,
            Clock clock,
            Duration claimLease,
            Duration retryBackoff,
            PlatformTransactionManager transactionManager) {
        this(
                new MybatisBalanceDeltaFlushOperations(outboxMapper, extDepartMapper),
                objectMapper,
                clock,
                claimLease,
                retryBackoff,
                transactional(transactionManager));
    }

    MybatisRechargeBalanceDeltaFlushAdapter(
            BalanceDeltaFlushOperations operations,
            ObjectMapper objectMapper,
            Clock clock,
            Duration claimLease,
            Duration retryBackoff) {
        this(operations, objectMapper, clock, claimLease, retryBackoff, Runnable::run);
    }

    MybatisRechargeBalanceDeltaFlushAdapter(
            BalanceDeltaFlushOperations operations,
            ObjectMapper objectMapper,
            Clock clock,
            Duration claimLease,
            Duration retryBackoff,
            TransactionRunner transactionRunner) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.claimLease = Objects.requireNonNull(claimLease, "claimLease must not be null");
        this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff must not be null");
        this.transactionRunner = Objects.requireNonNull(transactionRunner, "transactionRunner must not be null");
    }

    /**
     * 扫描并刷入当前分片的余额 delta 事件。
     *
     * <p>该后台路径可能批量遇到脏 payload 或商户行缺失，单事件失败按 error 记录并回写 outbox 失败状态，
     * 其他已抢占事件继续处理，避免一个坏事件阻断整批余额沉淀。
     */
    @Override
    public RechargeCompensationBatchResult flushDue(int shardTotal, int shardIndex, int limit, String workerId) {
        List<RechargeOutboxDO> events = operations.selectDueBalanceFlush(shardTotal, shardIndex, limit);
        int succeeded = 0;
        int failed = 0;
        int skipped = 0;
        for (RechargeOutboxDO event : events) {
            LocalDateTime lockedUntil = now().plusNanos(claimLease.toNanos());
            int claimed = operations.claimProcessing(event.getEventId(), version(event), workerId, lockedUntil);
            if (claimed != 1) {
                skipped++;
                continue;
            }
            long claimedVersion = version(event) + 1;
            try {
                transactionRunner.run(() -> flushClaimed(event, claimedVersion));
                succeeded++;
            } catch (RuntimeException ex) {
                failed++;
                log.error("余额delta刷账事件处理失败 eventId={}, orgCode={}, orderId={}, workerId={}",
                        event.getEventId(), event.getOrgCode(), event.getOrderId(), workerId, ex);
                operations.markFailed(
                        event.getEventId(),
                        claimedVersion,
                        ex.getMessage(),
                        now().plusNanos(retryBackoff.toNanos()),
                        now());
            }
        }
        return new RechargeCompensationBatchResult(events.size(), succeeded, failed, skipped);
    }

    /**
     * 刷入单个已抢占事件。
     *
     * <p>真实 Spring 容器中方法由外层 job 以单事件幂等重试兜底；这里保持操作顺序为“余额增量 -> outbox SUCCESS”，
     * 并依赖 outbox `event_id` 幂等和 `mc_ext_depart.balance = balance + delta` 原子更新避免重复丢失。
     */
    protected void flushClaimed(RechargeOutboxDO event, long claimedVersion) {
        long deltaAmount = deltaAmount(event.getPayload());
        int updated = operations.incrementBalance(event.getOrgCode(), deltaAmount);
        if (updated != 1) {
            throw new IllegalStateException("mc_ext_depart balance row not found for orgCode=" + event.getOrgCode());
        }
        int success = operations.markSuccess(event.getEventId(), claimedVersion, now());
        if (success != 1) {
            throw new IllegalStateException("mark balance flush outbox success failed eventId=" + event.getEventId());
        }
    }

    private long deltaAmount(String payload) {
        try {
            Map<String, Object> fields = objectMapper.readValue(payload, PAYLOAD_TYPE);
            Object value = fields.get("delta_amount");
            if (value instanceof Number number) {
                return number.longValue();
            }
            if (value instanceof String text && StringUtils.hasText(text)) {
                return Long.parseLong(text);
            }
            throw new IllegalArgumentException("delta_amount missing in balance flush payload");
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("parse balance flush payload failed", ex);
        }
    }

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(clock.instant(), LEGACY_CARDSHOP_DB_ZONE);
    }

    private static long version(RechargeOutboxDO event) {
        return event.getVersion() == null ? 0L : event.getVersion();
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

    interface BalanceDeltaFlushOperations {

        List<RechargeOutboxDO> selectDueBalanceFlush(int shardTotal, int shardIndex, int limit);

        int claimProcessing(String eventId, long currentVersion, String workerId, LocalDateTime lockedUntil);

        int incrementBalance(String orgCode, long deltaAmount);

        int markSuccess(String eventId, long currentVersion, LocalDateTime updatedAt);

        int markFailed(
                String eventId,
                long currentVersion,
                String errorMessage,
                LocalDateTime nextRetryTime,
                LocalDateTime updatedAt);
    }

    private static final class MybatisBalanceDeltaFlushOperations implements BalanceDeltaFlushOperations {

        private final RechargeOutboxMapper outboxMapper;
        private final McExtDepartMapper extDepartMapper;

        private MybatisBalanceDeltaFlushOperations(
                RechargeOutboxMapper outboxMapper,
                McExtDepartMapper extDepartMapper) {
            this.outboxMapper = Objects.requireNonNull(outboxMapper, "outboxMapper must not be null");
            this.extDepartMapper = Objects.requireNonNull(extDepartMapper, "extDepartMapper must not be null");
        }

        @Override
        public List<RechargeOutboxDO> selectDueBalanceFlush(int shardTotal, int shardIndex, int limit) {
            return outboxMapper.selectDueForShardByEventTypes(
                    shardTotal,
                    shardIndex,
                    limit,
                    List.of(OutboxEventType.BALANCE_DELTA_FLUSH.name()));
        }

        @Override
        public int claimProcessing(String eventId, long currentVersion, String workerId, LocalDateTime lockedUntil) {
            return outboxMapper.claimProcessing(eventId, currentVersion, workerId, lockedUntil);
        }

        @Override
        public int incrementBalance(String orgCode, long deltaAmount) {
            return extDepartMapper.incrementBalance(orgCode, deltaAmount);
        }

        @Override
        public int markSuccess(String eventId, long currentVersion, LocalDateTime updatedAt) {
            return outboxMapper.update(null, new LambdaUpdateWrapper<RechargeOutboxDO>()
                    .eq(RechargeOutboxDO::getEventId, eventId)
                    .eq(RechargeOutboxDO::getVersion, currentVersion)
                    .set(RechargeOutboxDO::getStatus, TaskStatus.SUCCESS.name())
                    .set(RechargeOutboxDO::getLockedUntil, null)
                    .set(RechargeOutboxDO::getLastError, "")
                    .set(RechargeOutboxDO::getUpdatedAt, updatedAt)
                    .setSql("version = version + 1"));
        }

        @Override
        public int markFailed(
                String eventId,
                long currentVersion,
                String errorMessage,
                LocalDateTime nextRetryTime,
                LocalDateTime updatedAt) {
            return outboxMapper.update(null, new LambdaUpdateWrapper<RechargeOutboxDO>()
                    .eq(RechargeOutboxDO::getEventId, eventId)
                    .eq(RechargeOutboxDO::getVersion, currentVersion)
                    .set(RechargeOutboxDO::getStatus, TaskStatus.FAILED.name())
                    .set(RechargeOutboxDO::getNextRetryTime, nextRetryTime)
                    .set(RechargeOutboxDO::getLockedUntil, null)
                    .set(RechargeOutboxDO::getLastError, truncate(errorMessage, 1000))
                    .set(RechargeOutboxDO::getUpdatedAt, updatedAt)
                    .setSql("retry_count = retry_count + 1")
                    .setSql("version = version + 1"));
        }

        private static String truncate(String value, int maxLength) {
            if (value == null) {
                return "";
            }
            return value.length() <= maxLength ? value : value.substring(0, maxLength);
        }
    }
}
