package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.recharge.PendingPersistOrderPort;
import com.cardshop.trade.engine.application.recharge.PendingPersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.PersistedOrderStatePort;
import com.cardshop.trade.engine.application.recharge.PersistMessagePublishResult;
import com.cardshop.trade.engine.domain.order.PersistStatus;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.util.StringUtils;

/**
 * 基于 Redis pending/org_index 的 MQ 补投扫描适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.11 XXL-Job Handler 契约」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「17.3 MQ 补投任务」。该适配器负责把 Redis 中到期的
 * pending 订单还原为 {@link PendingPersistOrderSnapshot}，供 application 层重新发布 RabbitMQ 持久化消息。
 *
 * <p>当前实现只推进 Redis 中的补投状态，不清理 pending；pending 只能在 Consumer 完成 MySQL 事务后由
 * {@code mark_db_persisted.lua} 清理，避免补投成功但 Consumer 尚未落库时误删恢复入口。
 */
public class RedisPendingPersistOrderAdapter implements PendingPersistOrderPort {

    private static final Logger log = LoggerFactory.getLogger(RedisPendingPersistOrderAdapter.class);
    private static final String STATUS_DB_PERSISTED = PersistStatus.DB_PERSISTED.name();
    private static final String STATUS_PERSISTING = PersistStatus.PERSISTING.name();
    private static final String STATUS_PERSIST_QUEUED = PersistStatus.PERSIST_QUEUED.name();
    private static final String STATUS_PERSIST_FAILED = PersistStatus.PERSIST_FAILED.name();
    private static final String STATUS_PENDING_MQ = PersistStatus.PENDING_MQ.name();
    private static final String FAIL_CODE_REPUBLISH_FAILED = "MQ_REPUBLISH_FAILED";
    private static final int MAX_ERROR_LENGTH = 500;

    private final PendingRedisOperations operations;
    private final RechargeRedisKeyBuilder keyBuilder;
    private final int bucketCount;
    private final Duration retryDelay;
    private final Duration claimLease;
    private final PersistedOrderStatePort persistedOrderStatePort;
    private final Clock clock;

    public RedisPendingPersistOrderAdapter(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties,
            Clock clock) {
        this(redisTemplate, keyBuilder, properties, PersistedOrderStatePort.noop(), clock);
    }

    public RedisPendingPersistOrderAdapter(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties,
            PersistedOrderStatePort persistedOrderStatePort,
            Clock clock) {
        this(
                new ReactivePendingRedisOperations(redisTemplate, redisTimeout(properties)),
                keyBuilder,
                properties.getRedis().getPendingOrgBucketCount(),
                Duration.ofMillis(Math.max(1000, properties.getMq().getConsumerRetryDelayMs())),
                Duration.ofSeconds(120),
                persistedOrderStatePort,
                clock);
    }

    RedisPendingPersistOrderAdapter(
            PendingRedisOperations operations,
            RechargeRedisKeyBuilder keyBuilder,
            int bucketCount,
            Duration retryDelay,
            Duration claimLease,
            PersistedOrderStatePort persistedOrderStatePort,
            Clock clock) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
        this.keyBuilder = Objects.requireNonNull(keyBuilder, "keyBuilder must not be null");
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be positive");
        }
        this.bucketCount = bucketCount;
        this.retryDelay = Objects.requireNonNull(retryDelay, "retryDelay must not be null");
        this.claimLease = Objects.requireNonNull(claimLease, "claimLease must not be null");
        this.persistedOrderStatePort = Objects.requireNonNull(persistedOrderStatePort, "persistedOrderStatePort must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * 扫描当前 XXL-Job 分片负责的 org_index bucket，并按商户 pending ZSET 找出到期订单。
     *
     * <p>补投任务属于后台高频路径，单条 Redis 脏数据只按 warn 记录并跳过；Redis 命令异常按 error 记录并返回已
     * 收集的快照，保留下一轮任务继续处理，避免一次扫描失败阻断其他 bucket。
     */
    @Override
    public List<PendingPersistOrderSnapshot> selectDueForShard(
            int shardTotal,
            int shardIndex,
            int limit,
            String workerId) {
        int normalizedTotal = Math.max(1, shardTotal);
        if (normalizedTotal > bucketCount) {
            throw new IllegalArgumentException(
                    "pendingOrgBucketCount must be greater than or equal to shardTotal: pendingOrgBucketCount=%d, shardTotal=%d"
                            .formatted(bucketCount, normalizedTotal));
        }
        int normalizedIndex = Math.floorMod(shardIndex, normalizedTotal);
        int remaining = Math.max(1, limit);
        long nowMillis = clock.millis();
        List<PendingPersistOrderSnapshot> snapshots = new ArrayList<>();
        for (int bucket = 0; bucket < bucketCount && snapshots.size() < remaining; bucket++) {
            if (bucket % normalizedTotal != normalizedIndex) {
                continue;
            }
            collectBucket(bucket, nowMillis, remaining, snapshots, text(workerId));
        }
        return snapshots;
    }

    @Override
    public void markRepublished(
            PendingPersistOrderSnapshot snapshot,
            PersistMessagePublishResult result,
            String workerId) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        Objects.requireNonNull(result, "result must not be null");
        String messageId = StringUtils.hasText(result.messageId()) ? result.messageId() : snapshot.messageId();
        long nowMillis = clock.millis();
        long nextRetryMillis = nowMillis + Math.max(1000, retryDelay.toMillis());
        String now = Long.toString(nowMillis);
        int publishCount = intField(snapshot.initialOrderSnapshot(), "mq_publish_count", 0) + 1;
        Map<String, String> orderUpdates = new LinkedHashMap<>();
        orderUpdates.put("persist_status", STATUS_PERSIST_QUEUED);
        orderUpdates.put("persist_msg_id", messageId);
        orderUpdates.put("mq_publish_count", Integer.toString(Math.max(1, publishCount)));
        orderUpdates.put("last_republish_worker", text(workerId));
        orderUpdates.put("next_retry_time", Long.toString(nextRetryMillis));
        orderUpdates.put("updated_at", now);

        Map<String, String> idemUpdates = new LinkedHashMap<>();
        idemUpdates.put("persist_status", STATUS_PERSIST_QUEUED);
        idemUpdates.put("persist_msg_id", messageId);
        idemUpdates.put("updated_at", now);

        try {
            operations.putAll(keyBuilder.orderKey(snapshot.orgCode(), snapshot.outOrderSn()), orderUpdates);
            operations.putAll(keyBuilder.idempotentKey(snapshot.orgCode(), snapshot.outOrderSn()), idemUpdates);
            /*
             * docs/交行直充业务系统详细设计.md「20.11 XXL-Job Handler 契约」要求 pending score
             * 表示下一次补偿时间。补投 confirm 成功后仍保留 pending 给 Consumer 落库清理，但必须把
             * score 推进到 retryDelay 后，避免 claim 租约一到期就反复重发同一订单。
             */
            operations.addToZSet(keyBuilder.pendingKey(snapshot.orgCode()), nextRetryMillis, snapshot.orderId());
            operations.addToZSet(
                    keyBuilder.pendingOrgIndexKey(keyBuilder.pendingOrgBucket(snapshot.orgCode())),
                    nextRetryMillis,
                    snapshot.orgCode());
        } catch (RuntimeException ex) {
            log.error("Redis pending补投成功后状态推进失败 orgCode={}, outOrderSn={}, orderId={}, messageId={}, workerId={}",
                    snapshot.orgCode(),
                    snapshot.outOrderSn(),
                    snapshot.orderId(),
                    messageId,
                    workerId,
                    ex);
        }
    }

    @Override
    public void markRepublishFailed(PendingPersistOrderSnapshot snapshot, String reason, String workerId) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        long nowMillis = clock.millis();
        long nextRetryMillis = nowMillis + Math.max(1000, retryDelay.toMillis());
        String failReason = truncate(StringUtils.hasText(reason) ? reason : "MQ republish failed");
        Map<String, String> orderUpdates = new LinkedHashMap<>();
        orderUpdates.put("persist_status", STATUS_PERSIST_FAILED);
        orderUpdates.put("last_error_code", FAIL_CODE_REPUBLISH_FAILED);
        orderUpdates.put("last_error", failReason);
        orderUpdates.put("next_retry_time", Long.toString(nextRetryMillis));
        orderUpdates.put("last_republish_worker", text(workerId));
        orderUpdates.put("updated_at", Long.toString(nowMillis));

        try {
            operations.putAll(keyBuilder.orderKey(snapshot.orgCode(), snapshot.outOrderSn()), orderUpdates);
            operations.addToZSet(keyBuilder.pendingKey(snapshot.orgCode()), nextRetryMillis, snapshot.orderId());
            operations.addToZSet(
                    keyBuilder.pendingOrgIndexKey(keyBuilder.pendingOrgBucket(snapshot.orgCode())),
                    nextRetryMillis,
                    snapshot.orgCode());
        } catch (RuntimeException ex) {
            log.error("Redis pending补投失败后状态推进失败 orgCode={}, outOrderSn={}, orderId={}, workerId={}",
                    snapshot.orgCode(),
                    snapshot.outOrderSn(),
                    snapshot.orderId(),
                    workerId,
                    ex);
        }
    }

    private void collectBucket(
            int bucket,
            long nowMillis,
            int limit,
            List<PendingPersistOrderSnapshot> snapshots,
            String workerId) {
        List<String> orgCodes;
        try {
            orgCodes = operations.rangeByScore(keyBuilder.pendingOrgIndexKey(bucket), 0, nowMillis, limit);
        } catch (RuntimeException ex) {
            log.error("Redis pending补投扫描org_index失败 bucket={}, shardLimit={}", bucket, limit, ex);
            return;
        }
        for (String orgCode : orgCodes) {
            if (snapshots.size() >= limit) {
                return;
            }
            collectOrg(orgCode, nowMillis, limit, snapshots, workerId);
        }
    }

    private void collectOrg(
            String orgCode,
            long nowMillis,
            int limit,
            List<PendingPersistOrderSnapshot> snapshots,
            String workerId) {
        if (!StringUtils.hasText(orgCode)) {
            return;
        }
        List<String> orderIds;
        try {
            orderIds = operations.rangeByScore(
                    keyBuilder.pendingKey(orgCode),
                    0,
                    nowMillis,
                    limit - snapshots.size());
        } catch (RuntimeException ex) {
            log.error("Redis pending补投扫描商户pending失败 orgCode={}", orgCode, ex);
            return;
        }
        for (String orderId : orderIds) {
            toSnapshot(orgCode, orderId, workerId).ifPresent(snapshots::add);
            if (snapshots.size() >= limit) {
                return;
            }
        }
    }

    private java.util.Optional<PendingPersistOrderSnapshot> toSnapshot(String orgCode, String orderId, String workerId) {
        try {
            Map<String, String> reservation = operations.entries(keyBuilder.reservationKey(orgCode, orderId));
            String outOrderSn = reservation.get("out_order_sn");
            if (!StringUtils.hasText(outOrderSn)) {
                log.warn("Redis pending补投跳过缺少reservation的订单 orgCode={}, orderId={}", orgCode, orderId);
                return java.util.Optional.empty();
            }
            if (!claim(orgCode, orderId, workerId)) {
                return java.util.Optional.empty();
            }
            Map<String, String> order = operations.entries(keyBuilder.orderKey(orgCode, outOrderSn));
            if (order.isEmpty()) {
                log.warn("Redis pending补投跳过缺少order hash的订单 orgCode={}, outOrderSn={}, orderId={}",
                        orgCode, outOrderSn, orderId);
                return java.util.Optional.empty();
            }
            String persistStatus = order.getOrDefault("persist_status", "");
            if (STATUS_DB_PERSISTED.equals(persistStatus) || STATUS_PERSISTING.equals(persistStatus)) {
                return java.util.Optional.empty();
            }
            if (!STATUS_PENDING_MQ.equals(persistStatus)
                    && !STATUS_PERSIST_QUEUED.equals(persistStatus)
                    && !STATUS_PERSIST_FAILED.equals(persistStatus)) {
                log.warn("Redis pending补投跳过不可补投状态 orgCode={}, outOrderSn={}, orderId={}, persistStatus={}",
                        orgCode, outOrderSn, orderId, persistStatus);
                return java.util.Optional.empty();
            }
            long version = longField(order, "order_version", 1L);
            if (persistedOrderStatePort.isPersistedAtLeast(orgCode, outOrderSn, orderId, version)) {
                repairAlreadyPersisted(orgCode, outOrderSn, orderId, version, workerId);
                return java.util.Optional.empty();
            }
            String requestHash = text(order.get("request_hash"), reservation.get("request_hash"));
            return java.util.Optional.of(new PendingPersistOrderSnapshot(
                    messageId(order, orderId),
                    text(order.get("org_code"), orgCode),
                    text(order.get("order_id"), orderId),
                    outOrderSn,
                    requestHash,
                    Math.max(1L, version),
                    initialSnapshot(order)));
        } catch (RuntimeException ex) {
            log.error("Redis pending补投快照映射失败 orgCode={}, orderId={}", orgCode, orderId, ex);
            return java.util.Optional.empty();
        }
    }

    private boolean claim(String orgCode, String orderId, String workerId) {
        String owner = claimOwner(workerId, orderId);
        try {
            return operations.setIfAbsent(keyBuilder.persistClaimKey(orgCode, orderId), owner, claimLease);
        } catch (RuntimeException ex) {
            log.error("Redis pending补投claim失败 orgCode={}, orderId={}, workerId={}", orgCode, orderId, workerId, ex);
            return false;
        }
    }

    private void repairAlreadyPersisted(
            String orgCode,
            String outOrderSn,
            String orderId,
            long persistedVersion,
            String workerId) {
        String now = Long.toString(clock.millis());
        Map<String, String> orderUpdates = new LinkedHashMap<>();
        orderUpdates.put("persist_status", STATUS_DB_PERSISTED);
        orderUpdates.put("persisted_version", Long.toString(Math.max(1L, persistedVersion)));
        orderUpdates.put("last_republish_worker", text(workerId));
        orderUpdates.put("updated_at", now);

        Map<String, String> idemUpdates = new LinkedHashMap<>();
        idemUpdates.put("persist_status", STATUS_DB_PERSISTED);
        idemUpdates.put("updated_at", now);

        try {
            operations.putAll(keyBuilder.orderKey(orgCode, outOrderSn), orderUpdates);
            operations.putAll(keyBuilder.idempotentKey(orgCode, outOrderSn), idemUpdates);
            operations.removeFromZSet(keyBuilder.pendingKey(orgCode), orderId);
            refreshOrgIndex(orgCode);
        } catch (RuntimeException ex) {
            log.error("Redis pending二次确认已落库后修复状态失败 orgCode={}, outOrderSn={}, orderId={}, workerId={}",
                    orgCode,
                    outOrderSn,
                    orderId,
                    workerId,
                    ex);
        }
    }

    private void refreshOrgIndex(String orgCode) {
        String pendingKey = keyBuilder.pendingKey(orgCode);
        String orgIndexKey = keyBuilder.pendingOrgIndexKey(keyBuilder.pendingOrgBucket(orgCode));
        List<String> nextOrders = operations.rangeByScore(pendingKey, 0, Long.MAX_VALUE, 1);
        if (nextOrders.isEmpty()) {
            operations.removeFromZSet(orgIndexKey, orgCode);
            return;
        }
        Long nextScore = operations.score(pendingKey, nextOrders.getFirst());
        if (nextScore == null) {
            operations.removeFromZSet(orgIndexKey, orgCode);
            return;
        }
        operations.addToZSet(orgIndexKey, nextScore, orgCode);
    }

    private static String claimOwner(String workerId, String orderId) {
        return (StringUtils.hasText(workerId) ? workerId : "unknown-worker") + ":" + orderId;
    }

    private static Map<String, Object> initialSnapshot(Map<String, String> order) {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        order.forEach((key, value) -> {
            if (StringUtils.hasText(key) && value != null) {
                snapshot.put(key, value);
            }
        });
        return snapshot;
    }

    private static String messageId(Map<String, String> order, String orderId) {
        String existing = order.get("persist_msg_id");
        if (StringUtils.hasText(existing)) {
            return existing;
        }
        return "republish-" + orderId + "-" + UUID.randomUUID();
    }

    private static long longField(Map<String, String> fields, String key, long fallback) {
        String value = fields.get(key);
        if (!StringUtils.hasText(value)) {
            return fallback;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("invalid redis long field: " + key + "=" + value, ex);
        }
    }

    private static int intField(Map<String, Object> fields, String key, int fallback) {
        Object value = fields.get(key);
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String text && StringUtils.hasText(text)) {
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("invalid redis int field: " + key + "=" + text, ex);
            }
        }
        return fallback;
    }

    private static String text(String primary, String fallback) {
        if (StringUtils.hasText(primary)) {
            return primary;
        }
        return StringUtils.hasText(fallback) ? fallback : "";
    }

    private static String text(String value) {
        return value == null ? "" : value;
    }

    private static String truncate(String value) {
        if (value == null || value.length() <= MAX_ERROR_LENGTH) {
            return value;
        }
        return value.substring(0, MAX_ERROR_LENGTH);
    }

    private static Duration redisTimeout(TradeEngineProperties properties) {
        return Duration.ofMillis(Math.max(100, properties.getRedis().getWaitTimeoutMs() * 5L));
    }

    interface PendingRedisOperations {

        List<String> rangeByScore(String key, long minScore, long maxScore, int limit);

        Map<String, String> entries(String key);

        void putAll(String key, Map<String, String> fields);

        void addToZSet(String key, long score, String value);

        boolean setIfAbsent(String key, String value, Duration ttl);

        void removeFromZSet(String key, String value);

        Long score(String key, String value);
    }

    private static final class ReactivePendingRedisOperations implements PendingRedisOperations {

        private final ReactiveStringRedisTemplate redisTemplate;
        private final Duration timeout;

        private ReactivePendingRedisOperations(ReactiveStringRedisTemplate redisTemplate, Duration timeout) {
            this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
            this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
        }

        @Override
        public List<String> rangeByScore(String key, long minScore, long maxScore, int limit) {
            List<String> values = redisTemplate.opsForZSet()
                    .rangeByScore(
                            key,
                            Range.closed((double) minScore, (double) maxScore),
                            Limit.limit().count(Math.max(1, limit)))
                    .collectList()
                    .block(timeout);
            return values == null ? List.of() : values;
        }

        @Override
        public Map<String, String> entries(String key) {
            Map<String, String> fields = redisTemplate.opsForHash()
                    .entries(key)
                    .collectMap(
                            entry -> String.valueOf(entry.getKey()),
                            entry -> String.valueOf(entry.getValue()))
                    .block(timeout);
            return fields == null ? Map.of() : fields;
        }

        @Override
        public void putAll(String key, Map<String, String> fields) {
            redisTemplate.opsForHash().putAll(key, fields).block(timeout);
        }

        @Override
        public void addToZSet(String key, long score, String value) {
            redisTemplate.opsForZSet().add(key, value, score).block(timeout);
        }

        @Override
        public boolean setIfAbsent(String key, String value, Duration ttl) {
            Boolean acquired = redisTemplate.opsForValue()
                    .setIfAbsent(key, value, ttl)
                    .block(timeout);
            return Boolean.TRUE.equals(acquired);
        }

        @Override
        public void removeFromZSet(String key, String value) {
            redisTemplate.opsForZSet().remove(key, value).block(timeout);
        }

        @Override
        public Long score(String key, String value) {
            Double score = redisTemplate.opsForZSet().score(key, value).block(timeout);
            return score == null ? null : score.longValue();
        }
    }
}
