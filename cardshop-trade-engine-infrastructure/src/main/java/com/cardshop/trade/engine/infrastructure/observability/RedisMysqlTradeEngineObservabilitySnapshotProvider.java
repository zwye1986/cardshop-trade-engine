package com.cardshop.trade.engine.infrastructure.observability;

import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeBalanceDeltaOutboxMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.util.StringUtils;

/**
 * 基于 Redis/MySQL 的业务监控快照 Provider。
 *
 * <p>参见 {@code docs/开发进度.md}「5.1 监控告警最小闭环执行边界」与
 * {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」「9.2 余额 Delta 刷账 Outbox」。
 * test/dev 共享旧卡管测试库时，Prometheus scrape 不能直接触发高频全表扫描；本类使用 TTL 缓存快照，
 * 刷新失败时返回上一次快照。
 *
 * <p>拆表后同时注入 {@link RechargeDispatchOutboxMapper} 与 {@link RechargeBalanceDeltaOutboxMapper}：
 * 派单积压按 event_type+status；余额积压只按 status。对账采样的未完成 delta 只读余额表。
 * 禁止把 {@code org_code} 做成 Prometheus 标签。
 */
public class RedisMysqlTradeEngineObservabilitySnapshotProvider implements TradeEngineObservabilitySnapshotProvider {

    private static final Logger log = LoggerFactory.getLogger(RedisMysqlTradeEngineObservabilitySnapshotProvider.class);
    private static final Duration DEFAULT_CACHE_TTL = Duration.ofSeconds(15);
    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");

    private final Operations operations;
    private final Clock clock;
    private final Duration cacheTtl;
    private volatile TradeEngineObservabilitySnapshot cachedSnapshot = TradeEngineObservabilitySnapshot.empty();
    private volatile Instant cachedUntil = Instant.EPOCH;

    public RedisMysqlTradeEngineObservabilitySnapshotProvider(
            Operations operations,
            Clock clock,
            Duration cacheTtl) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.cacheTtl = normalizeCacheTtl(cacheTtl);
    }

    public RedisMysqlTradeEngineObservabilitySnapshotProvider(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            TradeEngineProperties properties,
            RechargeDispatchOutboxMapper dispatchOutboxMapper,
            RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper,
            RechargeAcceptAnomalyMapper acceptAnomalyMapper,
            McExtDepartMapper extDepartMapper,
            Clock clock) {
        this(
                new RedisMysqlOperations(
                        redisTemplate,
                        keyBuilder,
                        properties,
                        dispatchOutboxMapper,
                        balanceDeltaOutboxMapper,
                        acceptAnomalyMapper,
                        extDepartMapper,
                        clock),
                clock,
                DEFAULT_CACHE_TTL);
    }

    @Override
    public TradeEngineObservabilitySnapshot currentSnapshot() {
        Instant now = clock.instant();
        if (now.isBefore(cachedUntil)) {
            return cachedSnapshot;
        }
        synchronized (this) {
            now = clock.instant();
            if (now.isBefore(cachedUntil)) {
                return cachedSnapshot;
            }
            try {
                TradeEngineObservabilitySnapshot refreshed = operations.refreshSnapshot();
                cachedSnapshot = refreshed == null ? TradeEngineObservabilitySnapshot.empty() : refreshed;
                cachedUntil = now.plus(cacheTtl);
                return cachedSnapshot;
            } catch (RuntimeException ex) {
                /*
                 * 监控刷新失败不能让 /actuator/prometheus 失败；共享旧库或 Redis 抖动时返回上一次快照，
                 * 同时记录 warn 供排查，下一次 TTL 到期后继续尝试。
                 */
                log.warn("交易引擎业务监控快照刷新失败，返回上一轮快照 cacheTtlSeconds={}",
                        cacheTtl.toSeconds(),
                        ex);
                cachedUntil = now.plus(cacheTtl);
                return cachedSnapshot;
            }
        }
    }

    /**
     * 快照刷新操作抽象，便于单元测试验证缓存行为。
     */
    @FunctionalInterface
    public interface Operations {

        TradeEngineObservabilitySnapshot refreshSnapshot();
    }

    private static Duration normalizeCacheTtl(Duration ttl) {
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            return DEFAULT_CACHE_TTL;
        }
        return ttl;
    }

    static LocalDateTime legacyCardshopDbNow(Clock clock) {
        /*
         * 旧卡管库 datetime 字段按 Asia/Shanghai 语义写入。参见详细设计「9.1 派单 Outbox」：
         * 监控 max_age_seconds 与 worker 扫描必须使用同一旧库本地时间，否则 UTC Clock 会把滞留时间误差放大 8 小时。
         */
        return LocalDateTime.ofInstant(Objects.requireNonNull(clock, "clock must not be null").instant(),
                LEGACY_CARDSHOP_DB_ZONE);
    }

    private static final class RedisMysqlOperations implements Operations {

        private static final int DEFAULT_PENDING_ORG_SCAN_LIMIT = 512;
        private static final int DEFAULT_BALANCE_SAMPLE_LIMIT = 200;

        private final ReactiveStringRedisTemplate redisTemplate;
        private final RechargeRedisKeyBuilder keyBuilder;
        private final TradeEngineProperties properties;
        private final RechargeDispatchOutboxMapper dispatchOutboxMapper;
        private final RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper;
        private final RechargeAcceptAnomalyMapper acceptAnomalyMapper;
        private final McExtDepartMapper extDepartMapper;
        private final Clock clock;
        private final Duration redisTimeout;

        private RedisMysqlOperations(
                ReactiveStringRedisTemplate redisTemplate,
                RechargeRedisKeyBuilder keyBuilder,
                TradeEngineProperties properties,
                RechargeDispatchOutboxMapper dispatchOutboxMapper,
                RechargeBalanceDeltaOutboxMapper balanceDeltaOutboxMapper,
                RechargeAcceptAnomalyMapper acceptAnomalyMapper,
                McExtDepartMapper extDepartMapper,
                Clock clock) {
            this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
            this.keyBuilder = Objects.requireNonNull(keyBuilder, "keyBuilder must not be null");
            this.properties = Objects.requireNonNull(properties, "properties must not be null");
            this.dispatchOutboxMapper =
                    Objects.requireNonNull(dispatchOutboxMapper, "dispatchOutboxMapper must not be null");
            this.balanceDeltaOutboxMapper =
                    Objects.requireNonNull(balanceDeltaOutboxMapper, "balanceDeltaOutboxMapper must not be null");
            this.acceptAnomalyMapper = Objects.requireNonNull(acceptAnomalyMapper, "acceptAnomalyMapper must not be null");
            this.extDepartMapper = Objects.requireNonNull(extDepartMapper, "extDepartMapper must not be null");
            this.clock = Objects.requireNonNull(clock, "clock must not be null");
            this.redisTimeout = Duration.ofMillis(Math.max(300, properties.getRedis().getWaitTimeoutMs() * 10L));
        }

        @Override
        public TradeEngineObservabilitySnapshot refreshSnapshot() {
            long nowMillis = clock.millis();
            LocalDateTime now = legacyCardshopDbNow(clock);
            return new TradeEngineObservabilitySnapshot(
                    redisPending(nowMillis),
                    dispatchOutboxMetrics(now),
                    balanceDeltaMetrics(now),
                    acceptAnomalyMetrics(),
                    balanceDiffOrgCount(),
                    false);
        }

        private TradeEngineObservabilitySnapshot.RedisPending redisPending(long nowMillis) {
            long total = 0;
            long due = 0;
            long maxAgeSeconds = 0;
            int remainingOrgScan = DEFAULT_PENDING_ORG_SCAN_LIMIT;
            int bucketCount = properties.getRedis().getPendingOrgBucketCount();
            for (int bucket = 0; bucket < bucketCount && remainingOrgScan > 0; bucket++) {
                List<String> orgCodes = zRangeByScore(
                        keyBuilder.pendingOrgIndexKey(bucket),
                        0,
                        Long.MAX_VALUE,
                        remainingOrgScan);
                remainingOrgScan -= orgCodes.size();
                for (String orgCode : orgCodes) {
                    if (!StringUtils.hasText(orgCode)) {
                        continue;
                    }
                    String pendingKey = keyBuilder.pendingKey(orgCode);
                    total += zCard(pendingKey);
                    due += zCount(pendingKey, 0, nowMillis);
                    List<String> oldest = zRangeByScore(pendingKey, 0, Long.MAX_VALUE, 1);
                    if (!oldest.isEmpty()) {
                        Long score = zScore(pendingKey, oldest.getFirst());
                        if (score != null && score < nowMillis) {
                            maxAgeSeconds = Math.max(maxAgeSeconds, (nowMillis - score) / 1000);
                        }
                    }
                }
            }
            return new TradeEngineObservabilitySnapshot.RedisPending(total, due, maxAgeSeconds);
        }

        /**
         * 派单 outbox 积压：event_type + status，对应详细设计「9.1」。
         */
        private Map<TradeEngineObservabilitySnapshot.OutboxKey, TradeEngineObservabilitySnapshot.CountAge>
                dispatchOutboxMetrics(LocalDateTime now) {
            Map<TradeEngineObservabilitySnapshot.OutboxKey, TradeEngineObservabilitySnapshot.CountAge> result =
                    new LinkedHashMap<>();
            for (Map<String, Object> row : dispatchOutboxMapper.selectObservabilityDispatchOutboxMetrics(now)) {
                String eventType = text(row.get("event_type"));
                String status = text(row.get("status"));
                if (!StringUtils.hasText(eventType) || !StringUtils.hasText(status)) {
                    continue;
                }
                result.put(
                        new TradeEngineObservabilitySnapshot.OutboxKey(eventType, status),
                        new TradeEngineObservabilitySnapshot.CountAge(
                                longValue(row.get("count")),
                                longValue(row.get("max_age_seconds"))));
            }
            return result;
        }

        /**
         * 余额 delta 积压：仅 status，对应详细设计「9.2」；禁止 org_code 标签。
         */
        private Map<String, TradeEngineObservabilitySnapshot.CountAge> balanceDeltaMetrics(LocalDateTime now) {
            Map<String, TradeEngineObservabilitySnapshot.CountAge> result = new LinkedHashMap<>();
            for (Map<String, Object> row : balanceDeltaOutboxMapper.selectObservabilityBalanceDeltaMetrics(now)) {
                String status = text(row.get("status"));
                if (!StringUtils.hasText(status)) {
                    continue;
                }
                result.put(
                        status,
                        new TradeEngineObservabilitySnapshot.CountAge(
                                longValue(row.get("count")),
                                longValue(row.get("max_age_seconds"))));
            }
            return result;
        }

        private Map<TradeEngineObservabilitySnapshot.AcceptAnomalyKey, Long> acceptAnomalyMetrics() {
            Map<TradeEngineObservabilitySnapshot.AcceptAnomalyKey, Long> result = new LinkedHashMap<>();
            for (Map<String, Object> row : acceptAnomalyMapper.selectObservabilityOpenMetrics()) {
                String anomalyType = text(row.get("anomaly_type"));
                String status = text(row.get("status"));
                if (!StringUtils.hasText(anomalyType) || !StringUtils.hasText(status)) {
                    continue;
                }
                result.put(
                        new TradeEngineObservabilitySnapshot.AcceptAnomalyKey(anomalyType, status),
                        longValue(row.get("count")));
            }
            return result;
        }

        private long balanceDiffOrgCount() {
            List<McExtDepartDO> rows = extDepartMapper.selectBalanceRowsForShard(1, 0, DEFAULT_BALANCE_SAMPLE_LIMIT);
            long diffCount = 0;
            for (McExtDepartDO row : rows) {
                if (!StringUtils.hasText(row.getOrgCode())) {
                    continue;
                }
                Object value = redisTemplate.opsForHash()
                        .get(keyBuilder.balanceKey(row.getOrgCode()), "available")
                        .block(redisTimeout);
                if (value == null) {
                    continue;
                }
                long redisAvailable = parseLong(value);
                long mysqlBalance = row.getBalance() == null ? 0L : row.getBalance();
                Long unfinishedDeltaValue = balanceDeltaOutboxMapper.sumUnfinishedBalanceDelta(row.getOrgCode());
                long unfinishedDelta = unfinishedDeltaValue == null ? 0L : unfinishedDeltaValue;
                if (mysqlBalance + unfinishedDelta != redisAvailable) {
                    diffCount++;
                }
            }
            return diffCount;
        }

        private List<String> zRangeByScore(String key, long minScore, long maxScore, int limit) {
            List<String> values = redisTemplate.opsForZSet()
                    .rangeByScore(
                            key,
                            Range.closed((double) minScore, (double) maxScore),
                            Limit.limit().count(Math.max(1, limit)))
                    .collectList()
                    .block(redisTimeout);
            return values == null ? List.of() : values;
        }

        private long zCard(String key) {
            Long value = redisTemplate.opsForZSet().size(key).block(redisTimeout);
            return value == null ? 0L : value;
        }

        private long zCount(String key, long minScore, long maxScore) {
            Long value = redisTemplate.opsForZSet()
                    .count(key, Range.closed((double) minScore, (double) maxScore))
                    .block(redisTimeout);
            return value == null ? 0L : value;
        }

        private Long zScore(String key, String value) {
            Double score = redisTemplate.opsForZSet().score(key, value).block(redisTimeout);
            return score == null ? null : score.longValue();
        }

        private static String text(Object value) {
            return value == null ? "" : value.toString();
        }

        private static long longValue(Object value) {
            if (value instanceof Number number) {
                return number.longValue();
            }
            return parseLong(value);
        }

        private static long parseLong(Object value) {
            if (value == null) {
                return 0L;
            }
            String text = value.toString();
            if (!StringUtils.hasText(text)) {
                return 0L;
            }
            return Long.parseLong(text);
        }
    }
}
