package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairPort;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.util.StringUtils;

/**
 * Redis 商户余额基线修复适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.4 缓存修复任务」。当前阶段先覆盖
 * dev/test 联调必须的余额基线预热：在持有商户级 {@code balance:repairing_lock} 时，用
 * {@code mc_ext_depart.balance} 重建 Redis {@code available/frozen/version} 字段。批量商品配置扫描和全量重建
 * 仍按文档作为后续任务；V1 请求级缺失/漂移比较修复由 {@code V1ProductConfigPreheatPort} 单独承担。
 */
public class RedisRechargeCacheRepairAdapter implements RechargeCacheRepairPort {

    private static final Logger log = LoggerFactory.getLogger(RedisRechargeCacheRepairAdapter.class);
    private static final Duration DEFAULT_REPAIR_LOCK_TTL = Duration.ofMinutes(5);

    private final CacheRepairOperations operations;
    private final Duration repairLockTtl;

    public RedisRechargeCacheRepairAdapter(
            McExtDepartMapper extDepartMapper,
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            Duration repairLockTtl,
            Duration redisTimeout) {
        this(new RedisCacheRepairOperations(
                extDepartMapper,
                redisTemplate,
                keyBuilder,
                redisTimeout),
                repairLockTtl);
    }

    RedisRechargeCacheRepairAdapter(CacheRepairOperations operations, Duration repairLockTtl) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
        this.repairLockTtl = normalizeLockTtl(repairLockTtl);
    }

    @Override
    public RechargeCompensationBatchResult repairDue(int shardTotal, int shardIndex, int limit, String workerId) {
        List<MerchantBalanceBaseline> baselines =
                operations.selectMerchantBalanceBaselines(shardTotal, shardIndex, limit);
        int succeeded = 0;
        int failed = 0;
        int skipped = 0;
        for (MerchantBalanceBaseline baseline : baselines) {
            if (!operations.acquireRepairLock(baseline.orgCode(), workerId, repairLockTtl)) {
                skipped++;
                continue;
            }
            try {
                operations.writeBalanceBaseline(
                        baseline.orgCode(),
                        baseline.available(),
                        0L,
                        baseline.version());
                succeeded++;
            } catch (RuntimeException ex) {
                failed++;
                log.error("Redis余额基线修复失败 orgCode={}, workerId={}", baseline.orgCode(), workerId, ex);
            } finally {
                operations.releaseRepairLock(baseline.orgCode(), workerId);
            }
        }
        return new RechargeCompensationBatchResult(baselines.size(), succeeded, failed, skipped);
    }

    public record MerchantBalanceBaseline(String orgCode, long available, long version) {
    }

    interface CacheRepairOperations {

        List<MerchantBalanceBaseline> selectMerchantBalanceBaselines(int shardTotal, int shardIndex, int limit);

        boolean acquireRepairLock(String orgCode, String workerId, Duration lockTtl);

        void writeBalanceBaseline(String orgCode, long available, long frozen, long version);

        void releaseRepairLock(String orgCode, String workerId);
    }

    private static final class RedisCacheRepairOperations implements CacheRepairOperations {

        private final McExtDepartMapper extDepartMapper;
        private final ReactiveStringRedisTemplate redisTemplate;
        private final RechargeRedisKeyBuilder keyBuilder;
        private final Duration redisTimeout;

        private RedisCacheRepairOperations(
                McExtDepartMapper extDepartMapper,
                ReactiveStringRedisTemplate redisTemplate,
                RechargeRedisKeyBuilder keyBuilder,
                Duration redisTimeout) {
            this.extDepartMapper = Objects.requireNonNull(extDepartMapper, "extDepartMapper must not be null");
            this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
            this.keyBuilder = Objects.requireNonNull(keyBuilder, "keyBuilder must not be null");
            this.redisTimeout = normalizeRedisTimeout(redisTimeout);
        }

        @Override
        public List<MerchantBalanceBaseline> selectMerchantBalanceBaselines(int shardTotal, int shardIndex, int limit) {
            return extDepartMapper.selectBalanceRowsForShard(shardTotal, shardIndex, limit)
                    .stream()
                    .filter(row -> StringUtils.hasText(row.getOrgCode()))
                    .map(row -> new MerchantBalanceBaseline(
                            row.getOrgCode(),
                            row.getBalance() == null ? 0L : row.getBalance(),
                            row.getId() == null ? 0L : row.getId()))
                    .toList();
        }

        @Override
        public boolean acquireRepairLock(String orgCode, String workerId, Duration lockTtl) {
            Boolean acquired = redisTemplate.opsForValue()
                    .setIfAbsent(keyBuilder.balanceRepairingLockKey(orgCode), workerId, lockTtl)
                    .block(redisTimeout);
            return Boolean.TRUE.equals(acquired);
        }

        @Override
        public void writeBalanceBaseline(String orgCode, long available, long frozen, long version) {
            redisTemplate.opsForHash()
                    .putAll(keyBuilder.balanceKey(orgCode), Map.of(
                            "available", Long.toString(available),
                            "frozen", Long.toString(frozen),
                            "version", Long.toString(version)))
                    .block(redisTimeout);
        }

        @Override
        public void releaseRepairLock(String orgCode, String workerId) {
            String lockKey = keyBuilder.balanceRepairingLockKey(orgCode);
            String owner = redisTemplate.opsForValue().get(lockKey).block(redisTimeout);
            if (workerId.equals(owner)) {
                redisTemplate.delete(lockKey).block(redisTimeout);
            }
        }

        private static Duration normalizeRedisTimeout(Duration timeout) {
            if (timeout == null || timeout.isZero() || timeout.isNegative()) {
                return Duration.ofSeconds(3);
            }
            return timeout;
        }
    }

    private static Duration normalizeLockTtl(Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return DEFAULT_REPAIR_LOCK_TTL;
        }
        return ttl;
    }
}
