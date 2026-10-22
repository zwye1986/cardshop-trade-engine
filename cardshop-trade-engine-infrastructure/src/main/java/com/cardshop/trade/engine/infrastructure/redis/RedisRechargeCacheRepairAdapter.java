package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.recharge.RechargeCacheRepairPort;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.StringUtils;

/**
 * Redis 商户余额基线修复适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.4 缓存修复任务」。当前阶段先覆盖
 * dev/test 新商户余额初始化：已有balance不覆盖，只有Redis无pending且MySQL无新引擎历史才初始化。
 * 参见详细设计「9.2 成功预扣收尾最小修订」。批量商品配置扫描和全量重建
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
            String owner = workerId + ":" + UUID.randomUUID();
            if (!operations.acquireRepairLock(baseline.orgCode(), owner, repairLockTtl)) {
                skipped++;
                continue;
            }
            try {
                if (operations.initializeBalanceIfSafe(baseline.orgCode(), owner)) succeeded++;
                else skipped++;
            } catch (RuntimeException ex) {
                failed++;
                log.error("Redis余额基线修复失败 orgCode={}, workerId={}", baseline.orgCode(), workerId, ex);
            } finally {
                operations.releaseRepairLock(baseline.orgCode(), owner);
            }
        }
        return new RechargeCompensationBatchResult(baselines.size(), succeeded, failed, skipped);
    }

    public record MerchantBalanceBaseline(String orgCode, long available, long version) {
    }

    interface CacheRepairOperations {

        List<MerchantBalanceBaseline> selectMerchantBalanceBaselines(int shardTotal, int shardIndex, int limit);

        boolean acquireRepairLock(String orgCode, String workerId, Duration lockTtl);

        boolean initializeBalanceIfSafe(String orgCode, String owner);

        void releaseRepairLock(String orgCode, String workerId);
    }

    private static final class RedisCacheRepairOperations implements CacheRepairOperations {

        // 相同owner令牌下先读后写两次校验；SQL必须位于两次Redis检查之间，不能使用扫描时的旧余额。
        private static final DefaultRedisScript<Long> INITIALIZE_IF_SAFE = new DefaultRedisScript<>("""
                if redis.call('GET', KEYS[1]) ~= ARGV[1]
                    or redis.call('EXISTS', KEYS[2]) == 1
                    or redis.call('ZCARD', KEYS[3]) ~= 0 then return 0 end
                if ARGV[2] == 'CHECK' then return 1 end
                redis.call('HSET', KEYS[2], 'available', ARGV[3], 'frozen', '0', 'version', ARGV[4])
                return 1
                """, Long.class);
        private static final DefaultRedisScript<Long> UNLOCK_IF_OWNED = new DefaultRedisScript<>("""
                if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
                return 0
                """, Long.class);

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
        public boolean initializeBalanceIfSafe(String orgCode, String owner) {
            var keys = List.of(keyBuilder.balanceRepairingLockKey(orgCode), keyBuilder.balanceKey(orgCode),
                    keyBuilder.pendingKey(orgCode));
            Long safe = redisTemplate.execute(INITIALIZE_IF_SAFE, keys, List.of(owner, "CHECK"))
                    .next().block(redisTimeout);
            if (!Long.valueOf(1).equals(safe)) return false;
            // TODO: 有历史商户的余额丢失须另行实现受控逐单重建，不能覆写为MySQL余额/frozen0；见详细设计9.2。
            McExtDepartDO source = extDepartMapper.selectUnusedMerchantBalance(orgCode);
            if (source == null || source.getBalance() == null || source.getId() == null) return false;
            Long initialized = redisTemplate.execute(INITIALIZE_IF_SAFE, keys,
                    List.of(owner, "WRITE", Long.toString(source.getBalance()), Long.toString(source.getId())))
                    .next().block(redisTimeout);
            return Long.valueOf(1).equals(initialized);
        }

        @Override
        public void releaseRepairLock(String orgCode, String workerId) {
            String lockKey = keyBuilder.balanceRepairingLockKey(orgCode);
            redisTemplate.execute(UNLOCK_IF_OWNED, List.of(lockKey), List.of(workerId)).next().block(redisTimeout);
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
