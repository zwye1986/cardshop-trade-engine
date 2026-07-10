package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.RechargeBalanceReconcilePort;
import com.cardshop.trade.engine.application.recharge.RechargeCompensationBatchResult;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.cardshop.trade.engine.infrastructure.redis.RechargeRedisKeyBuilder;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.util.StringUtils;

/**
 * 基于 MySQL + Redis 的商户余额对账适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.5 余额对账任务」。当前实现只在
 * dev/test 阶段做“发现差异”：计算 {@code mc_ext_depart.balance + 未完成 BALANCE_DELTA_FLUSH delta}
 * 是否等于 Redis {@code balance.available}。发现差异只返回失败计数和日志，不在对账线程直接释放资金或覆盖旧表余额。
 */
public class MybatisRechargeBalanceReconcileAdapter implements RechargeBalanceReconcilePort {

    private static final Logger log = LoggerFactory.getLogger(MybatisRechargeBalanceReconcileAdapter.class);

    private final BalanceReconcileOperations operations;

    public MybatisRechargeBalanceReconcileAdapter(
            McExtDepartMapper extDepartMapper,
            RechargeOutboxMapper outboxMapper,
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            Duration redisTimeout) {
        this(new MybatisBalanceReconcileOperations(
                extDepartMapper,
                outboxMapper,
                redisTemplate,
                keyBuilder,
                redisTimeout));
    }

    MybatisRechargeBalanceReconcileAdapter(BalanceReconcileOperations operations) {
        this.operations = Objects.requireNonNull(operations, "operations must not be null");
    }

    @Override
    public RechargeCompensationBatchResult reconcileDue(int shardTotal, int shardIndex, int limit, String workerId) {
        List<MerchantBalanceRow> rows = operations.selectMerchantBalancesForShard(shardTotal, shardIndex, limit);
        int succeeded = 0;
        int failed = 0;
        int skipped = 0;
        for (MerchantBalanceRow row : rows) {
            Optional<Long> maybeRedisAvailable = operations.readRedisAvailable(row.orgCode());
            if (maybeRedisAvailable.isEmpty()) {
                skipped++;
                log.warn("余额对账跳过Redis余额缺失 orgCode={}, workerId={}", row.orgCode(), workerId);
                continue;
            }
            long unfinishedDelta = operations.sumUnfinishedBalanceDelta(row.orgCode());
            long projectedMysqlAvailable = row.mysqlBalance() + unfinishedDelta;
            long redisAvailable = maybeRedisAvailable.get();
            if (projectedMysqlAvailable == redisAvailable) {
                succeeded++;
                continue;
            }
            failed++;
            log.error("余额对账发现差异 orgCode={}, mysqlBalance={}, unfinishedDelta={}, projectedMysqlAvailable={}, redisAvailable={}, workerId={}",
                    row.orgCode(),
                    row.mysqlBalance(),
                    unfinishedDelta,
                    projectedMysqlAvailable,
                    redisAvailable,
                    workerId);
        }
        return new RechargeCompensationBatchResult(rows.size(), succeeded, failed, skipped);
    }

    public record MerchantBalanceRow(String orgCode, long mysqlBalance) {
    }

    interface BalanceReconcileOperations {

        List<MerchantBalanceRow> selectMerchantBalancesForShard(int shardTotal, int shardIndex, int limit);

        Optional<Long> readRedisAvailable(String orgCode);

        long sumUnfinishedBalanceDelta(String orgCode);
    }

    private static final class MybatisBalanceReconcileOperations implements BalanceReconcileOperations {

        private final McExtDepartMapper extDepartMapper;
        private final RechargeOutboxMapper outboxMapper;
        private final ReactiveStringRedisTemplate redisTemplate;
        private final RechargeRedisKeyBuilder keyBuilder;
        private final Duration redisTimeout;

        private MybatisBalanceReconcileOperations(
                McExtDepartMapper extDepartMapper,
                RechargeOutboxMapper outboxMapper,
                ReactiveStringRedisTemplate redisTemplate,
                RechargeRedisKeyBuilder keyBuilder,
                Duration redisTimeout) {
            this.extDepartMapper = Objects.requireNonNull(extDepartMapper, "extDepartMapper must not be null");
            this.outboxMapper = Objects.requireNonNull(outboxMapper, "outboxMapper must not be null");
            this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
            this.keyBuilder = Objects.requireNonNull(keyBuilder, "keyBuilder must not be null");
            this.redisTimeout = normalizeTimeout(redisTimeout);
        }

        @Override
        public List<MerchantBalanceRow> selectMerchantBalancesForShard(int shardTotal, int shardIndex, int limit) {
            return extDepartMapper.selectBalanceRowsForShard(shardTotal, shardIndex, limit)
                    .stream()
                    .filter(row -> StringUtils.hasText(row.getOrgCode()))
                    .map(row -> new MerchantBalanceRow(row.getOrgCode(), row.getBalance() == null ? 0L : row.getBalance()))
                    .toList();
        }

        @Override
        public Optional<Long> readRedisAvailable(String orgCode) {
            Object value = redisTemplate.opsForHash()
                    .get(keyBuilder.balanceKey(orgCode), "available")
                    .block(redisTimeout);
            if (value == null) {
                return Optional.empty();
            }
            String text = value.toString();
            if (!StringUtils.hasText(text)) {
                return Optional.empty();
            }
            return Optional.of(Long.parseLong(text));
        }

        @Override
        public long sumUnfinishedBalanceDelta(String orgCode) {
            Long value = outboxMapper.sumUnfinishedBalanceDelta(orgCode);
            return value == null ? 0L : value;
        }

        private static Duration normalizeTimeout(Duration timeout) {
            if (timeout == null || timeout.isZero() || timeout.isNegative()) {
                return Duration.ofSeconds(3);
            }
            return timeout;
        }
    }
}
