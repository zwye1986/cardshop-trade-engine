package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Redis 缓存修复/余额基线预热适配器测试。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.4 缓存修复任务」。当前最小实现先覆盖
 * dev/test 联调最需要的商户余额基线：必须持有 `balance:repairing_lock` 后才能重写 balance hash。
 */
class RedisRechargeCacheRepairAdapterTest {

    @Test
    void initializesMissingBalanceOnlyWhenSafetyChecksPass() {
        FakeOperations operations = new FakeOperations(List.of(
                new RedisRechargeCacheRepairAdapter.MerchantBalanceBaseline("A05", 1_000L, 22L)));
        operations.lockAcquired = true;
        RedisRechargeCacheRepairAdapter adapter = new RedisRechargeCacheRepairAdapter(
                operations,
                Duration.ofMinutes(5));

        var result = adapter.repairDue(2, 1, 10, "worker-A");

        assertThat(result.scanned()).isEqualTo(1);
        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(result.failed()).isZero();
        assertThat(result.skipped()).isZero();
        assertThat(operations.writes).containsExactly("A05:1000:0:22");
        assertThat(operations.releases).hasSize(1);
        assertThat(operations.releases.getFirst()).startsWith("A05:worker-A:");
    }

    @Test
    void existingOrUnsafeBalanceIsSkippedWithoutOverwritingFunds() {
        FakeOperations operations = new FakeOperations(List.of(
                new RedisRechargeCacheRepairAdapter.MerchantBalanceBaseline("A35", 1_000L, 56L)));
        operations.lockAcquired = true;
        operations.initializationSafe = false;
        var result = new RedisRechargeCacheRepairAdapter(operations, Duration.ofMinutes(5))
                .repairDue(1, 0, 10, "worker");
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(result.succeeded()).isZero();
        assertThat(operations.writes).isEmpty();
        assertThat(operations.releases).hasSize(1);
    }

    @Test
    void skipsBalanceBaselineWhenRepairLockIsHeldByAnotherWorker() {
        FakeOperations operations = new FakeOperations(List.of(
                new RedisRechargeCacheRepairAdapter.MerchantBalanceBaseline("A36", 500L, 35L)));
        RedisRechargeCacheRepairAdapter adapter = new RedisRechargeCacheRepairAdapter(
                operations,
                Duration.ofMinutes(5));

        var result = adapter.repairDue(1, 0, 10, "worker-B");

        assertThat(result.scanned()).isEqualTo(1);
        assertThat(result.succeeded()).isZero();
        assertThat(result.failed()).isZero();
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(operations.writes).isEmpty();
        assertThat(operations.releases).isEmpty();
    }

    private static final class FakeOperations implements RedisRechargeCacheRepairAdapter.CacheRepairOperations {

        private final List<RedisRechargeCacheRepairAdapter.MerchantBalanceBaseline> baselines;
        private final List<String> writes = new ArrayList<>();
        private final List<String> releases = new ArrayList<>();
        private boolean lockAcquired;
        private boolean initializationSafe = true;

        private FakeOperations(List<RedisRechargeCacheRepairAdapter.MerchantBalanceBaseline> baselines) {
            this.baselines = baselines;
        }

        @Override
        public List<RedisRechargeCacheRepairAdapter.MerchantBalanceBaseline> selectMerchantBalanceBaselines(
                int shardTotal,
                int shardIndex,
                int limit) {
            return baselines;
        }

        @Override
        public boolean acquireRepairLock(String orgCode, String workerId, Duration lockTtl) {
            return lockAcquired;
        }

        @Override
        public boolean initializeBalanceIfSafe(String orgCode, String owner) {
            if (!initializationSafe) return false;
            var baseline = baselines.stream().filter(row -> row.orgCode().equals(orgCode)).findFirst().orElseThrow();
            writes.add(orgCode + ":" + baseline.available() + ":0:" + baseline.version());
            return true;
        }

        @Override
        public void releaseRepairLock(String orgCode, String workerId) {
            releases.add(orgCode + ":" + workerId);
        }
    }
}
