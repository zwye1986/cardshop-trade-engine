package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * 余额对账适配器测试。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.5 余额对账任务」。当前阶段只做
 * dev/test 可观测对账：比较 `mc_ext_depart.balance + 未完成 BALANCE_DELTA_FLUSH` 与 Redis available，
 * 发现差异后记录失败计数，不在对账任务内直接改钱。
 */
class MybatisRechargeBalanceReconcileAdapterTest {

    @Test
    void reportsSuccessWhenProjectedMysqlBalanceMatchesRedisAvailable() {
        FakeOperations operations = new FakeOperations(List.of(
                new MybatisRechargeBalanceReconcileAdapter.MerchantBalanceRow("A05", 1_000L)));
        operations.redisAvailable.put("A05", 900L);
        operations.unfinishedDelta.put("A05", -100L);
        MybatisRechargeBalanceReconcileAdapter adapter = new MybatisRechargeBalanceReconcileAdapter(operations);

        var result = adapter.reconcileDue(4, 2, 10, "worker-A");

        assertThat(result.scanned()).isEqualTo(1);
        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(result.failed()).isZero();
        assertThat(operations.selectCalls).containsExactly("4:2:10");
    }

    @Test
    void reportsFailureWhenProjectedMysqlBalanceDiffersFromRedisAvailable() {
        FakeOperations operations = new FakeOperations(List.of(
                new MybatisRechargeBalanceReconcileAdapter.MerchantBalanceRow("A05", 1_000L)));
        operations.redisAvailable.put("A05", 900L);
        operations.unfinishedDelta.put("A05", -50L);
        MybatisRechargeBalanceReconcileAdapter adapter = new MybatisRechargeBalanceReconcileAdapter(operations);

        var result = adapter.reconcileDue(1, 0, 10, "worker-A");

        assertThat(result.scanned()).isEqualTo(1);
        assertThat(result.succeeded()).isZero();
        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.skipped()).isZero();
    }

    @Test
    void skipsMerchantWhenRedisBalanceIsMissing() {
        FakeOperations operations = new FakeOperations(List.of(
                new MybatisRechargeBalanceReconcileAdapter.MerchantBalanceRow("A36", 500L)));
        MybatisRechargeBalanceReconcileAdapter adapter = new MybatisRechargeBalanceReconcileAdapter(operations);

        var result = adapter.reconcileDue(1, 0, 10, "worker-A");

        assertThat(result.scanned()).isEqualTo(1);
        assertThat(result.succeeded()).isZero();
        assertThat(result.failed()).isZero();
        assertThat(result.skipped()).isEqualTo(1);
    }

    private static final class FakeOperations implements MybatisRechargeBalanceReconcileAdapter.BalanceReconcileOperations {

        private final List<MybatisRechargeBalanceReconcileAdapter.MerchantBalanceRow> rows;
        private final List<String> selectCalls = new ArrayList<>();
        private final Map<String, Long> redisAvailable = new java.util.HashMap<>();
        private final Map<String, Long> unfinishedDelta = new java.util.HashMap<>();

        private FakeOperations(List<MybatisRechargeBalanceReconcileAdapter.MerchantBalanceRow> rows) {
            this.rows = rows;
        }

        @Override
        public List<MybatisRechargeBalanceReconcileAdapter.MerchantBalanceRow> selectMerchantBalancesForShard(
                int shardTotal,
                int shardIndex,
                int limit) {
            selectCalls.add(shardTotal + ":" + shardIndex + ":" + limit);
            return rows;
        }

        @Override
        public Optional<Long> readRedisAvailable(String orgCode) {
            return Optional.ofNullable(redisAvailable.get(orgCode));
        }

        @Override
        public long sumUnfinishedBalanceDelta(String orgCode) {
            return unfinishedDelta.getOrDefault(orgCode, 0L);
        }
    }
}
