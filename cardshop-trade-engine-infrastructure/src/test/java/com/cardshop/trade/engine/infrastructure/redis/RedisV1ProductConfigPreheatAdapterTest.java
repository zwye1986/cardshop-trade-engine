package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * V1 商品配置 Redis 比较修复测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。修复 Lua 必须先确认
 * 父商品快照仍与当前请求一致，避免旧请求把刚完成刷新的价格覆盖回旧值。
 */
class RedisV1ProductConfigPreheatAdapterTest {

    @Test
    void repairsMissingOrDriftedChildWhenParentSnapshotStillMatches() {
        FakeOperations operations = new FakeOperations(RedisV1ProductConfigPreheatAdapter.RepairOutcome.REPAIRED);
        RedisV1ProductConfigPreheatAdapter adapter = adapter(operations);

        adapter.refreshIfMissingOrStale("A39", minisoSelection());

        assertThat(operations.parentKey)
                .isEqualTo("cardshop:trade:recharge:{A39}:product-selection:80100022");
        assertThat(operations.childKey)
                .isEqualTo("cardshop:trade:recharge:{A39}:product:80100023:config");
        assertThat(operations.expectedParent).containsExactlyInAnyOrderEntriesOf(Map.ofEntries(
                Map.entry("selected_product_id", "80100023"),
                Map.entry("sale_price", "10000"),
                Map.entry("product_version", "1"),
                Map.entry("product_type", "1"),
                Map.entry("selection_fingerprint", minisoSelection().cacheFingerprint())));
        assertThat(operations.desiredChild).containsExactlyInAnyOrderEntriesOf(Map.of(
                "status", "ENABLED",
                "version", "1",
                "price", "10000",
                "product_type", "1"));
        assertThat(operations.physicalTtl).isEqualTo(Duration.ofMinutes(60));
    }

    @Test
    void doesNotOverwriteChildWhenParentSnapshotHasMovedToNewPrice() {
        FakeOperations operations = new FakeOperations(RedisV1ProductConfigPreheatAdapter.RepairOutcome.PARENT_CHANGED);
        RedisV1ProductConfigPreheatAdapter adapter = adapter(operations);

        adapter.refreshIfMissingOrStale("A39", minisoSelection());

        assertThat(operations.calls).isEqualTo(1);
    }

    @Test
    void skipsSelectionWithoutResolvedProductSnapshot() {
        FakeOperations operations = new FakeOperations(RedisV1ProductConfigPreheatAdapter.RepairOutcome.REPAIRED);
        RedisV1ProductConfigPreheatAdapter adapter = adapter(operations);

        adapter.refreshIfMissingOrStale("A39", V1ProductSelection.failed(
                "80100022", "PRODUCT_NO_PERMISSION", "商品无权限", 1L));

        assertThat(operations.calls).isZero();
    }

    @Test
    void repairsResolvedChildSnapshotEvenWhenCandidateValidationFailed() {
        FakeOperations operations = new FakeOperations(RedisV1ProductConfigPreheatAdapter.RepairOutcome.REPAIRED);
        RedisV1ProductConfigPreheatAdapter adapter = adapter(operations);

        adapter.refreshIfMissingOrStale("A39", failedButResolvedMinisoSelection());

        assertThat(operations.desiredChild).containsEntry("product_type", "0");
    }

    private static RedisV1ProductConfigPreheatAdapter adapter(FakeOperations operations) {
        return new RedisV1ProductConfigPreheatAdapter(
                operations,
                new RechargeRedisKeyBuilder("cardshop:trade", 128),
                Duration.ofMinutes(60));
    }

    private static V1ProductSelection minisoSelection() {
        return V1ProductSelection.passed(
                "80100022", "80100023", "MINISO", "名创直充100", "codekey",
                1, 10000L, 10000L, 9000L, 1L);
    }

    private static V1ProductSelection failedButResolvedMinisoSelection() {
        return new V1ProductSelection(
                "80100022", "80100023", "MINISO", "名创直充100", "codekey",
                0, 10000L, 10000L, 9000L, 1L,
                true, ValidationStatus.FAILED, "NOT_RECHARGE_PRODUCT", "该商品不是直充产品");
    }

    private static final class FakeOperations implements RedisV1ProductConfigPreheatAdapter.RedisProductConfigOperations {

        private final RedisV1ProductConfigPreheatAdapter.RepairOutcome outcome;
        private int calls;
        private String parentKey;
        private String childKey;
        private final Map<String, String> expectedParent = new LinkedHashMap<>();
        private final Map<String, String> desiredChild = new LinkedHashMap<>();
        private Duration physicalTtl;

        private FakeOperations(RedisV1ProductConfigPreheatAdapter.RepairOutcome outcome) {
            this.outcome = outcome;
        }

        @Override
        public RedisV1ProductConfigPreheatAdapter.RepairOutcome repairIfParentMatches(
                String parentKey,
                String childKey,
                Map<String, String> expectedParent,
                Map<String, String> desiredChild,
                Duration physicalTtl) {
            calls++;
            this.parentKey = parentKey;
            this.childKey = childKey;
            this.expectedParent.putAll(expectedParent);
            this.desiredChild.putAll(desiredChild);
            this.physicalTtl = physicalTtl;
            return outcome;
        }
    }
}
