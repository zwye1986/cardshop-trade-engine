package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V1 商品配置 Redis 补热测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * Java 已经从 MySQL 形成商品快照，Redis 缺少商品配置时只能按该快照补写实际子商品 key，
 * 之后仍由接单 Lua 做版本、价格和类型的最终 fail closed 校验。
 */
class RedisV1ProductConfigPreheatAdapterTest {

    @Test
    void writesSelectedChildProductConfigWhenMissing() {
        FakeOperations operations = new FakeOperations(true);
        RedisV1ProductConfigPreheatAdapter adapter = new RedisV1ProductConfigPreheatAdapter(
                operations,
                new RechargeRedisKeyBuilder("cardshop:trade", 128));

        adapter.preheatIfMissing("A39", minisoSelection());

        assertThat(operations.writtenKey).isEqualTo("cardshop:trade:recharge:{A39}:product:80100023:config");
        assertThat(operations.writtenFields).containsExactlyInAnyOrderEntriesOf(Map.of(
                "status", "ENABLED",
                "version", "1",
                "price", "1",
                "product_type", "1"));
        assertThat(operations.putIfAbsentCalls).isEqualTo(1);
    }

    @Test
    void doesNotOverwriteExistingProductConfig() {
        FakeOperations operations = new FakeOperations(false);
        RedisV1ProductConfigPreheatAdapter adapter = new RedisV1ProductConfigPreheatAdapter(
                operations,
                new RechargeRedisKeyBuilder("cardshop:trade", 128));

        adapter.preheatIfMissing("A39", minisoSelection());

        assertThat(operations.writtenFields).isEmpty();
        assertThat(operations.putIfAbsentCalls).isEqualTo(1);
    }

    @Test
    void skipsPreheatForFailedProductSelection() {
        FakeOperations operations = new FakeOperations(true);
        RedisV1ProductConfigPreheatAdapter adapter = new RedisV1ProductConfigPreheatAdapter(
                operations,
                new RechargeRedisKeyBuilder("cardshop:trade", 128));

        adapter.preheatIfMissing("A39", V1ProductSelection.failed(
                "80100022",
                "PRODUCT_NO_PERMISSION",
                "商品无权限",
                1L));

        assertThat(operations.putIfAbsentCalls).isZero();
        assertThat(operations.writtenFields).isEmpty();
    }

    @Test
    void writesResolvedChildSnapshotEvenWhenCandidateValidationFailed() {
        FakeOperations operations = new FakeOperations(true);
        RedisV1ProductConfigPreheatAdapter adapter = new RedisV1ProductConfigPreheatAdapter(
                operations,
                new RechargeRedisKeyBuilder("cardshop:trade", 128));

        adapter.preheatIfMissing("A39", failedButResolvedMinisoSelection());

        assertThat(operations.writtenKey).isEqualTo("cardshop:trade:recharge:{A39}:product:80100023:config");
        assertThat(operations.writtenFields).containsEntry("product_type", "0");
    }

    private static V1ProductSelection minisoSelection() {
        return new V1ProductSelection(
                "80100022",
                "80100023",
                "MINISO",
                "名创直充100",
                "codekey",
                1,
                1L,
                10000L,
                1L,
                1L,
                true,
                ValidationStatus.PASSED,
                "",
                "");
    }

    private static V1ProductSelection failedButResolvedMinisoSelection() {
        return new V1ProductSelection(
                "80100022",
                "80100023",
                "MINISO",
                "名创直充100",
                "codekey",
                0,
                1L,
                1L,
                1L,
                1L,
                true,
                ValidationStatus.FAILED,
                "NOT_RECHARGE_PRODUCT",
                "该商品不是直充产品");
    }

    private static final class FakeOperations implements RedisV1ProductConfigPreheatAdapter.RedisProductConfigOperations {

        private final boolean inserted;
        private int putIfAbsentCalls;
        private String writtenKey;
        private final Map<String, String> writtenFields = new LinkedHashMap<>();

        private FakeOperations(boolean inserted) {
            this.inserted = inserted;
        }

        @Override
        public boolean putIfAbsent(String key, Map<String, String> fields) {
            putIfAbsentCalls++;
            if (inserted) {
                writtenKey = key;
                writtenFields.putAll(fields);
            }
            return inserted;
        }
    }
}
