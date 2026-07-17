package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * V1 商户配置 Redis 启动预热适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。Lua 接单前会校验
 * {@code merchant:config.status/version}，启动预热必须写入与旧库商户资料一致的门禁字段。
 */
class RedisV1MerchantConfigPreheatAdapterTest {

    @Test
    void writesEnabledMerchantConfigHashWithVersion() {
        FakeOperations operations = new FakeOperations();
        RedisV1MerchantConfigPreheatAdapter adapter = adapter(operations);

        adapter.preheat(new V1MerchantProfile("app-A39", "A39", "secret", true, 54L));

        assertThat(operations.key)
                .isEqualTo("cardshop:trade:recharge:{A39}:merchant:config");
        assertThat(operations.fields).containsExactly(
                Map.entry("status", "ENABLED"),
                Map.entry("version", "54"));
    }

    @Test
    void writesDisabledMerchantConfigHashWithSameVersion() {
        FakeOperations operations = new FakeOperations();
        RedisV1MerchantConfigPreheatAdapter adapter = adapter(operations);

        adapter.preheat(new V1MerchantProfile("app-B01", "B01", "secret", false, 77L));

        assertThat(operations.fields).containsExactly(
                Map.entry("status", "DISABLED"),
                Map.entry("version", "77"));
    }

    private static RedisV1MerchantConfigPreheatAdapter adapter(FakeOperations operations) {
        return new RedisV1MerchantConfigPreheatAdapter(
                operations,
                new RechargeRedisKeyBuilder("cardshop:trade", 128));
    }

    private static final class FakeOperations implements RedisV1MerchantConfigPreheatAdapter.MerchantConfigOperations {

        private String key;
        private final Map<String, String> fields = new LinkedHashMap<>();

        @Override
        public void write(String key, Map<String, String> fields) {
            this.key = key;
            this.fields.clear();
            this.fields.putAll(fields);
        }
    }
}
