package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * `accept_recharge_order.lua` 脚本契约测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6.1 Redis Lua 入参与 Key 构造」
 * 和「6. Redis 受理与缓存层」。该测试不替代真实 Redis 集成测试，但能阻止脚本漏掉 D4 必须的
 * 幂等、nonce、配置版本、余额预扣、pending、org_index 和订单事件写入。
 */
class AcceptRechargeOrderLuaContractTest {

    private static final Path SCRIPT = Path.of("src/main/resources/redis/lua/accept_recharge_order.lua");

    @Test
    void documentsStableKeysAndChecksDuplicateBeforeNonce() throws IOException {
        String script = Files.readString(SCRIPT);

        assertThat(script)
                .contains("-- 1 balanceKey")
                .contains("-- 2 idemKey")
                .contains("-- 8 nonceKey")
                .contains("-- 12 orgIndexBucketKey")
                .contains("-- 13 productSelectionKey")
                .contains("DUPLICATE")
                .contains("NONCE_REPLAY")
                .contains("BALANCE_REPAIRING");
        assertThat(script.indexOf("redis.call('EXISTS', KEYS[2])"))
                .isLessThan(script.indexOf("redis.call('SET', KEYS[8]"));
        assertThat(script.indexOf("redis.call('EXISTS', KEYS[2])"))
                .isLessThan(script.indexOf("redis.call('EXISTS', KEYS[11])"));
    }

    @Test
    void fencesParentSelectionAndHardExpiryInsideAtomicAccept() throws IOException {
        String script = Files.readString(SCRIPT);

        assertThat(script)
                .contains("local FIELD_PRODUCT_ID = 'product_id'")
                .contains("local FIELD_SELECTED_PRODUCT_ID = 'selected_product_id'")
                .contains("local FIELD_SALE_PRICE = 'sale_price'")
                .contains("local FIELD_PRODUCT_VERSION = 'product_version'")
                .contains("local FIELD_PRODUCT_TYPE = 'product_type'")
                .contains("local FIELD_PRODUCT_SNAPSHOT_AVAILABLE = 'product_snapshot_available'")
                .contains("local FIELD_CACHE_KIND = 'cache_kind'")
                .contains("local FIELD_SELECTION_FINGERPRINT = 'selection_fingerprint'")
                .contains("local FIELD_EXPIRE_AT = 'expire_at'")
                .contains("local CHILD_FIELD_STATUS = 'status'")
                .contains("local CHILD_FIELD_VERSION = 'version'")
                .contains("local CHILD_FIELD_PRICE = 'price'")
                .contains("redis.call('HGET', KEYS[13], FIELD_PRODUCT_ID)")
                .contains("redis.call('HGET', KEYS[13], FIELD_SELECTION_FINGERPRINT)")
                .contains("redis.call('HGET', KEYS[13], FIELD_EXPIRE_AT)")
                .contains("redis.call('HGET', KEYS[10], CHILD_FIELD_STATUS)")
                .contains("redis.call('HGET', KEYS[10], CHILD_FIELD_VERSION)")
                .contains("redis.call('HGET', KEYS[10], CHILD_FIELD_PRICE)")
                .contains("PRODUCT_SELECTION_STALE");
    }

    @Test
    void writesSuccessfulAndFailedOrdersWithPendingIndexesAndEvents() throws IOException {
        String script = Files.readString(SCRIPT);

        assertThat(script)
                .contains("ACCEPTED")
                .contains("ACCEPTED_FAILED")
                .contains("CONFIG_CHANGED")
                .contains("BALANCE_NOT_ENOUGH")
                .contains("MERCHANT_DISABLED")
                .contains("PRODUCT_NO_PERMISSION")
                .contains("NOT_RECHARGE_PRODUCT")
                .contains("PRICE_INVALID")
                .contains("redis.call('HINCRBY', KEYS[1], 'available'")
                .contains("redis.call('HINCRBY', KEYS[1], 'frozen'")
                .contains("redis.call('HSET', KEYS[3]")
                .contains("redis.call('HSET', KEYS[4]")
                .contains("redis.call('HSET', KEYS[7]")
                .contains("redis.call('ZADD', KEYS[5]")
                .contains("redis.call('ZADD', KEYS[6]")
                .contains("redis.call('ZADD', KEYS[12]");
    }

    @Test
    void setsFallbackTtlForShortLivedOrderAndNonceKeys() throws IOException {
        String script = Files.readString(SCRIPT);

        assertThat(script)
                .contains("redis.call('PEXPIRE', KEYS[2]")
                .contains("redis.call('PEXPIRE', KEYS[3]")
                .contains("redis.call('PEXPIRE', KEYS[4]")
                .contains("redis.call('PEXPIRE', KEYS[6]")
                .contains("redis.call('PEXPIRE', KEYS[7]")
                .contains("redis.call('PEXPIRE', KEYS[8]");
    }
}
