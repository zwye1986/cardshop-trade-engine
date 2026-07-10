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
                .contains("DUPLICATE")
                .contains("NONCE_REPLAY")
                .contains("BALANCE_REPAIRING");
        assertThat(script.indexOf("redis.call('EXISTS', KEYS[2])"))
                .isLessThan(script.indexOf("redis.call('SET', KEYS[8]"));
        assertThat(script.indexOf("redis.call('EXISTS', KEYS[2])"))
                .isLessThan(script.indexOf("redis.call('EXISTS', KEYS[11])"));
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
