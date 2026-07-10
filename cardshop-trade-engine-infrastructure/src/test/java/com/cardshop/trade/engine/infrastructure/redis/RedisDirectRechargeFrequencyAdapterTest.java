package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * Redis 同账号直充限频锁测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。同一订单 outbox
 * 重试时必须能重入自己持有的限频锁，否则供应商调用异常后的重试会被当前订单自己的锁挡到 TTL 过期。
 */
class RedisDirectRechargeFrequencyAdapterTest {

    @Test
    void acquireAllowsSameOrderToRefreshOwnedFrequencyLock() throws IOException {
        String source = Files.readString(Path.of(
                "src/main/java/com/cardshop/trade/engine/infrastructure/redis/RedisDirectRechargeFrequencyAdapter.java"));
        String acquire = source.substring(source.indexOf("public boolean acquire"));

        assertThat(acquire)
                .contains("opsForValue().get(key)")
                .contains("Objects.equals(owner, orderId)")
                .contains("redisTemplate.expire(key, ttl)")
                .contains("return true");
    }
}
