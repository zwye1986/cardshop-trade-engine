package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.recharge.DirectRechargeFrequencyPort;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.util.StringUtils;

/**
 * 基于 Redis 的同账号直充限频锁。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」。锁 key 使用
 * {@code recharge:{orgCode}:deliver:frequency:{account}}，锁值为 orderId，释放时必须比较 owner。
 */
public class RedisDirectRechargeFrequencyAdapter implements DirectRechargeFrequencyPort {

    private static final Logger log = LoggerFactory.getLogger(RedisDirectRechargeFrequencyAdapter.class);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(300);

    private final ReactiveStringRedisTemplate redisTemplate;
    private final Duration operationTimeout;

    public RedisDirectRechargeFrequencyAdapter(ReactiveStringRedisTemplate redisTemplate, Duration operationTimeout) {
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
        this.operationTimeout = Objects.requireNonNull(operationTimeout, "operationTimeout must not be null");
    }

    @Override
    public boolean acquire(String orgCode, String account, String orderId, Duration ttl) {
        String key = key(orgCode, account);
        if (!StringUtils.hasText(key)) {
            log.warn("直充限频锁缺少必要字段，跳过限频 orderId={}", orderId);
            return true;
        }
        try {
            Boolean acquired = redisTemplate.opsForValue()
                    .setIfAbsent(key, orderId, ttl)
                    .block(timeout());
            if (Boolean.TRUE.equals(acquired)) {
                return true;
            }
            String owner = redisTemplate.opsForValue().get(key).block(timeout());
            if (Objects.equals(owner, orderId)) {
                redisTemplate.expire(key, ttl).block(timeout());
                return true;
            }
            return false;
        } catch (Exception ex) {
            log.error("直充限频锁获取异常 orderId={}, key={}", orderId, key, ex);
            return true;
        }
    }

    @Override
    public void releaseIfOwned(String orgCode, String account, String orderId, Duration minimumHold) {
        String key = key(orgCode, account);
        if (!StringUtils.hasText(key)) {
            return;
        }
        try {
            String owner = redisTemplate.opsForValue().get(key).block(timeout());
            if (!Objects.equals(owner, orderId)) {
                return;
            }
            if (minimumHold != null && !minimumHold.isZero() && !minimumHold.isNegative()) {
                redisTemplate.expire(key, minimumHold).block(timeout());
            } else {
                redisTemplate.delete(key).block(timeout());
            }
        } catch (Exception ex) {
            log.error("直充限频锁释放异常 orderId={}, key={}", orderId, key, ex);
        }
    }

    private Duration timeout() {
        return operationTimeout.isZero() || operationTimeout.isNegative() ? DEFAULT_TIMEOUT : operationTimeout;
    }

    private static String key(String orgCode, String account) {
        if (!StringUtils.hasText(orgCode) || !StringUtils.hasText(account)) {
            return "";
        }
        return "recharge:" + orgCode + ":deliver:frequency:" + account;
    }
}
