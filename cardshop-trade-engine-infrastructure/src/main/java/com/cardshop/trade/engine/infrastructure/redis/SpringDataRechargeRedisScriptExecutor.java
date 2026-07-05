package com.cardshop.trade.engine.infrastructure.redis;

import java.util.List;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

/**
 * 基于 Spring Data Redis Reactive 客户端的 Lua 执行器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9. Redis 设计落地」和
 * 「20.13.6 Redis Lua 下单结果」。该类只封装客户端调用，不承载业务状态映射，方便后续 D5 增加 WAIT、
 * timeout 和 BUSY 分类处理。
 */
public class SpringDataRechargeRedisScriptExecutor implements RechargeRedisScriptExecutor {

    private final ReactiveStringRedisTemplate redisTemplate;

    public SpringDataRechargeRedisScriptExecutor(ReactiveStringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Mono<Object> execute(RedisScript<?> script, List<String> keys, List<String> args) {
        return redisTemplate.execute((RedisScript) script, keys, args).next().cast(Object.class);
    }
}
