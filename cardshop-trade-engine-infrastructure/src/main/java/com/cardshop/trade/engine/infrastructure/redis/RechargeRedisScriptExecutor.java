package com.cardshop.trade.engine.infrastructure.redis;

import java.util.List;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

/**
 * Redis Lua 脚本执行器抽象。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * 该接口把 Spring Data Redis 客户端隔离在一层很薄的适配器后面，业务接单适配器只关心脚本、KEYS 和 ARGV，
 * 测试也可以用手写替身覆盖执行结果，避免依赖真实 Redis 或 JVM agent mock 能力。
 */
public interface RechargeRedisScriptExecutor {

    /**
     * 执行一个 Redis Lua 脚本并返回首个结果。
     */
    Mono<Object> execute(RedisScript<?> script, List<String> keys, List<String> args);
}
