package com.cardshop.trade.engine.infrastructure.redis;

import java.time.Duration;
import java.util.List;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

/**
 * 一次直充接单可靠性链路使用的 Redis 会话。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「14.2 Redis WAIT 协议」。Redis WAIT
 * 的确认语义绑定“当前客户端连接此前发出的写命令”，因此 accept Lua 与 WAIT 不能拆到两条独立连接。
 * 该抽象把 Lettuce 连接生命周期收敛在 infrastructure 内，应用层只看到接单与可靠性端口。
 */
public interface RechargeRedisSession extends AutoCloseable {

    /**
     * 在当前 Redis 会话上执行 Lua 脚本。
     */
    Mono<Object> execute(RedisScript<?> script, List<String> keys, List<String> args);

    /**
     * 在同一个 Redis 会话上执行 WAIT，确认此前写入的复制进度。
     */
    Mono<Long> waitForReplication(int replicas, Duration timeout);

    @Override
    void close();
}
