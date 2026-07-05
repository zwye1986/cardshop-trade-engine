package com.cardshop.trade.engine.infrastructure.redis;

import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * 直充 Redis Lua 脚本加载器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.14 实施任务拆分」任务 5。
 * 所有脚本通过 classpath 固定路径加载，避免运行时从文件系统读取导致部署包路径差异；Spring Data Redis
 * 会基于脚本文本计算 SHA1，后续 Redis 端可复用脚本缓存。
 */
public class RechargeRedisScriptLoader {

    private static final String ACCEPT_RECHARGE_ORDER_SCRIPT =
            "redis/lua/accept_recharge_order.lua";

    private final RedisScript<List> acceptRechargeOrderScript;

    public RechargeRedisScriptLoader() {
        this.acceptRechargeOrderScript = load(ACCEPT_RECHARGE_ORDER_SCRIPT);
    }

    public RedisScript<List> acceptRechargeOrderScript() {
        return acceptRechargeOrderScript;
    }

    private static RedisScript<List> load(String location) {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(location));
        script.setResultType(List.class);
        return script;
    }
}
