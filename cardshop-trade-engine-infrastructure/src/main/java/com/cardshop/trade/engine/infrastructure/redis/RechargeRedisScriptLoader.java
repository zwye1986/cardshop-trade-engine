package com.cardshop.trade.engine.infrastructure.redis;

import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * 直充 Redis Lua 脚本加载器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「13. 测试与验收标准」任务 5。
 * 所有脚本通过 classpath 固定路径加载，避免运行时从文件系统读取导致部署包路径差异；Spring Data Redis
 * 会基于脚本文本计算 SHA1，后续 Redis 端可复用脚本缓存。
 */
public class RechargeRedisScriptLoader {

    private static final String ACCEPT_RECHARGE_ORDER_SCRIPT =
            "redis/lua/accept_recharge_order.lua";
    private static final String CANCEL_UNCONFIRMED_ACCEPT_SCRIPT =
            "redis/lua/cancel_unconfirmed_accept.lua";
    private static final String MARK_PERSIST_QUEUED_SCRIPT =
            "redis/lua/mark_persist_queued.lua";
    private static final String MARK_DB_PERSISTED_SCRIPT =
            "redis/lua/mark_db_persisted.lua";
    private static final String MARK_PERSIST_FAILED_SCRIPT =
            "redis/lua/mark_persist_failed.lua";
    private static final String RELEASE_BALANCE_RESERVATION_SCRIPT =
            "redis/lua/release_balance_reservation.lua";

    private final RedisScript<List> acceptRechargeOrderScript;
    private final RedisScript<List> cancelUnconfirmedAcceptScript;
    private final RedisScript<List> markPersistQueuedScript;
    private final RedisScript<List> markDbPersistedScript;
    private final RedisScript<List> markPersistFailedScript;
    private final RedisScript<List> releaseBalanceReservationScript;

    public RechargeRedisScriptLoader() {
        this.acceptRechargeOrderScript = load(ACCEPT_RECHARGE_ORDER_SCRIPT);
        this.cancelUnconfirmedAcceptScript = load(CANCEL_UNCONFIRMED_ACCEPT_SCRIPT);
        this.markPersistQueuedScript = load(MARK_PERSIST_QUEUED_SCRIPT);
        this.markDbPersistedScript = load(MARK_DB_PERSISTED_SCRIPT);
        this.markPersistFailedScript = load(MARK_PERSIST_FAILED_SCRIPT);
        this.releaseBalanceReservationScript = load(RELEASE_BALANCE_RESERVATION_SCRIPT);
    }

    public RedisScript<List> acceptRechargeOrderScript() {
        return acceptRechargeOrderScript;
    }

    public RedisScript<List> cancelUnconfirmedAcceptScript() {
        return cancelUnconfirmedAcceptScript;
    }

    public RedisScript<List> markPersistQueuedScript() {
        return markPersistQueuedScript;
    }

    public RedisScript<List> markDbPersistedScript() {
        return markDbPersistedScript;
    }

    public RedisScript<List> markPersistFailedScript() {
        return markPersistFailedScript;
    }

    public RedisScript<List> releaseBalanceReservationScript() {
        return releaseBalanceReservationScript;
    }

    private static RedisScript<List> load(String location) {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(location));
        script.setResultType(List.class);
        return script;
    }
}
