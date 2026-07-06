package com.cardshop.trade.engine.infrastructure.redis;

/**
 * Redis 接单会话工厂。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9. Redis 设计落地」。每次 Redis-first
 * 下单都通过该工厂取得一个可执行 Lua 和 WAIT 的会话，避免上层直接依赖 Lettuce 连接对象。
 */
public interface RechargeRedisSessionFactory {

    /**
     * 打开一个 Redis 会话。调用方必须在 WAIT、取消或状态推进结束后关闭会话。
     */
    RechargeRedisSession openSession();
}
