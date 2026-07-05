package com.cardshop.trade.engine.application.recharge;

import java.util.concurrent.CompletionStage;

/**
 * Redis-first 下单受理端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
 * application 层只依赖该端口表达“把订单原子受理到 Redis”的能力，不感知 Spring Data Redis、
 * Lua 脚本或 Redis 拓扑细节，保证 Redis 适配器可独立测试和替换。
 */
public interface AcceptRechargeOrderPort {

    /**
     * 调用 Redis Lua 原子受理直充订单。
     *
     * @param command 已完成协议安全校验和 Java 候选校验的下单命令
     * @return 异步受理结果；Redis 不可用、脚本超时和状态未知会映射为失败结果或异常，由上层 D5/D6 门禁继续处理
     */
    CompletionStage<AcceptRechargeOrderResult> accept(AcceptRechargeOrderCommand command);
}
