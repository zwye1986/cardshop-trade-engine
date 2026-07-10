package com.cardshop.trade.engine.application.recharge;

import java.util.Map;

/**
 * Redis HA 写入门禁端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」和
 * 「4. Application 用例编排层」。下单入口只关心“当前是否允许继续写 Redis”和“遇到状态未知窗口时关闭写入”，
 * 不直接依赖 Sentinel、Lettuce 事件或 Nacos 动态配置等基础设施细节。
 */
public interface RedisHaGuardPort {

    /**
     * 判断当前是否允许执行 Redis 写入型 Lua。
     *
     * <p>返回 {@code false} 时，`/createRechargeOrder` 必须在调用 `accept.lua` 前 fail closed，避免在
     * Redis 主从切换、复制异常或人工修复窗口继续制造余额预扣和 pending 订单。
     */
    boolean isWriteAllowed();

    /**
     * 关闭 Redis 写入。
     *
     * <p>调用方必须传入不含密钥、token、手机号等敏感信息的定位上下文。上下文主要用于日志和后续人工核查，
     * 具体持久化方式由基础设施层决定。
     *
     * @param reason 关闭原因，例如 WAIT_FAILED_CANCEL_FAILED
     * @param context 非敏感定位上下文，例如 org_code/out_order_sn/order_id/request_hash
     */
    void closeWrite(String reason, Map<String, Object> context);

    /**
     * local profile 或尚未装配 HA Guard 时的空实现；默认允许写入，保持既有单元测试行为。
     */
    static RedisHaGuardPort noop() {
        return new RedisHaGuardPort() {
            @Override
            public boolean isWriteAllowed() {
                return true;
            }

            @Override
            public void closeWrite(String reason, Map<String, Object> context) {
                // 空实现只用于未装配 HA Guard 的本地测试场景。
            }
        };
    }
}
