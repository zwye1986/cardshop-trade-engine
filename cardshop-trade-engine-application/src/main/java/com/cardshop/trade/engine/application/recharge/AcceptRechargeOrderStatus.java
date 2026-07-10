package com.cardshop.trade.engine.application.recharge;

/**
 * Redis Lua 下单受理结果码。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * 这里保留 Lua 返回码的领域语义，接口层后续再映射为旧 V1 协议的 code/msg/data。
 */
public enum AcceptRechargeOrderStatus {

    /**
     * 业务校验通过，Redis 已写订单快照、幂等、余额预扣、pending 和初始事件。
     */
    ACCEPTED(true),

    /**
     * 业务校验失败，但 Redis 已写失败订单快照、幂等、pending 和初始事件，不允许派单。
     */
    ACCEPTED_FAILED(true),

    /**
     * 同一商户订单号已存在；按协议返回明确重复，不新增订单、不检查 nonce。
     */
    DUPLICATE(false),

    /**
     * 同商户不同业务请求复用了 nonce，按重放风险拒绝，不新增订单。
     */
    NONCE_REPLAY(false),

    /**
     * Java 候选配置版本和 Redis 当前配置版本不一致，上层应刷新配置后最多内部重试一次。
     */
    CONFIG_CHANGED(false),

    /**
     * 余额修复锁存在，拒绝新下单，避免修复任务覆盖实时预扣。
     */
    BALANCE_REPAIRING(false),

    /**
     * Redis 写入开关关闭，下单必须 fail closed。
     */
    REDIS_WRITE_DISABLED(false),

    /**
     * Redis 客户端或脚本执行不可用，不能降级到旧系统重试。
     */
    REDIS_UNAVAILABLE(false),

    /**
     * Lua 返回了当前版本未知的结果码，进入保守失败处理。
     */
    UNKNOWN(false);

    private final boolean orderWritten;

    AcceptRechargeOrderStatus(boolean orderWritten) {
        this.orderWritten = orderWritten;
    }

    /**
     * 判断 Redis 是否已经写入了订单快照；D5/D6 后续 WAIT、MQ 和异常核查会使用该语义决定是否允许重试。
     */
    public boolean isOrderWritten() {
        return orderWritten;
    }
}
