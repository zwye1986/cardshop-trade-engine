package com.cardshop.trade.engine.application.v1;

/**
 * 可信持久事实的直充外单占用检查，不承担展示快照或 Redis 受理期查询。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」「8. MySQL/Liquibase 持久化层」。
 * 任一旧/新直充主单或扩展身份存在即为占用；数据库故障必须抛出，不能返回不存在。
 */
@FunctionalInterface
public interface RechargeOrderExistencePort {

    /** 验签和语法检查后、选品及任何失败事实/Redis 接单前执行；此检查不提供跨系统原子认领。 */
    boolean existsPersistedOrder(String orgCode, String outOrderSn);
}
