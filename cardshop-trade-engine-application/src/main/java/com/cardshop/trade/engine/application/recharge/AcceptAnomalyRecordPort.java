package com.cardshop.trade.engine.application.recharge;

/**
 * Redis 接单状态未知异常登记端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。下单用例只知道
 * “需要登记异常”，不关心异常最终写入 MySQL、日志系统还是后续人工队列；这样 application/API 层不会反向依赖
 * MyBatis 或具体表结构。实现方应保证失败可观测，调用方会把登记失败视为旁路失败，不改变接口返回。
 */
public interface AcceptAnomalyRecordPort {

    /**
     * 登记一条接单状态未知异常。
     *
     * <p>实现可以使用稳定 `anomalyId` 做幂等；如果发生重复登记，建议保留原始记录或刷新错误原因，不能抛出
     * 影响下单接口主流程的异常。
     */
    void record(AcceptAnomalyRecord record);

    /**
     * 未装配 MySQL 异常表时使用的空实现，保证 local profile 或单元测试仍可构造用例。
     */
    static AcceptAnomalyRecordPort noop() {
        return record -> {
        };
    }
}
