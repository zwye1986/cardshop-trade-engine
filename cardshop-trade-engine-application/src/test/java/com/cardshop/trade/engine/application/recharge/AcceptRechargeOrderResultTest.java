package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Redis 接单结果失败码契约测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。Java 侧主动构造的失败结果
 * 必须集中使用工厂方法，避免同一失败语义在不同适配器里散落字符串字面量。
 */
class AcceptRechargeOrderResultTest {

    @Test
    void redisWriteDisabledUsesStatusNameAsFailCode() {
        AcceptRechargeOrderResult result = AcceptRechargeOrderResult.redisWriteDisabled("ORDER-1001");

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.REDIS_WRITE_DISABLED);
        assertThat(result.failCode()).isEqualTo(AcceptRechargeOrderResult.FAIL_CODE_REDIS_WRITE_DISABLED);
        assertThat(result.orderWritten()).isFalse();
    }

    @Test
    void redisUnavailableUsesStatusNameAsFailCode() {
        AcceptRechargeOrderResult result =
                AcceptRechargeOrderResult.redisUnavailable("ORDER-1001", "Redis 会话不可用");

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.REDIS_UNAVAILABLE);
        assertThat(result.failCode()).isEqualTo(AcceptRechargeOrderResult.FAIL_CODE_REDIS_UNAVAILABLE);
        assertThat(result.failReason()).isEqualTo("Redis 会话不可用");
        assertThat(result.orderWritten()).isFalse();
    }
}
