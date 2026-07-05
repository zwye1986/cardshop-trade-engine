package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Lua 返回值映射测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
 * Lua 返回扁平 key-value 数组，Java 侧必须稳定映射成应用层结果，后续接口响应、WAIT 和 MQ confirm
 * 都会基于这个结果判断是否已经写入订单。
 */
class AcceptRechargeOrderLuaResultMapperTest {

    private final AcceptRechargeOrderLuaResultMapper mapper = new AcceptRechargeOrderLuaResultMapper();

    @Test
    void mapsAcceptedLuaArrayToApplicationResult() {
        var result = mapper.map(List.of(
                "code", "ACCEPTED",
                "order_id", "202607051337458961474292",
                "validation_status", "PASSED",
                "fail_code", "",
                "fail_reason", "",
                "dispatch_allowed", "1",
                "balance_reserved", "1",
                "order_version", "1"));

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.ACCEPTED);
        assertThat(result.orderId()).isEqualTo("202607051337458961474292");
        assertThat(result.dispatchAllowed()).isTrue();
        assertThat(result.balanceReserved()).isTrue();
        assertThat(result.orderWritten()).isTrue();
        assertThat(result.orderVersion()).isEqualTo(1);
    }

    @Test
    void mapsUnknownStructureToConservativeUnknownResult() {
        var result = mapper.map("unexpected");

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.UNKNOWN);
        assertThat(result.orderWritten()).isFalse();
        assertThat(result.failReason()).contains("Lua 返回结构不是数组");
    }
}
