package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Redis 查单适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。Redis hash 字段是下单 Lua、
 * 查单接口和后续 Consumer 的共享合同，适配器必须稳定映射为 application 层快照。
 */
class RedisRechargeOrderQueryAdapterTest {

    @Test
    void mapsRedisOrderHashToSnapshot() {
        var adapter = new RedisRechargeOrderQueryAdapter(
                key -> Map.of(
                        "order_id", "202607051337458961474292",
                        "out_order_sn", "OUT-1001",
                        "order_status", "ACCEPTED",
                        "persist_status", "PERSIST_QUEUED",
                        "validation_status", "PASSED",
                        "fail_code", "",
                        "fail_reason", "",
                        "dispatch_allowed", "1"),
                new RechargeRedisKeyBuilder("cardshop:trade", 128));

        var snapshot = adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001");

        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().orderId()).isEqualTo("202607051337458961474292");
        assertThat(snapshot.get().persistStatus()).isEqualTo("PERSIST_QUEUED");
        assertThat(snapshot.get().dispatchAllowed()).isTrue();
    }

    @Test
    void returnsEmptyWhenRedisHashDoesNotExist() {
        var adapter = new RedisRechargeOrderQueryAdapter(
                key -> Map.of(),
                new RechargeRedisKeyBuilder("cardshop:trade", 128));

        assertThat(adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-MISSING")).isEmpty();
    }
}
