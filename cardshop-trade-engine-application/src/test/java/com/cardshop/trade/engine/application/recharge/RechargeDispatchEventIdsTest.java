package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * 供应商履约 outbox 事件号契约测试。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。落库写入 outbox 和落库后 timer
 * 热唤醒必须共享同一个 event_id 生成规则，避免一侧修改前缀后另一侧静默唤醒失败。
 */
class RechargeDispatchEventIdsTest {

    @Test
    void buildsDispatchEventIdFromSharedContract() {
        assertThat(RechargeDispatchEventIds.dispatchEventId("ORDER-1"))
                .isEqualTo("dispatch-ORDER-1");
        assertThat(RechargeDispatchEventIds.supplierQueryEventId("ORDER-1"))
                .isEqualTo("supplier-query-ORDER-1");
    }

    @Test
    void rejectsBlankOrderId() {
        assertThatThrownBy(() -> RechargeDispatchEventIds.dispatchEventId(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("orderId");
    }
}
