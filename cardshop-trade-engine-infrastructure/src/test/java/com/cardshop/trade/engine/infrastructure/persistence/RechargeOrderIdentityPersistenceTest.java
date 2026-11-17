package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.PersistSource;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** 主单唯一键无操作不能证明消息身份；参见详细设计8.2.1。 */
class RechargeOrderIdentityPersistenceTest {
    @Test
    void duplicateKeyWithoutMatchingPlatformFactMustNotCreateEvents() {
        var orders = mock(McOrderMapper.class);
        var ext = mock(RechargeOrderExtMapper.class);
        var dispatch = mock(RechargeDispatchOutboxMapper.class);
        var delta = mock(RechargeBalanceDeltaOutboxMapper.class);
        var records = mock(McDepartRecordMapper.class);
        var adapter = new MybatisRechargeOrderPersistenceAdapter(
                orders, ext, dispatch, delta, records, new ObjectMapper());

        assertThatThrownBy(() -> adapter.persist(aggregate("LOSER", "OUT", 137)))
                .isInstanceOf(IllegalStateException.class);
        verify(ext, never()).insertOrUpdatePersistState(any());
        verifyNoInteractions(dispatch, delta, records);
    }

    static OrderPersistAggregate aggregate(String orderId, String outOrderSn, long amount) {
        return new OrderPersistAggregate("persist-" + orderId,
                new PersistOrderSnapshot("A35", "app", orderId, outOrderSn, "PARENT", "SKU", "10000000000",
                        "https://example.invalid/notify", 1, amount, "合成商品", 1000L, 1, "codekey", "BOCOM",
                        "hash-" + orderId, "v1", "SHA-256", "ACCEPTED", "PASSED", "", "",
                        true, true, "PERSIST_QUEUED", 1L, 0L, 1),
                PersistSource.MQ_SNAPSHOT_FALLBACK, Instant.parse("2026-11-16T01:00:00Z"));
    }
}
