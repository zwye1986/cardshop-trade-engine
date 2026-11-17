package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cardshop.trade.engine.application.recharge.PersistedOrderProbe;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * MySQL 已落库状态确认 adapter 测试。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。批消费会用该 adapter
 * 批量识别 RabbitMQ 主队列里的已落库重复消息，只有 MySQL 已覆盖本次选中快照版本时才能跳过批写。
 */
class MybatisPersistedOrderStateAdapterTest {

    @Test
    void batchLookupReturnsOnlyRowsMatchingBusinessKeyAndPersistedVersion() {
        RechargeOrderExtMapper mapper = mock(RechargeOrderExtMapper.class);
        when(mapper.selectByOrderIds(List.of("ORDER-1", "ORDER-2", "ORDER-3", "ORDER-4")))
                .thenReturn(List.of(
                        ext("ORDER-1", "A36", "OUT-1", "DB_PERSISTED", 2L),
                        ext("ORDER-2", "A36", "OUT-2", "PERSIST_QUEUED", 5L),
                        ext("ORDER-3", "A36", "OUT-3-OTHER", "DB_PERSISTED", 5L),
                        ext("ORDER-4", "A36", "OUT-4", "DB_PERSISTED", 1L)));
        var orders = mock(McOrderMapper.class);
        when(orders.selectPersistIdentities(List.of("ORDER-1", "ORDER-2", "ORDER-3", "ORDER-4")))
                .thenReturn(java.util.stream.IntStream.rangeClosed(1, 4).mapToObj(i -> order(i)).toList());
        MybatisPersistedOrderStateAdapter adapter = new MybatisPersistedOrderStateAdapter(orders, mapper);

        Set<PersistedOrderProbe> persisted = adapter.findPersistedAtLeast(List.of(
                probe(1, 2L), probe(2, 5L), probe(3, 5L), probe(4, 2L)));

        assertThat(persisted).containsExactly(probe(1, 2L));
        verify(mapper).selectByOrderIds(List.of("ORDER-1", "ORDER-2", "ORDER-3", "ORDER-4"));
    }

    @Test
    void batchLookupReturnsEmptyWhenMapperFails() {
        RechargeOrderExtMapper mapper = mock(RechargeOrderExtMapper.class);
        when(mapper.selectByOrderIds(List.of("ORDER-1"))).thenThrow(new IllegalStateException("db unavailable"));
        MybatisPersistedOrderStateAdapter adapter = new MybatisPersistedOrderStateAdapter(mock(McOrderMapper.class), mapper);

        Set<PersistedOrderProbe> persisted = adapter.findPersistedAtLeast(List.of(
                new PersistedOrderProbe("A36", "OUT-1", "ORDER-1", 1L)));

        assertThat(persisted).isEmpty();
    }

    @Test
    void batchSkipRejectsAmountHashMetadataAndOwnershipMismatch() {
        var orders = mock(McOrderMapper.class);
        var extensions = mock(RechargeOrderExtMapper.class);
        var order = order(1);
        var ext = ext("ORDER-1", "A36", "OUT-1", "DB_PERSISTED", 2);
        when(orders.selectPersistIdentities(List.of("ORDER-1"))).thenReturn(List.of(order));
        when(extensions.selectByOrderIds(List.of("ORDER-1"))).thenReturn(List.of(ext));
        var adapter = new MybatisPersistedOrderStateAdapter(orders, extensions);
        assertThat(adapter.findPersistedAtLeast(List.of(probe(1, 2)))).containsExactly(probe(1, 2));
        order.setOrderPrice(138L);
        assertThat(adapter.findPersistedAtLeast(List.of(probe(1, 2)))).isEmpty();
        order.setOrderPrice(137L);
        for (String different : List.of("hash", "version", "algorithm", "ownership")) {
            ext.setRequestHash(different.equals("hash") ? "other" : "hash");
            ext.setRequestHashVersion(different.equals("version") ? "v2" : "v1");
            ext.setRequestHashAlg(different.equals("algorithm") ? "SM3" : "SHA-256");
            order.setTradeEngineOwned(different.equals("ownership") ? 0 : 1);
            assertThat(adapter.findPersistedAtLeast(List.of(probe(1, 2)))).isEmpty();
        }
    }

    private static PersistedOrderProbe probe(int number, long version) {
        return new PersistedOrderProbe("A36", "OUT-" + number, "ORDER-" + number, version,
                137L, "hash", "v1", "SHA-256");
    }

    private static McOrderDO order(int number) {
        var order = new McOrderDO();
        order.setOrderId("ORDER-" + number);
        order.setOrgCode("A36");
        order.setOutOrderId("OUT-" + number);
        order.setOrderPrice(137L);
        order.setProductType(1);
        order.setTradeEngineOwned(1);
        return order;
    }

    private static RechargeOrderExtDO ext(
            String orderId,
            String orgCode,
            String outOrderSn,
            String persistStatus,
            long persistedVersion) {
        RechargeOrderExtDO ext = new RechargeOrderExtDO();
        ext.setOrderId(orderId);
        ext.setOrgCode(orgCode);
        ext.setOutOrderSn(outOrderSn);
        ext.setPersistStatus(persistStatus);
        ext.setPersistedVersion(persistedVersion);
        ext.setRequestHash("hash");
        ext.setRequestHashVersion("v1");
        ext.setRequestHashAlg("SHA-256");
        return ext;
    }
}
