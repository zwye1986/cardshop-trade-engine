package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cardshop.trade.engine.application.recharge.PersistedOrderProbe;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
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
        MybatisPersistedOrderStateAdapter adapter = new MybatisPersistedOrderStateAdapter(mapper);

        Set<PersistedOrderProbe> persisted = adapter.findPersistedAtLeast(List.of(
                new PersistedOrderProbe("A36", "OUT-1", "ORDER-1", 2L),
                new PersistedOrderProbe("A36", "OUT-2", "ORDER-2", 5L),
                new PersistedOrderProbe("A36", "OUT-3", "ORDER-3", 5L),
                new PersistedOrderProbe("A36", "OUT-4", "ORDER-4", 2L)));

        assertThat(persisted).containsExactly(new PersistedOrderProbe("A36", "OUT-1", "ORDER-1", 2L));
        verify(mapper).selectByOrderIds(List.of("ORDER-1", "ORDER-2", "ORDER-3", "ORDER-4"));
    }

    @Test
    void batchLookupReturnsEmptyWhenMapperFails() {
        RechargeOrderExtMapper mapper = mock(RechargeOrderExtMapper.class);
        when(mapper.selectByOrderIds(List.of("ORDER-1"))).thenThrow(new IllegalStateException("db unavailable"));
        MybatisPersistedOrderStateAdapter adapter = new MybatisPersistedOrderStateAdapter(mapper);

        Set<PersistedOrderProbe> persisted = adapter.findPersistedAtLeast(List.of(
                new PersistedOrderProbe("A36", "OUT-1", "ORDER-1", 1L)));

        assertThat(persisted).isEmpty();
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
        return ext;
    }
}
