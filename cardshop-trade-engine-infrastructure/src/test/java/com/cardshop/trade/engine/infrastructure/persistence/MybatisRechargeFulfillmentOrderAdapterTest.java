package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.cardshop.trade.engine.application.recharge.FulfillmentDispatchStatus;
import com.cardshop.trade.engine.application.recharge.OrderFulfillmentStatusUpdate;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * MyBatis 供应商履约订单适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。终态回写需要保持旧表
 * `mc_order` 的排障字段一致，便于按订单快速定位最后一次供应商流水。
 */
class MybatisRechargeFulfillmentOrderAdapterTest {

    @BeforeAll
    static void initMybatisPlusTableInfo() {
        MybatisConfiguration configuration = new MybatisConfiguration();
        TableInfoHelper.initTableInfo(new MapperBuilderAssistant(configuration, ""), McOrderDO.class);
        TableInfoHelper.initTableInfo(new MapperBuilderAssistant(configuration, ""), RechargeOrderExtDO.class);
    }

    @Test
    void updateFulfillmentStatusPersistsSupplierFlowIdWhenPresent() {
        McOrderMapper orderMapper = mock(McOrderMapper.class);
        MybatisRechargeFulfillmentOrderAdapter adapter = adapter(orderMapper);

        adapter.updateFulfillmentStatus(new OrderFulfillmentStatusUpdate(
                "ORDER-1001",
                FulfillmentDispatchStatus.SUCCESS,
                12,
                "PROCESSED",
                99L,
                null,
                null,
                "供应商充值成功"));

        ArgumentCaptor<LambdaUpdateWrapper<McOrderDO>> wrapperCaptor =
                ArgumentCaptor.forClass(LambdaUpdateWrapper.class);
        verify(orderMapper).update(isNull(), wrapperCaptor.capture());
        assertThat(wrapperCaptor.getValue().getSqlSet()).contains("flow_id");
    }

    private static MybatisRechargeFulfillmentOrderAdapter adapter(McOrderMapper orderMapper) {
        return new MybatisRechargeFulfillmentOrderAdapter(
                orderMapper,
                mock(RechargeOrderExtMapper.class),
                mock(McOrderSupplierFlowMapper.class),
                new ObjectMapper(),
                Clock.fixed(Instant.parse("2026-07-07T01:30:00Z"), ZoneOffset.UTC));
    }
}
