package com.cardshop.trade.engine.infrastructure.persistence;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 既有映射单测的内存事实存储；数据库事务/唯一竞争另由真实MySQL回归验证。 */
final class RecordingOrderFacts {
    final Map<String, McOrderDO> orders = new LinkedHashMap<>();
    final Map<String, RechargeOrderExtDO> extensions = new LinkedHashMap<>();

    RechargeOrderIdentityVerifier verifier() {
        var orderMapper = mock(McOrderMapper.class);
        var extMapper = mock(RechargeOrderExtMapper.class);
        when(orderMapper.selectPersistIdentities(anyList())).thenAnswer(call -> {
            List<String> ids = call.getArgument(0);
            return orders.values().stream().filter(row -> ids.contains(row.getOrderId())).toList();
        });
        when(extMapper.selectByOrderIds(anyList())).thenAnswer(call -> {
            List<String> ids = call.getArgument(0);
            return extensions.values().stream().filter(row -> ids.contains(row.getOrderId())).toList();
        });
        return new RechargeOrderIdentityVerifier(orderMapper, extMapper);
    }
}
