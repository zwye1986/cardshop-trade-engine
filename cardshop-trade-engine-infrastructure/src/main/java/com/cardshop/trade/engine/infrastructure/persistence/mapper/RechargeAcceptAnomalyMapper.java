package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeAcceptAnomalyDO;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;

/**
 * Redis 受理状态未知异常 Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.13.6 Redis Lua 下单结果」。
 */
@Mapper
public interface RechargeAcceptAnomalyMapper extends BaseMapper<RechargeAcceptAnomalyDO> {

    /**
     * 按商户订单号读取仍需自动或人工核查的异常记录。
     */
    default List<RechargeAcceptAnomalyDO> selectOpenByOrgAndOutOrder(String orgCode, String outOrderSn) {
        return selectList(new LambdaQueryWrapper<RechargeAcceptAnomalyDO>()
                .eq(RechargeAcceptAnomalyDO::getOrgCode, orgCode)
                .eq(RechargeAcceptAnomalyDO::getOutOrderSn, outOrderSn)
                .in(
                        RechargeAcceptAnomalyDO::getStatus,
                        AcceptAnomalyStatus.INIT.name(),
                        AcceptAnomalyStatus.CHECKING.name(),
                        AcceptAnomalyStatus.MANUAL_REQUIRED.name())
                .orderByDesc(RechargeAcceptAnomalyDO::getUpdatedAt));
    }
}
