package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeAcceptAnomalyDO;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

/**
 * Redis 受理状态未知异常 Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.13.6 Redis Lua 下单结果」。
 */
@Mapper
public interface RechargeAcceptAnomalyMapper extends BaseMapper<RechargeAcceptAnomalyDO> {

    /**
     * 按 `anomaly_id` 幂等登记受理异常。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.8 接单异常核查表」和
     * 「20.13.6 Redis Lua 下单结果」。WAIT/cancel 异常属于旁路证据留存，上游重试可能让同一订单同一异常
     * 被登记多次；这里刷新最近错误和更新时间，但保留首次写入的 raw_context/status，避免已进入核查或人工处理的记录
     * 被重复登记覆盖。
     */
    @Insert("""
            insert into recharge_accept_anomaly (
              anomaly_id,
              org_code,
              out_order_sn,
              order_id,
              request_hash,
              persist_msg_id,
              anomaly_type,
              status,
              raw_context,
              last_error,
              created_at,
              updated_at
            )
            values (
              #{anomalyId},
              #{orgCode},
              #{outOrderSn},
              #{orderId},
              #{requestHash},
              #{persistMsgId},
              #{anomalyType},
              #{status},
              #{rawContext},
              #{lastError},
              #{createdAt},
              #{updatedAt}
            )
            on duplicate key update
              last_error = values(last_error),
              updated_at = values(updated_at)
            """)
    int insertOrRefreshByAnomalyId(RechargeAcceptAnomalyDO row);

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
