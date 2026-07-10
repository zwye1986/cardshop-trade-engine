package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeAcceptAnomalyDO;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

/**
 * Redis 受理状态未知异常 Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「6. Redis 受理与缓存层」。
 */
@Mapper
public interface RechargeAcceptAnomalyMapper extends BaseMapper<RechargeAcceptAnomalyDO> {

    /**
     * 按 `anomaly_id` 幂等登记受理异常。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「14.3 MySQL 表字段」和
     * 「6. Redis 受理与缓存层」。WAIT/cancel 异常属于旁路证据留存，上游重试可能让同一订单同一异常
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

    /**
     * 读取一批仍需自动核查的异常记录。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」。当前 D5B 表结构尚未包含
     * `locked_until`，因此这里做固定上限保护，避免异常积压时单次任务扫出过大结果集；后续补租约字段时可升级为
     * CAS claim。
     */
    default List<RechargeAcceptAnomalyDO> selectOpenForCheck(int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 500));
        return selectList(new LambdaQueryWrapper<RechargeAcceptAnomalyDO>()
                .in(
                        RechargeAcceptAnomalyDO::getStatus,
                        AcceptAnomalyStatus.INIT.name(),
                        AcceptAnomalyStatus.CHECKING.name(),
                        AcceptAnomalyStatus.MANUAL_REQUIRED.name())
                .orderByAsc(RechargeAcceptAnomalyDO::getUpdatedAt)
                .last("limit " + safeLimit));
    }

    /**
     * 标记异常进入自动核查中。
     */
    default int markChecking(String anomalyId, LocalDateTime now) {
        return update(null, new LambdaUpdateWrapper<RechargeAcceptAnomalyDO>()
                .set(RechargeAcceptAnomalyDO::getStatus, AcceptAnomalyStatus.CHECKING.name())
                .set(RechargeAcceptAnomalyDO::getUpdatedAt, now)
                .eq(RechargeAcceptAnomalyDO::getAnomalyId, anomalyId)
                .in(
                        RechargeAcceptAnomalyDO::getStatus,
                        AcceptAnomalyStatus.INIT.name(),
                        AcceptAnomalyStatus.CHECKING.name(),
                        AcceptAnomalyStatus.MANUAL_REQUIRED.name()));
    }

    /**
     * 写入自动核查三方证据并推进最终状态。
     *
     * <p>自动核查可能由多个节点或多次任务并发触发。参见
     * {@code docs/交行直充业务系统详细设计.md}「6.3 Redis HA Guard 与状态未知窗口」：D5B 尚未引入 `locked_until`
     * 租约字段，因此这里至少用状态条件保护终态记录，避免慢核查任务覆盖已经人工或自动 resolved 的结果。
     */
    default int updateCheckResult(RechargeAcceptAnomalyDO row) {
        return update(null, new LambdaUpdateWrapper<RechargeAcceptAnomalyDO>()
                .set(RechargeAcceptAnomalyDO::getStatus, row.getStatus())
                .set(RechargeAcceptAnomalyDO::getRedisCheckResult, row.getRedisCheckResult())
                .set(RechargeAcceptAnomalyDO::getMysqlCheckResult, row.getMysqlCheckResult())
                .set(RechargeAcceptAnomalyDO::getMqCheckResult, row.getMqCheckResult())
                .set(RechargeAcceptAnomalyDO::getResolution, row.getResolution())
                .set(RechargeAcceptAnomalyDO::getResolvedBy, row.getResolvedBy())
                .set(RechargeAcceptAnomalyDO::getResolvedAt, row.getResolvedAt())
                .set(RechargeAcceptAnomalyDO::getLastError, row.getLastError())
                .set(RechargeAcceptAnomalyDO::getUpdatedAt, row.getUpdatedAt())
                .eq(RechargeAcceptAnomalyDO::getAnomalyId, row.getAnomalyId())
                .in(
                        RechargeAcceptAnomalyDO::getStatus,
                        AcceptAnomalyStatus.INIT.name(),
                        AcceptAnomalyStatus.CHECKING.name(),
                        AcceptAnomalyStatus.MANUAL_REQUIRED.name()));
    }
}
