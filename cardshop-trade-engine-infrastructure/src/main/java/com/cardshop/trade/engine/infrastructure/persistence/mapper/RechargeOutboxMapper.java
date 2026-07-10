package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * 可靠事件表 Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md} 的「6.6 新增表」。
 * 扫描和抢占均使用单表 LambdaWrapper，分片条件使用 MySQL 函数表达式。
 */
@Mapper
public interface RechargeOutboxMapper extends BaseMapper<RechargeOutboxDO> {

    /**
     * 幂等写入可靠事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。
     * Persist Consumer 可能因为 RabbitMQ 重投重复处理同一订单，`SUPPLIER_DISPATCH` outbox 必须按
     * `event_id` 幂等插入，避免唯一键冲突被误判为数据库故障。
     */
    @Insert("""
            insert into recharge_outbox (
              event_id,
              event_type,
              org_code,
              order_id,
              out_order_sn,
              payload,
              status,
              retry_count,
              max_retry_count,
              next_retry_time,
              locked_by,
              locked_until,
              version,
              last_error,
              shard_key,
              created_at,
              updated_at
            )
            values (
              #{eventId},
              #{eventType},
              #{orgCode},
              #{orderId},
              #{outOrderSn},
              #{payload},
              #{status},
              #{retryCount},
              #{maxRetryCount},
              #{nextRetryTime},
              #{lockedBy},
              #{lockedUntil},
              #{version},
              #{lastError},
              #{shardKey},
              #{createdAt},
              #{updatedAt}
            )
            on duplicate key update
              event_id = event_id
            """)
    int insertIfAbsent(RechargeOutboxDO outbox);

    /**
     * 汇总未完成的余额 delta。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.5 余额对账任务」。对账时不能只比较
     * `mc_ext_depart.balance` 和 Redis available；还必须把尚未刷入旧表的 `BALANCE_DELTA_FLUSH` 事件纳入
     * 投影，否则正在重试的刷账事件会被误报为资金差异。
     */
    @Select("""
            select coalesce(sum(cast(json_unquote(json_extract(payload, '$.delta_amount')) as signed)), 0)
            from recharge_outbox
            where org_code = #{orgCode}
              and event_type = 'BALANCE_DELTA_FLUSH'
              and status <> 'SUCCESS'
            """)
    Long sumUnfinishedBalanceDelta(@Param("orgCode") String orgCode);

    /**
     * 扫描到期事件和租约失效事件。
     */
    default List<RechargeOutboxDO> selectDueForShard(int shardTotal, int shardIndex, int limit) {
        return selectDueForShardByEventTypes(
                shardTotal,
                shardIndex,
                limit,
                List.of(OutboxEventType.SUPPLIER_DISPATCH.name(), OutboxEventType.INVALID_ORDER.name()));
    }

    /**
     * 按事件类型扫描到期事件和租约失效事件。
     *
     * <p>D9/D10 的履约 worker 与 D12 的余额刷账 worker 共享 outbox 表，但处理语义不同；扫描时必须
     * 先按 event_type 隔离，避免通用供应商 worker 把 `BALANCE_DELTA_FLUSH` 误标为不支持事件。
     */
    default List<RechargeOutboxDO> selectDueForShardByEventTypes(
            int shardTotal,
            int shardIndex,
            int limit,
            List<String> eventTypes) {
        return selectDueForShardByEventTypes(shardTotal, shardIndex, limit, eventTypes, LocalDateTime.now());
    }

    /**
     * 使用调用方注入的时钟扫描到期事件，便于固定 Clock 的测试和生产排障统一时间来源。
     */
    default List<RechargeOutboxDO> selectDueForShardByEventTypes(
            int shardTotal,
            int shardIndex,
            int limit,
            List<String> eventTypes,
            LocalDateTime now) {
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        return selectList(new LambdaQueryWrapper<RechargeOutboxDO>()
                .in(RechargeOutboxDO::getEventType, eventTypes)
                .and(wrapper -> wrapper
                        .and(due -> due
                                .in(RechargeOutboxDO::getStatus, TaskStatus.INIT.name(), TaskStatus.FAILED.name())
                                .le(RechargeOutboxDO::getNextRetryTime, now))
                        .or(zombie -> zombie
                                .eq(RechargeOutboxDO::getStatus, TaskStatus.PROCESSING.name())
                                .and(lock -> lock
                                        .lt(RechargeOutboxDO::getLockedUntil, now)
                                        .or()
                                        .isNull(RechargeOutboxDO::getLockedUntil))))
                .apply("mod(crc32(shard_key), {0}) = {1}", shardTotal, shardIndex)
                .orderByAsc(RechargeOutboxDO::getNextRetryTime)
                .orderByAsc(RechargeOutboxDO::getId)
                .last("limit " + safeLimit));
    }

    /**
     * 基于事件 ID 和版本号抢占事件。
     */
    default int claimProcessing(String eventId, long currentVersion, String workerId, LocalDateTime lockedUntil) {
        return claimProcessing(eventId, currentVersion, workerId, lockedUntil, LocalDateTime.now());
    }

    /**
     * 使用调用方注入的时钟执行僵尸任务回收判断。
     */
    default int claimProcessing(
            String eventId,
            long currentVersion,
            String workerId,
            LocalDateTime lockedUntil,
            LocalDateTime now) {
        return update(null, new LambdaUpdateWrapper<RechargeOutboxDO>()
                .eq(RechargeOutboxDO::getEventId, eventId)
                .eq(RechargeOutboxDO::getVersion, currentVersion)
                .and(wrapper -> wrapper
                        .in(RechargeOutboxDO::getStatus, TaskStatus.INIT.name(), TaskStatus.FAILED.name())
                        .apply("retry_count < max_retry_count")
                        .or(zombie -> zombie
                                .eq(RechargeOutboxDO::getStatus, TaskStatus.PROCESSING.name())
                                .and(lock -> lock
                                        .lt(RechargeOutboxDO::getLockedUntil, now)
                                        .or()
                                        .isNull(RechargeOutboxDO::getLockedUntil))))
                .set(RechargeOutboxDO::getStatus, TaskStatus.PROCESSING.name())
                .set(RechargeOutboxDO::getLockedBy, workerId)
                .set(RechargeOutboxDO::getLockedUntil, lockedUntil)
                .setSql("version = version + 1"));
    }
}
