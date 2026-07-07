package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

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
     * 扫描到期事件和租约失效事件。
     */
    default List<RechargeOutboxDO> selectDueForShard(int shardTotal, int shardIndex, int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        LocalDateTime now = LocalDateTime.now();
        return selectList(new LambdaQueryWrapper<RechargeOutboxDO>()
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
        LocalDateTime now = LocalDateTime.now();
        return update(null, new LambdaUpdateWrapper<RechargeOutboxDO>()
                .eq(RechargeOutboxDO::getEventId, eventId)
                .eq(RechargeOutboxDO::getVersion, currentVersion)
                .and(wrapper -> wrapper
                        .in(RechargeOutboxDO::getStatus, TaskStatus.INIT.name(), TaskStatus.FAILED.name())
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
