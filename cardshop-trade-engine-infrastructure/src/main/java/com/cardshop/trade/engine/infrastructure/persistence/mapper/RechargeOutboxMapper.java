package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import java.time.LocalDateTime;
import java.util.List;
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
