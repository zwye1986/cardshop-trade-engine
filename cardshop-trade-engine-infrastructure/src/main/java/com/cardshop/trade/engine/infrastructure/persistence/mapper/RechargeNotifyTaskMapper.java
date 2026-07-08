package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeNotifyTaskDO;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

/**
 * 商户回调通知任务 Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.13.12 回调设计」。
 */
@Mapper
public interface RechargeNotifyTaskMapper extends BaseMapper<RechargeNotifyTaskDO> {

    /**
     * 幂等写入回调任务。
     */
    @Insert("""
            insert into recharge_notify_task (
              task_id,
              notify_type,
              org_code,
              order_id,
              out_order_sn,
              notify_url,
              biz_status,
              payload,
              status,
              retry_count,
              max_retry_count,
              next_retry_time,
              locked_by,
              locked_until,
              last_error,
              created_at,
              updated_at
            )
            values (
              #{taskId},
              #{notifyType},
              #{orgCode},
              #{orderId},
              #{outOrderSn},
              #{notifyUrl},
              #{bizStatus},
              #{payload},
              #{status},
              #{retryCount},
              #{maxRetryCount},
              #{nextRetryTime},
              #{lockedBy},
              #{lockedUntil},
              #{lastError},
              #{createdAt},
              #{updatedAt}
            )
            on duplicate key update
              task_id = task_id
            """)
    int insertIfAbsent(RechargeNotifyTaskDO task);

    /**
     * 单表扫描到期通知任务。
     */
    default List<RechargeNotifyTaskDO> selectDueTasks(int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        LocalDateTime now = LocalDateTime.now();
        return selectList(new LambdaQueryWrapper<RechargeNotifyTaskDO>()
                .and(wrapper -> wrapper
                        .and(due -> due
                                .in(RechargeNotifyTaskDO::getStatus, TaskStatus.INIT.name(), TaskStatus.FAILED.name())
                                .le(RechargeNotifyTaskDO::getNextRetryTime, now))
                        .or(zombie -> zombie
                                .eq(RechargeNotifyTaskDO::getStatus, TaskStatus.PROCESSING.name())
                                .and(lock -> lock
                                        .lt(RechargeNotifyTaskDO::getLockedUntil, now)
                                        .or()
                                        .isNull(RechargeNotifyTaskDO::getLockedUntil))))
                .orderByAsc(RechargeNotifyTaskDO::getNextRetryTime)
                .orderByAsc(RechargeNotifyTaskDO::getId)
                .last("limit " + safeLimit));
    }
}
