package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeNotifyTaskDO;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;

/**
 * 商户回调通知任务 Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.13.12 回调设计」。
 */
@Mapper
public interface RechargeNotifyTaskMapper extends BaseMapper<RechargeNotifyTaskDO> {

    /**
     * 单表扫描到期通知任务。
     */
    default List<RechargeNotifyTaskDO> selectDueTasks(int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        return selectList(new LambdaQueryWrapper<RechargeNotifyTaskDO>()
                .in(RechargeNotifyTaskDO::getStatus, TaskStatus.INIT.name(), TaskStatus.FAILED.name())
                .le(RechargeNotifyTaskDO::getNextRetryTime, LocalDateTime.now())
                .orderByAsc(RechargeNotifyTaskDO::getNextRetryTime)
                .orderByAsc(RechargeNotifyTaskDO::getId)
                .last("limit " + safeLimit));
    }
}
