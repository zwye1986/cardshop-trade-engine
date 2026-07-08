package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartRecordDO;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

/**
 * 旧系统商户资金流水表 `mc_depart_record` Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。
 * 新服务通过 Liquibase 为 `type=1 + order_id` 增加生成列唯一键 `trade_engine_record_key`，
 * 本 Mapper 使用 `on duplicate key update` 实现 DB 级幂等，避免 RabbitMQ 重投或补偿任务并发导致重复扣款流水。
 */
@Mapper
public interface McDepartRecordMapper extends BaseMapper<McDepartRecordDO> {

    /**
     * 写入资金流水。
     *
     * <p>旧表字段 `type=1` 表示扣款，`value` 按正数记录扣款金额；`trade_engine_record_key`
     * 是生成列，不参与 insert。唯一冲突时保持原流水不变。
     */
    @Insert("""
            insert into mc_depart_record (
              org_code,
              before_balance,
              balance,
              create_time,
              type,
              order_id,
              `value`
            )
            values (
              #{orgCode},
              #{beforeBalance},
              #{balance},
              #{createTime},
              #{type},
              #{orderId},
              #{value}
            )
            on duplicate key update
              order_id = order_id
            """)
    int insertIfAbsent(McDepartRecordDO record);
}
