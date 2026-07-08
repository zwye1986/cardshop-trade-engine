package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderSupplierFlowDO;
import org.apache.ibatis.annotations.Mapper;

/**
 * 旧系统供应商调用流水 Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」。
 */
@Mapper
public interface McOrderSupplierFlowMapper extends BaseMapper<McOrderSupplierFlowDO> {
}
