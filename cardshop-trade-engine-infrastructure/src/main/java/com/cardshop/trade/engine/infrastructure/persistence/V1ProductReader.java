package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.infrastructure.persistence.entity.McProductDO;

import java.util.Collection;
import java.util.List;

/**
 * V1 商品单表读取接口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 */
@FunctionalInterface
public interface V1ProductReader {

    List<McProductDO> selectEnabledByProductIds(Collection<String> productIds);
}
