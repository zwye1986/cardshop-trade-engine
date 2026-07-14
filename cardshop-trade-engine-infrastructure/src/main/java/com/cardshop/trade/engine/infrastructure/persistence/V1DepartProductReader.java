package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartProductDO;

import java.util.List;

/**
 * V1 商户商品关系单表读取接口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 * 读取条件使用外部请求 {@code product_id} 匹配 {@code mc_depart_product.parent_id}，
 * 返回该父商品下商户已授权的候选子商品集合。
 */
@FunctionalInterface
public interface V1DepartProductReader {

    List<McDepartProductDO> selectEnabledByOrgAndProduct(String orgCode, String productId);
}
