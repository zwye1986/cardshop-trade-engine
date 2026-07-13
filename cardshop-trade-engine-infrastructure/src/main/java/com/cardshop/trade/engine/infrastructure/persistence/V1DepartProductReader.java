package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartProductDO;

import java.util.List;

/**
 * V1 商户商品关系单表读取接口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 * 读取条件必须使用外部请求 {@code product_id} 精确匹配 {@code mc_depart_product.product_id}，
 * 不再读取 {@code parent_id} 下的 sibling 商品集合。
 */
@FunctionalInterface
public interface V1DepartProductReader {

    List<McDepartProductDO> selectEnabledByOrgAndProduct(String orgCode, String productId);
}
