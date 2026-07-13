package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.cardshop.trade.engine.infrastructure.persistence.V1DepartProductReader;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartProductDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * 旧卡管商户商品关系表 `mc_depart_product` Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 * 新服务只按接口文档中的请求 {@code product_id} 精确读取商户授权，再到 {@code mc_product}
 * 读取同一商品的供应商配置；避免依赖 MyBatis-Plus BaseMapper 通用 statement，也遵守 SQL 禁止多表关联要求。
 */
@Mapper
public interface McDepartProductMapper extends V1DepartProductReader {

    /**
     * 按商户和请求商品读取已启用的商户商品关系。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」：
     * `/v1/cardapi/order/createRechargeOrder` 的 {@code product_id} 是平台提供的商品编号，不能再按
     * {@code parent_id} 拉取 sibling 商品做旧卡管式择优，否则受理价格、商品类型和实际派单目标会错位。
     */
    @Override
    @Select("""
            select
              id,
              org_code as orgCode,
              status,
              product_id as productId,
              price,
              sort,
              parent_id as parentId
            from mc_depart_product
            where org_code = #{orgCode}
              and product_id = #{productId}
              and status = '1'
            """)
    List<McDepartProductDO> selectEnabledByOrgAndProduct(
            @Param("orgCode") String orgCode,
            @Param("productId") String productId);
}
