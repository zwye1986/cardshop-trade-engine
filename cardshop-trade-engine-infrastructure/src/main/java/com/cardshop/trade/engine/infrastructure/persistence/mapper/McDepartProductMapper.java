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
 * V1 接口请求 {@code product_id} 是商户看到的父产品号，本 Mapper 按旧卡管语义读取该父产品下
 * 已授权的子商品，再由 Java 层读取 {@code mc_product} 并完成候选排序；避免依赖 MyBatis-Plus
 * BaseMapper 通用 statement，也遵守 SQL 禁止多表关联要求。
 */
@Mapper
public interface McDepartProductMapper extends V1DepartProductReader {

    /**
     * 按商户和请求父商品读取已启用的商户商品关系。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」：
     * `/v1/cardapi/order/createRechargeOrder` 的 {@code product_id} 是平台提供的父商品编号，
     * 需要按 {@code parent_id} 拉取该父商品下的候选子商品。
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
              and parent_id = #{productId}
              and status = '1'
            """)
    List<McDepartProductDO> selectEnabledByOrgAndProduct(
            @Param("orgCode") String orgCode,
            @Param("productId") String productId);
}
