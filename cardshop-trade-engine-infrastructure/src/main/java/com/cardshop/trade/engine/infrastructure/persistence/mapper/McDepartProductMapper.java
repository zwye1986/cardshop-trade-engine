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
 * 旧系统 XML 通过多表关联返回商品快照；新服务拆成显式单表查询后在 Java 内组装，
 * 避免依赖 MyBatis-Plus BaseMapper 通用 statement，也遵守 SQL 禁止多表关联要求。
 */
@Mapper
public interface McDepartProductMapper extends V1DepartProductReader {

    /**
     * 按商户和父商品读取已启用的商户商品关系。
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
              and parent_id = #{parentProductId}
              and status = '1'
            """)
    List<McDepartProductDO> selectEnabledByOrgAndParent(
            @Param("orgCode") String orgCode,
            @Param("parentProductId") String parentProductId);
}
