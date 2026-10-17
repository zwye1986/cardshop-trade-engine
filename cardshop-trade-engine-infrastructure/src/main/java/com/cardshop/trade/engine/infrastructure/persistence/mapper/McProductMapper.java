package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.cardshop.trade.engine.infrastructure.persistence.V1ProductReader;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McProductDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.Collection;
import java.util.List;

/**
 * 旧卡管商品表 `mc_product` Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 * 查询只按商品编码集合读取启用商品，不和商户商品关系表做 SQL join；显式 SQL 避免运行期依赖
 * MyBatis-Plus BaseMapper 通用 statement。
 */
@Mapper
public interface McProductMapper extends V1ProductReader {

    /**
     * 按商品编码集合读取已启用商品。
     */
    @Override
    @Select("""
            <script>
            select
              id,
              status,
              product_id as productId,
              channel,
              external_product_id as externalProductId,
              coupon_type as couponType,
              product_name as productName,
              value,
              base_price as basePrice,
              type,
              account_type as accountType,
              version
            from mc_product
            where status = '1'
            <choose>
              <when test="productIds != null and productIds.size() > 0">
                and product_id in
                <foreach collection="productIds" item="productId" open="(" separator="," close=")">
                  #{productId}
                </foreach>
              </when>
              <otherwise>
                and 1 = 0
              </otherwise>
            </choose>
            </script>
            """)
    List<McProductDO> selectEnabledByProductIds(@Param("productIds") Collection<String> productIds);
}
