package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.infrastructure.persistence.V1ProductReader;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McProductDO;
import org.apache.ibatis.annotations.Mapper;

import java.util.Collection;
import java.util.List;

/**
 * 旧卡管商品表 `mc_product` Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.5 商品选择规则」。
 * 查询只按商品编码集合读取启用商品，不和商户商品关系表做 SQL join。
 */
@Mapper
public interface McProductMapper extends BaseMapper<McProductDO>, V1ProductReader {

    /**
     * 按商品编码集合读取已启用商品。
     */
    default List<McProductDO> selectEnabledByProductIds(Collection<String> productIds) {
        return selectList(new LambdaQueryWrapper<McProductDO>()
                .in(McProductDO::getProductId, productIds)
                .eq(McProductDO::getStatus, "1"));
    }
}
