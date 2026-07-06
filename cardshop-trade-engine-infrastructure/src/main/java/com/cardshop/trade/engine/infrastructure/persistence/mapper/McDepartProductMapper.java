package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.infrastructure.persistence.V1DepartProductReader;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McDepartProductDO;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * 旧卡管商户商品关系表 `mc_depart_product` Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.5 商品选择规则」。
 * 旧系统 XML 通过多表关联返回商品快照；新服务拆成单表查询后在 Java 内组装，遵守 SQL 禁止多表关联要求。
 */
@Mapper
public interface McDepartProductMapper extends BaseMapper<McDepartProductDO>, V1DepartProductReader {

    /**
     * 按商户和父商品读取已启用的商户商品关系。
     */
    default List<McDepartProductDO> selectEnabledByOrgAndParent(String orgCode, String parentProductId) {
        return selectList(new LambdaQueryWrapper<McDepartProductDO>()
                .eq(McDepartProductDO::getOrgCode, orgCode)
                .eq(McDepartProductDO::getParentId, parentProductId)
                .eq(McDepartProductDO::getStatus, "1"));
    }
}
