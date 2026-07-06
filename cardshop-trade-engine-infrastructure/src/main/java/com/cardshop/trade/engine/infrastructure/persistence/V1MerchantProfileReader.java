package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;

/**
 * V1 商户资料单表读取接口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」。
 * 抽出该小接口是为了让 MyBatis Mapper 和单元测试 fake 都只暴露必要查询能力。
 */
@FunctionalInterface
public interface V1MerchantProfileReader {

    McExtDepartDO selectByAppIdAndOptionalOrg(String appId, String orgCode);
}
