package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;

/**
 * V1 商户资料单表读取接口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。
 * 抽出该小接口是为了让 MyBatis Mapper 和单元测试 fake 都只暴露必要查询能力。
 */
@FunctionalInterface
public interface V1MerchantProfileReader {

    McExtDepartDO selectByAppIdAndOptionalOrg(String appId, String orgCode);

    /**
     * 按旧 `/auth/getToken` 的 app_id/app_secret 读取商户资料。
     *
     * <p>默认返回 null，便于单测使用 lambda 只覆盖 token 鉴权读取；真实 Mapper 必须覆盖该方法。
     */
    default McExtDepartDO selectByAppIdAndAppSecret(String appId, String appSecret) {
        return null;
    }

    /**
     * 按商户编码读取回调签名资料。
     */
    default McExtDepartDO selectByOrgCode(String orgCode) {
        return null;
    }
}
