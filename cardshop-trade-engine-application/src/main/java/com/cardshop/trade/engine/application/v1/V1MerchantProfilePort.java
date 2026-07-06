package com.cardshop.trade.engine.application.v1;

import java.util.Optional;

/**
 * V1 商户资料读取端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」。
 * 基础设施实现应按旧卡管 `mc_ext_depart` 的 appId/orgCode/status 语义读取商户资料，且不得把
 * 请求体传入的 orgCode 当作可信身份。
 */
@FunctionalInterface
public interface V1MerchantProfilePort {

    /**
     * 按 token 解析出的 appId 和 orgCode 查找商户资料。
     */
    Optional<V1MerchantProfile> findByAppIdAndOrgCode(String appId, String orgCode);
}
