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

    /**
     * 按商户编码读取回调签名资料。
     *
     * <p>回调任务只有 orgCode，不带原始 token appId；真实基础设施适配器应覆盖该方法并按
     * {@code mc_ext_depart.org_code} 单表读取。
     */
    default Optional<V1MerchantProfile> findByOrgCode(String orgCode) {
        return findByAppIdAndOrgCode("", orgCode);
    }

    /**
     * 按 `/auth/getToken` 请求中的 appId/appSecret 查找商户资料。
     *
     * <p>默认返回空，避免影响只需要 token 鉴权资料读取的测试替身；真实基础设施适配器必须实现该方法。
     */
    default Optional<V1MerchantProfile> findByAppIdAndAppSecret(String appId, String appSecret) {
        return Optional.empty();
    }
}
