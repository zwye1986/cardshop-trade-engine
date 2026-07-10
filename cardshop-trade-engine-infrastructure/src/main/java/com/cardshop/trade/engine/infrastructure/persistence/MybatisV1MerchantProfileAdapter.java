package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;
import org.apache.ibatis.exceptions.TooManyResultsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.Optional;

/**
 * 基于旧卡管 `mc_ext_depart` 的 V1 商户资料适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。
 * 该适配器只读商户安全资料，不写余额、不写订单，避免和旧卡管余额链路发生双写。
 */
public class MybatisV1MerchantProfileAdapter implements V1MerchantProfilePort {

    private static final Logger log = LoggerFactory.getLogger(MybatisV1MerchantProfileAdapter.class);
    private static final String DEPART_STATUS_NORMAL = "0";

    private final V1MerchantProfileReader reader;

    public MybatisV1MerchantProfileAdapter(V1MerchantProfileReader reader) {
        this.reader = reader;
    }

    @Override
    public Optional<V1MerchantProfile> findByAppIdAndOrgCode(String appId, String orgCode) {
        if (!StringUtils.hasText(appId)) {
            return Optional.empty();
        }
        McExtDepartDO merchant = reader.selectByAppIdAndOptionalOrg(appId, orgCode);
        return toProfile(merchant);
    }

    /**
     * `/auth/getToken` 使用 appId/appSecret 定位唯一商户，参见详细设计「3. API/V1 接入层」。
     * 旧表若出现重复凭证，不能静默挑选第一条，否则后续 SM4 密钥、机构号和 Redis token 归属都可能错位；
     * 因此捕获 MyBatis-Plus 非唯一结果并按凭证错误处理，日志只记录 appId，避免泄露 appSecret。
     */
    @Override
    public Optional<V1MerchantProfile> findByAppIdAndAppSecret(String appId, String appSecret) {
        if (!StringUtils.hasText(appId) || !StringUtils.hasText(appSecret)) {
            return Optional.empty();
        }
        try {
            McExtDepartDO merchant = reader.selectByAppIdAndAppSecret(appId, appSecret);
            return toProfile(merchant);
        } catch (TooManyResultsException ex) {
            log.error("V1 token credential lookup matched multiple merchants, appId={}, error={}",
                    appId, ex.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public Optional<V1MerchantProfile> findByOrgCode(String orgCode) {
        if (!StringUtils.hasText(orgCode)) {
            return Optional.empty();
        }
        return toProfile(reader.selectByOrgCode(orgCode));
    }

    private static Optional<V1MerchantProfile> toProfile(McExtDepartDO merchant) {
        if (merchant == null || !StringUtils.hasText(merchant.getOrgCode())) {
            return Optional.empty();
        }
        return Optional.of(new V1MerchantProfile(
                merchant.getAppId(),
                merchant.getOrgCode(),
                merchant.getAesKey(),
                DEPART_STATUS_NORMAL.equals(merchant.getDepartStatus()),
                version(merchant.getId())));
    }

    private static Long version(Long id) {
        return id == null || id <= 0 ? 1L : id;
    }
}
