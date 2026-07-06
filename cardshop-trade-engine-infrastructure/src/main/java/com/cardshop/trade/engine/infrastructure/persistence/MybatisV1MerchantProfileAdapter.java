package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McExtDepartDO;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Optional;

/**
 * 基于旧卡管 `mc_ext_depart` 的 V1 商户资料适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」。
 * 该适配器只读商户安全资料，不写余额、不写订单，避免和旧卡管余额链路发生双写。
 */
@Component
@ConditionalOnBean(V1MerchantProfileReader.class)
public class MybatisV1MerchantProfileAdapter implements V1MerchantProfilePort {

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
