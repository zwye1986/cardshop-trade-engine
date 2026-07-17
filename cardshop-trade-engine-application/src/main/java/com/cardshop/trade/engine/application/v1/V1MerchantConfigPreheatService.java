package com.cardshop.trade.engine.application.v1;

import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V1 商户配置启动预热服务。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。该服务只按显式白名单
 * 补齐 Lua 下单硬依赖的 {@code merchant:config.status/version}，不扫描全量商户，也不负责余额或商品配置预热。
 */
public class V1MerchantConfigPreheatService {

    private static final Logger log = LoggerFactory.getLogger(V1MerchantConfigPreheatService.class);

    private final V1MerchantProfilePort merchantProfilePort;
    private final V1MerchantConfigPreheatPort preheatPort;
    private final V1MerchantConfigPreheatSettings settings;

    public V1MerchantConfigPreheatService(
            V1MerchantProfilePort merchantProfilePort,
            V1MerchantConfigPreheatPort preheatPort,
            V1MerchantConfigPreheatSettings settings) {
        this.merchantProfilePort = Objects.requireNonNull(merchantProfilePort, "merchantProfilePort must not be null");
        this.preheatPort = Objects.requireNonNull(preheatPort, "preheatPort must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
    }

    /**
     * 执行配置白名单中的商户预热。
     */
    public V1MerchantConfigPreheatResult preheatConfiguredMerchants() {
        if (!settings.enabled()) {
            return new V1MerchantConfigPreheatResult(0, 0, 0);
        }
        List<String> orgCodes = settings.normalizedOrgCodes();
        int succeeded = 0;
        int failed = 0;
        for (String orgCode : orgCodes) {
            try {
                V1MerchantProfile profile = merchantProfilePort.findByOrgCode(orgCode)
                        .orElseThrow(() -> new IllegalStateException("V1商户配置预热未找到商户 orgCode=" + orgCode));
                validateProfile(orgCode, profile);
                preheatPort.preheat(profile);
                succeeded++;
                log.info("V1商户配置启动预热完成 orgCode={}, enabled={}, version={}",
                        profile.orgCode(),
                        profile.enabled(),
                        profile.configVersion());
            } catch (RuntimeException ex) {
                failed++;
                if (settings.failOnError()) {
                    throw new IllegalStateException(
                            "V1商户配置启动预热失败 orgCode=" + orgCode + ", reason=" + ex.getMessage(),
                            ex);
                }
                log.error("V1商户配置启动预热失败，已按配置继续启动 orgCode={}", orgCode, ex);
            }
        }
        return new V1MerchantConfigPreheatResult(orgCodes.size(), succeeded, failed);
    }

    private static void validateProfile(String requestedOrgCode, V1MerchantProfile profile) {
        if (profile.orgCode() == null || profile.orgCode().isBlank()) {
            throw new IllegalStateException("V1商户配置预热返回空orgCode requestedOrgCode=" + requestedOrgCode);
        }
        if (profile.configVersion() == null || profile.configVersion() <= 0) {
            throw new IllegalStateException("V1商户配置预热缺少configVersion orgCode=" + requestedOrgCode);
        }
    }
}
