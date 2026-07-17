package com.cardshop.trade.engine.application.v1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * V1 商户配置启动预热服务测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。启动预热只处理
 * Nacos 白名单中的商户，补齐 Lua 下单前置依赖的 {@code merchant:config}，不能扫描全量旧库。
 */
class V1MerchantConfigPreheatServiceTest {

    @Test
    void preheatsDistinctConfiguredOrgCodesInOrder() {
        RecordingPreheatPort preheatPort = new RecordingPreheatPort();
        V1MerchantConfigPreheatService service = new V1MerchantConfigPreheatService(
                merchantPort(Map.of(
                        "A39", merchant("A39", true, 54L),
                        "B01", merchant("B01", false, 77L))),
                preheatPort,
                new V1MerchantConfigPreheatSettings(true, List.of(" A39 ", "", "A39", "B01"), true));

        V1MerchantConfigPreheatResult result = service.preheatConfiguredMerchants();

        assertThat(preheatPort.profiles)
                .extracting(V1MerchantProfile::orgCode)
                .containsExactly("A39", "B01");
        assertThat(result.requested()).isEqualTo(2);
        assertThat(result.succeeded()).isEqualTo(2);
        assertThat(result.failed()).isZero();
    }

    @Test
    void skipsAllWorkWhenPreheatIsDisabled() {
        RecordingPreheatPort preheatPort = new RecordingPreheatPort();
        V1MerchantConfigPreheatService service = new V1MerchantConfigPreheatService(
                merchantPort(Map.of("A39", merchant("A39", true, 54L))),
                preheatPort,
                new V1MerchantConfigPreheatSettings(false, List.of("A39"), true));

        V1MerchantConfigPreheatResult result = service.preheatConfiguredMerchants();

        assertThat(preheatPort.profiles).isEmpty();
        assertThat(result.requested()).isZero();
        assertThat(result.succeeded()).isZero();
        assertThat(result.failed()).isZero();
    }

    @Test
    void throwsWhenConfiguredMerchantIsMissingAndFailOnErrorIsEnabled() {
        V1MerchantConfigPreheatService service = new V1MerchantConfigPreheatService(
                merchantPort(Map.of()),
                new RecordingPreheatPort(),
                new V1MerchantConfigPreheatSettings(true, List.of("A39"), true));

        assertThatThrownBy(service::preheatConfiguredMerchants)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("A39");
    }

    @Test
    void throwsWhenMerchantConfigVersionIsMissingAndFailOnErrorIsEnabled() {
        V1MerchantConfigPreheatService service = new V1MerchantConfigPreheatService(
                merchantPort(Map.of("A39", merchant("A39", true, null))),
                new RecordingPreheatPort(),
                new V1MerchantConfigPreheatSettings(true, List.of("A39"), true));

        assertThatThrownBy(service::preheatConfiguredMerchants)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("configVersion");
    }

    @Test
    void continuesAfterSingleMerchantFailureWhenFailOnErrorIsDisabled() {
        RecordingPreheatPort preheatPort = new RecordingPreheatPort();
        preheatPort.failOrgCodes.add("A39");
        V1MerchantConfigPreheatService service = new V1MerchantConfigPreheatService(
                merchantPort(Map.of(
                        "A39", merchant("A39", true, 54L),
                        "B01", merchant("B01", true, 77L))),
                preheatPort,
                new V1MerchantConfigPreheatSettings(true, List.of("A39", "B01"), false));

        V1MerchantConfigPreheatResult result = service.preheatConfiguredMerchants();

        assertThat(preheatPort.profiles)
                .extracting(V1MerchantProfile::orgCode)
                .containsExactly("B01");
        assertThat(result.requested()).isEqualTo(2);
        assertThat(result.succeeded()).isEqualTo(1);
        assertThat(result.failed()).isEqualTo(1);
    }

    private static V1MerchantProfilePort merchantPort(Map<String, V1MerchantProfile> profiles) {
        Map<String, V1MerchantProfile> copy = new HashMap<>(profiles);
        return (appId, orgCode) -> Optional.ofNullable(copy.get(orgCode));
    }

    private static V1MerchantProfile merchant(String orgCode, boolean enabled, Long configVersion) {
        return new V1MerchantProfile("app-" + orgCode, orgCode, "secret", enabled, configVersion);
    }

    private static final class RecordingPreheatPort implements V1MerchantConfigPreheatPort {

        private final List<String> failOrgCodes = new ArrayList<>();
        private final List<V1MerchantProfile> profiles = new ArrayList<>();

        @Override
        public void preheat(V1MerchantProfile profile) {
            if (failOrgCodes.contains(profile.orgCode())) {
                throw new IllegalStateException("boom " + profile.orgCode());
            }
            profiles.add(profile);
        }
    }
}
