package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolSettings;
import com.cardshop.trade.engine.api.v1.routing.V1RoutingSettings;
import com.cardshop.trade.engine.api.v1.security.V1TimestampSettings;
import com.cardshop.trade.engine.api.v1.security.V1TokenSettings;
import com.cardshop.trade.engine.application.v1.UnavailableV1CardApiUseCaseService;
import com.cardshop.trade.engine.domain.service.RequestHashCanonicalizer;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * `/v1/cardapi` 入口框架 Bean 装配。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13 `/v1/cardapi` 小粒度详细设计」。
 * 这里负责把 infrastructure 的 Nacos 绑定配置转换为 API 模块的轻量配置视图，避免 API 反向依赖基础设施模块。
 */
@Configuration(proxyBeanMethods = false)
public class V1CardApiConfiguration {

    @Bean
    public V1RoutingSettings v1RoutingSettings(TradeEngineProperties properties) {
        var routing = properties.getRouting();
        return new V1RoutingSettings(
                routing.isEnabled(),
                routing.getLegacyBaseUrl(),
                Duration.ofMillis(routing.getProxyTimeoutMs()),
                routing.getGrayOrgCodes(),
                routing.getGrayAppIds(),
                routing.getSupportedPaths());
    }

    @Bean
    public V1TokenSettings v1TokenSettings(TradeEngineProperties properties) {
        var v1 = properties.getV1();
        return new V1TokenSettings(
                v1.getJwtSecret(),
                v1.getTokenClaimKey(),
                Duration.ofSeconds(v1.getTokenClockSkewSeconds()));
    }

    @Bean
    public V1TimestampSettings v1TimestampSettings(TradeEngineProperties properties) {
        return new V1TimestampSettings(Duration.ofSeconds(properties.getV1().getTimestampAllowedSkewSeconds()));
    }

    @Bean
    public V1ProtocolSettings v1ProtocolSettings(TradeEngineProperties properties) {
        return new V1ProtocolSettings(properties.getV1().isRequestSm4Enabled());
    }

    @Bean
    public RequestHashCanonicalizer requestHashCanonicalizer() {
        return new RequestHashCanonicalizer();
    }

    @Bean
    public UnavailableV1CardApiUseCaseService unavailableV1CardApiUseCaseService() {
        return new UnavailableV1CardApiUseCaseService();
    }
}
