package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolSettings;
import com.cardshop.trade.engine.api.v1.routing.V1RoutingSettings;
import com.cardshop.trade.engine.api.v1.security.V1TimestampSettings;
import com.cardshop.trade.engine.api.v1.security.V1TokenSettings;
import com.cardshop.trade.engine.api.v1.usecase.V1RechargeAcceptSettings;
import com.cardshop.trade.engine.application.v1.CreateRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.IssueV1TokenUseCase;
import com.cardshop.trade.engine.application.v1.QueryRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.domain.service.RequestHashCanonicalizer;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
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
    public V1RechargeAcceptSettings v1RechargeAcceptSettings(TradeEngineProperties properties) {
        return new V1RechargeAcceptSettings(Duration.ofSeconds(properties.getOrder().getPendingProtectSeconds()));
    }

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public RequestHashCanonicalizer requestHashCanonicalizer() {
        return new RequestHashCanonicalizer();
    }

    @Bean
    @ConditionalOnMissingBean(IssueV1TokenUseCase.class)
    public IssueV1TokenUseCase unavailableIssueV1TokenUseCase() {
        // TODO(D5): 接入商户凭证校验和 Redis token 写入；参见 docs/交行直充业务系统详细设计.md「20.13.5 Token 生成接口」。
        return rawBody -> V1CardApiResult.unavailable("新交易引擎Token生成暂未启用");
    }

    @Bean
    public CreateRechargeOrderUseCase unavailableCreateRechargeOrderUseCase() {
        // TODO(D3/D4): 当 Redis 接单端口或商户/商品查询端口未装配时保守拒绝；参见 docs/交行直充业务系统详细设计.md「20.13.6 Redis Lua 下单结果」。
        return command -> V1CardApiResult.unavailable("新交易引擎直充受理暂未启用");
    }

    @Bean
    @ConditionalOnMissingBean(QueryRechargeOrderUseCase.class)
    public QueryRechargeOrderUseCase unavailableQueryRechargeOrderUseCase() {
        // TODO(D6): 接入 Redis/MySQL 查单聚合；参见 docs/交行直充业务系统详细设计.md「20.13.10 查单设计」。
        return command -> V1CardApiResult.unavailable("新交易引擎查单暂未启用");
    }

    @Bean
    @ConditionalOnMissingBean(InvalidRechargeOrderUseCase.class)
    public InvalidRechargeOrderUseCase unavailableInvalidRechargeOrderUseCase() {
        // TODO(D6): 接入作废状态机和供应商撤单能力；参见 docs/交行直充业务系统详细设计.md「20.13.11 作废设计」。
        return command -> V1CardApiResult.unavailable("新交易引擎作废暂未启用");
    }
}
