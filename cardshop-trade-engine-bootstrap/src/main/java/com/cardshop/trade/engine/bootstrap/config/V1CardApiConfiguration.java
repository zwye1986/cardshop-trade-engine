package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckService;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolSettings;
import com.cardshop.trade.engine.api.v1.routing.V1RoutingSettings;
import com.cardshop.trade.engine.api.v1.security.V1TimestampSettings;
import com.cardshop.trade.engine.api.v1.security.V1TokenSettings;
import com.cardshop.trade.engine.api.v1.usecase.V1CreateRechargeOrderUseCaseService;
import com.cardshop.trade.engine.api.v1.usecase.V1InvalidRechargeOrderUseCaseService;
import com.cardshop.trade.engine.api.v1.usecase.V1IssueTokenUseCaseService;
import com.cardshop.trade.engine.api.v1.usecase.V1QueryRechargeOrderUseCaseService;
import com.cardshop.trade.engine.api.v1.usecase.V1RechargeAcceptSettings;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.v1.CreateRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.IssueV1TokenUseCase;
import com.cardshop.trade.engine.application.v1.QueryRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderIdGenerator;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.application.v1.V1ProductCandidateSelector;
import com.cardshop.trade.engine.application.v1.V1TokenStorePort;
import com.cardshop.trade.engine.domain.service.RequestHashCanonicalizer;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

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
                Duration.ofSeconds(v1.getTokenClockSkewSeconds()),
                v1.getTokenRedisKeyPrefix(),
                Duration.ofSeconds(v1.getTokenTtlSeconds()));
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
    @ConditionalOnBean({
            AcceptRechargeOrderPort.class,
            RechargeAcceptReliabilityPort.class,
            PersistRechargeOrderMessagePublisher.class,
            V1MerchantProfilePort.class,
            V1ProductCandidateSelector.class
    })
    @ConditionalOnMissingBean(CreateRechargeOrderUseCase.class)
    public CreateRechargeOrderUseCase v1CreateRechargeOrderUseCase(
            V1ProtocolPrecheckService precheckService,
            RequestHashCanonicalizer requestHashCanonicalizer,
            V1MerchantProfilePort merchantProfilePort,
            V1ProductCandidateSelector productCandidateSelector,
            AcceptRechargeOrderPort acceptRechargeOrderPort,
            RechargeAcceptReliabilityPort rechargeAcceptReliabilityPort,
            PersistRechargeOrderMessagePublisher persistMessagePublisher,
            RechargeOrderIdGenerator orderIdGenerator,
            Clock clock,
            V1RechargeAcceptSettings settings,
            @org.springframework.beans.factory.annotation.Qualifier("tradeDbTaskExecutor") java.util.concurrent.Executor tradeDbTaskExecutor) {
        return new V1CreateRechargeOrderUseCaseService(
                precheckService,
                requestHashCanonicalizer,
                merchantProfilePort,
                productCandidateSelector,
                acceptRechargeOrderPort,
                rechargeAcceptReliabilityPort,
                persistMessagePublisher,
                orderIdGenerator,
                clock,
                settings,
                tradeDbTaskExecutor);
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
    @ConditionalOnBean({V1MerchantProfilePort.class, V1TokenStorePort.class})
    @ConditionalOnMissingBean(IssueV1TokenUseCase.class)
    public IssueV1TokenUseCase v1IssueTokenUseCase(
            V1MerchantProfilePort merchantProfilePort,
            V1TokenStorePort tokenStorePort,
            V1TokenSettings settings,
            Clock clock) {
        return new V1IssueTokenUseCaseService(merchantProfilePort, tokenStorePort, settings, clock);
    }

    @Bean
    @ConditionalOnMissingBean(IssueV1TokenUseCase.class)
    public IssueV1TokenUseCase unavailableIssueV1TokenUseCase() {
        return rawBody -> V1CardApiResult.unavailable("新交易引擎Token生成暂未启用");
    }

    @Bean
    @ConditionalOnMissingBean(CreateRechargeOrderUseCase.class)
    public CreateRechargeOrderUseCase unavailableCreateRechargeOrderUseCase() {
        // TODO(D3/D4): 当 Redis 接单端口或商户/商品查询端口未装配时保守拒绝；参见 docs/交行直充业务系统详细设计.md「20.13.6 Redis Lua 下单结果」。
        return command -> CompletableFuture.completedFuture(V1CardApiResult.unavailable("新交易引擎直充受理暂未启用"));
    }

    @Bean
    @ConditionalOnBean({V1MerchantProfilePort.class, RechargeOrderQueryPort.class})
    @ConditionalOnMissingBean(QueryRechargeOrderUseCase.class)
    public QueryRechargeOrderUseCase v1QueryRechargeOrderUseCase(
            V1ProtocolPrecheckService precheckService,
            V1MerchantProfilePort merchantProfilePort,
            RechargeOrderQueryPort orderQueryPort) {
        return new V1QueryRechargeOrderUseCaseService(precheckService, merchantProfilePort, orderQueryPort);
    }

    @Bean
    @ConditionalOnMissingBean(QueryRechargeOrderUseCase.class)
    public QueryRechargeOrderUseCase unavailableQueryRechargeOrderUseCase() {
        // TODO(D6): 接入 Redis/MySQL 查单聚合；参见 docs/交行直充业务系统详细设计.md「20.13.10 查单设计」。
        return command -> V1CardApiResult.unavailable("新交易引擎查单暂未启用");
    }

    @Bean
    @ConditionalOnBean({V1MerchantProfilePort.class, RechargeOrderQueryPort.class})
    @ConditionalOnMissingBean(InvalidRechargeOrderUseCase.class)
    public InvalidRechargeOrderUseCase v1InvalidRechargeOrderUseCase(
            V1ProtocolPrecheckService precheckService,
            V1MerchantProfilePort merchantProfilePort,
            RechargeOrderQueryPort orderQueryPort) {
        return new V1InvalidRechargeOrderUseCaseService(precheckService, merchantProfilePort, orderQueryPort);
    }

    @Bean
    @ConditionalOnMissingBean(InvalidRechargeOrderUseCase.class)
    public InvalidRechargeOrderUseCase unavailableInvalidRechargeOrderUseCase() {
        // TODO(D6): 接入作废状态机和供应商撤单能力；参见 docs/交行直充业务系统详细设计.md「20.13.11 作废设计」。
        return command -> V1CardApiResult.unavailable("新交易引擎作废暂未启用");
    }
}
