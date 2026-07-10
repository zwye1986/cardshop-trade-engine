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
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecordPort;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.application.v1.CreateRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxPort;
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
import org.springframework.beans.factory.ObjectProvider;
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

    /**
     * 创建 V1 直充下单用例。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
     * `AcceptAnomalyRecordPort` 是旁路能力：生产环境有 anomaly 表时写入核查入口，local profile
     * 或局部测试未装配该端口时退化为空实现，避免把“异常登记能力是否存在”变成下单 Bean 创建条件。
     * 核心依赖使用 {@link ObjectProvider} 在 Bean 创建时判断，而不是用 {@code @ConditionalOnBean}：
     * bootstrap 配置和 infrastructure 配置同属组件扫描，条件在配置解析期过早判断会使 dev 环境误注册 fallback。
     */
    @Bean
    @ConditionalOnMissingBean(CreateRechargeOrderUseCase.class)
    public CreateRechargeOrderUseCase v1CreateRechargeOrderUseCase(
            V1ProtocolPrecheckService precheckService,
            RequestHashCanonicalizer requestHashCanonicalizer,
            ObjectProvider<V1MerchantProfilePort> merchantProfilePortProvider,
            ObjectProvider<V1ProductCandidateSelector> productCandidateSelectorProvider,
            ObjectProvider<AcceptRechargeOrderPort> acceptRechargeOrderPortProvider,
            ObjectProvider<RechargeAcceptReliabilityPort> rechargeAcceptReliabilityPortProvider,
            ObjectProvider<PersistRechargeOrderMessagePublisher> persistMessagePublisherProvider,
            ObjectProvider<AcceptAnomalyRecordPort> acceptAnomalyRecordPortProvider,
            ObjectProvider<RedisHaGuardPort> redisHaGuardPortProvider,
            RechargeOrderIdGenerator orderIdGenerator,
            Clock clock,
            V1RechargeAcceptSettings settings,
            @org.springframework.beans.factory.annotation.Qualifier("tradeDbTaskExecutor") java.util.concurrent.Executor tradeDbTaskExecutor) {
        V1MerchantProfilePort merchantProfilePort = merchantProfilePortProvider.getIfAvailable();
        V1ProductCandidateSelector productCandidateSelector = productCandidateSelectorProvider.getIfAvailable();
        AcceptRechargeOrderPort acceptRechargeOrderPort = acceptRechargeOrderPortProvider.getIfAvailable();
        RechargeAcceptReliabilityPort rechargeAcceptReliabilityPort = rechargeAcceptReliabilityPortProvider.getIfAvailable();
        PersistRechargeOrderMessagePublisher persistMessagePublisher = persistMessagePublisherProvider.getIfAvailable();
        if (merchantProfilePort == null
                || productCandidateSelector == null
                || acceptRechargeOrderPort == null
                || rechargeAcceptReliabilityPort == null
                || persistMessagePublisher == null) {
            return unavailableCreateRechargeOrderUseCase();
        }
        return new V1CreateRechargeOrderUseCaseService(
                precheckService,
                requestHashCanonicalizer,
                merchantProfilePort,
                productCandidateSelector,
                acceptRechargeOrderPort,
                rechargeAcceptReliabilityPort,
                persistMessagePublisher,
                acceptAnomalyRecordPortProvider.getIfAvailable(AcceptAnomalyRecordPort::noop),
                redisHaGuardPortProvider.getIfAvailable(RedisHaGuardPort::noop),
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
    @ConditionalOnMissingBean(IssueV1TokenUseCase.class)
    public IssueV1TokenUseCase v1IssueTokenUseCase(
            ObjectProvider<V1MerchantProfilePort> merchantProfilePortProvider,
            ObjectProvider<V1TokenStorePort> tokenStorePortProvider,
            V1TokenSettings settings,
            Clock clock) {
        // 参见详细设计「20.13.5 Token 生成接口」：真实签发依赖旧商户表和共享 Redis token key。
        // 这里延迟到 Bean 创建期检查依赖，避免配置类解析顺序导致灰度环境误返回“Token生成暂未启用”。
        V1MerchantProfilePort merchantProfilePort = merchantProfilePortProvider.getIfAvailable();
        V1TokenStorePort tokenStorePort = tokenStorePortProvider.getIfAvailable();
        if (merchantProfilePort == null || tokenStorePort == null) {
            return unavailableIssueV1TokenUseCase();
        }
        return new V1IssueTokenUseCaseService(merchantProfilePort, tokenStorePort, settings, clock);
    }

    private static IssueV1TokenUseCase unavailableIssueV1TokenUseCase() {
        return rawBody -> V1CardApiResult.unavailable("新交易引擎Token生成暂未启用");
    }

    private static CreateRechargeOrderUseCase unavailableCreateRechargeOrderUseCase() {
        // TODO(D3/D4): 当 Redis 接单端口或商户/商品查询端口未装配时保守拒绝；参见 docs/交行直充业务系统详细设计.md「20.13.6 Redis Lua 下单结果」。
        return command -> CompletableFuture.completedFuture(V1CardApiResult.unavailable("新交易引擎直充受理暂未启用"));
    }

    @Bean
    @ConditionalOnMissingBean(QueryRechargeOrderUseCase.class)
    public QueryRechargeOrderUseCase v1QueryRechargeOrderUseCase(
            V1ProtocolPrecheckService precheckService,
            ObjectProvider<V1MerchantProfilePort> merchantProfilePortProvider,
            ObjectProvider<RechargeOrderQueryPort> orderQueryPortProvider) {
        /*
         * 参见 docs/交行直充业务系统详细设计.md「20.13.10 查单设计」。V1 查询端口依赖的 Redis/MySQL
         * 聚合适配器来自 infrastructure 自动配置，真实 dev/test 启动时它们可能晚于 bootstrap 普通配置类
         * 出现。这里沿用下单/Token 的延迟取 Bean 模式，避免 @ConditionalOnBean 在配置解析期过早失败后
         * 把 fallback 注册成最终 QueryRechargeOrderUseCase，导致灰度请求误返回“新交易引擎查单暂未启用”。
         */
        V1MerchantProfilePort merchantProfilePort = merchantProfilePortProvider.getIfAvailable();
        RechargeOrderQueryPort orderQueryPort = orderQueryPortProvider.getIfAvailable();
        if (merchantProfilePort == null || orderQueryPort == null) {
            return unavailableQueryRechargeOrderUseCase();
        }
        return new V1QueryRechargeOrderUseCaseService(precheckService, merchantProfilePort, orderQueryPort);
    }

    private static QueryRechargeOrderUseCase unavailableQueryRechargeOrderUseCase() {
        // TODO(D6): 接入 Redis/MySQL 查单聚合；参见 docs/交行直充业务系统详细设计.md「20.13.10 查单设计」。
        return command -> V1CardApiResult.unavailable("新交易引擎查单暂未启用");
    }

    @Bean
    @ConditionalOnMissingBean(InvalidRechargeOrderUseCase.class)
    public InvalidRechargeOrderUseCase v1InvalidRechargeOrderUseCase(
            V1ProtocolPrecheckService precheckService,
            ObjectProvider<V1MerchantProfilePort> merchantProfilePortProvider,
            ObjectProvider<RechargeOrderQueryPort> orderQueryPortProvider,
            ObjectProvider<InvalidRechargeOrderOutboxPort> invalidOrderOutboxPortProvider) {
        /*
         * 参见 docs/交行直充业务系统详细设计.md「20.13.11 作废设计」。作废入口除了商户资料和订单查询端口，
         * 还依赖 MyBatis outbox adapter 暴露的 InvalidRechargeOrderOutboxPort。该 adapter 由
         * infrastructure 后置自动配置创建；若使用 @ConditionalOnBean，test188 dev 会在真实端口出现前
         * 注册 fallback，表现为已灰度、已验签但 `/v1/cardapi/order/invalid` 返回“暂未启用”。这里在 Bean
         * 创建期取真实依赖，缺失时仍 fail closed，不把未装配环境伪装成可作废。
         */
        V1MerchantProfilePort merchantProfilePort = merchantProfilePortProvider.getIfAvailable();
        RechargeOrderQueryPort orderQueryPort = orderQueryPortProvider.getIfAvailable();
        InvalidRechargeOrderOutboxPort invalidOrderOutboxPort = invalidOrderOutboxPortProvider.getIfAvailable();
        if (merchantProfilePort == null || orderQueryPort == null || invalidOrderOutboxPort == null) {
            return unavailableInvalidRechargeOrderUseCase();
        }
        return new V1InvalidRechargeOrderUseCaseService(
                precheckService,
                merchantProfilePort,
                orderQueryPort,
                invalidOrderOutboxPort);
    }

    private static InvalidRechargeOrderUseCase unavailableInvalidRechargeOrderUseCase() {
        // TODO(D6): 接入作废状态机和供应商撤单能力；参见 docs/交行直充业务系统详细设计.md「20.13.11 作废设计」。
        return command -> V1CardApiResult.unavailable("新交易引擎作废暂未启用");
    }
}
