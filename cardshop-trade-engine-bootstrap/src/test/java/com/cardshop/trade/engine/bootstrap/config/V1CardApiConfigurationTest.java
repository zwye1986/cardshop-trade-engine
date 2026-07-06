package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckService;
import com.cardshop.trade.engine.api.v1.usecase.V1CreateRechargeOrderUseCaseService;
import com.cardshop.trade.engine.api.v1.usecase.V1IssueTokenUseCaseService;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderPort;
import com.cardshop.trade.engine.application.recharge.PersistRechargeOrderMessagePublisher;
import com.cardshop.trade.engine.application.recharge.RechargeAcceptReliabilityPort;
import com.cardshop.trade.engine.application.v1.CreateRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.IssueV1TokenUseCase;
import com.cardshop.trade.engine.application.v1.QueryRechargeOrderUseCase;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderIdGenerator;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import com.cardshop.trade.engine.application.v1.V1ProductCandidateSelector;
import com.cardshop.trade.engine.application.v1.V1TokenStorePort;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V1CardApiConfiguration 装配正确性测试。
 *
 * <p>验证 {@link CreateRechargeOrderUseCase} bean 在两种场景下都只有一个：
 * <ul>
 *   <li>依赖就绪时注册真实实现</li>
 *   <li>依赖缺失时注册 fallback</li>
 * </ul>
 * 该断言保证 {@code @ConditionalOnBean}+{@code @ConditionalOnMissingBean} 互斥装配正确。
 */
class V1CardApiConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(V1CardApiConfiguration.class, StubBeans.class);

    @Test
    void registersSingleRealBeanWhenDependenciesPresent() {
        contextRunner
                .withBean(AcceptRechargeOrderPort.class, () -> null)
                .withBean(RechargeAcceptReliabilityPort.class, () -> null)
                .withBean(PersistRechargeOrderMessagePublisher.class, () -> null)
                .withBean(V1MerchantProfilePort.class, () -> null)
                .withBean(V1ProductCandidateSelector.class, () -> null)
                .run(context -> {
                    assertThat(context.getBeansOfType(CreateRechargeOrderUseCase.class))
                            .as("依赖就绪时应只注册真实实现，不注册 fallback")
                            .hasSize(1);
                    assertThat(context).hasSingleBean(CreateRechargeOrderUseCase.class);
                    assertThat(context).hasBean("v1CreateRechargeOrderUseCase");
                    assertThat(context).doesNotHaveBean("unavailableCreateRechargeOrderUseCase");
                    assertThat(context.getBean(CreateRechargeOrderUseCase.class))
                            .isInstanceOf(V1CreateRechargeOrderUseCaseService.class);
                });
    }

    @Test
    void registersSingleFallbackBeanWhenDependenciesMissing() {
        contextRunner
                .run(context -> {
                    assertThat(context.getBeansOfType(CreateRechargeOrderUseCase.class))
                            .as("依赖缺失时应只注册 fallback 拒绝实现")
                            .hasSize(1);
                    assertThat(context).hasSingleBean(CreateRechargeOrderUseCase.class);
                    assertThat(context).doesNotHaveBean("v1CreateRechargeOrderUseCase");
                    assertThat(context).hasBean("unavailableCreateRechargeOrderUseCase");
                    // fallback 返回的是 bean method 直接 new 的 lambda，不是 V1CreateRechargeOrderUseCaseService
                    assertThat(context.getBean(CreateRechargeOrderUseCase.class))
                            .isNotInstanceOf(V1CreateRechargeOrderUseCaseService.class);
                });
    }

    @Test
    void registersRealIssueTokenUseCaseWhenMerchantAndTokenStorePresent() {
        contextRunner
                .withBean(V1MerchantProfilePort.class, () -> null)
                .withBean(V1TokenStorePort.class, () -> null)
                .run(context -> {
                    assertThat(context.getBeansOfType(IssueV1TokenUseCase.class))
                            .as("商户资料和 token store 就绪时应注册真实 token 生成用例")
                            .hasSize(1);
                    assertThat(context).hasBean("v1IssueTokenUseCase");
                    assertThat(context).doesNotHaveBean("unavailableIssueV1TokenUseCase");
                    assertThat(context.getBean(IssueV1TokenUseCase.class))
                            .isInstanceOf(V1IssueTokenUseCaseService.class);
                });
    }

    @Test
    void registersFallbackIssueTokenUseCaseWhenTokenStoreMissing() {
        contextRunner
                .withBean(V1MerchantProfilePort.class, () -> null)
                .run(context -> {
                    assertThat(context.getBeansOfType(IssueV1TokenUseCase.class)).hasSize(1);
                    assertThat(context).doesNotHaveBean("v1IssueTokenUseCase");
                    assertThat(context).hasBean("unavailableIssueV1TokenUseCase");
                    assertThat(context.getBean(IssueV1TokenUseCase.class))
                            .isNotInstanceOf(V1IssueTokenUseCaseService.class);
                });
    }

    @Test
    void registersRealQueryAndInvalidUseCasesWhenOrderQueryPortPresent() {
        contextRunner
                .withBean(V1MerchantProfilePort.class, () -> null)
                .withBean(RechargeOrderQueryPort.class, () -> null)
                .run(context -> {
                    assertThat(context).hasBean("v1QueryRechargeOrderUseCase");
                    assertThat(context).hasBean("v1InvalidRechargeOrderUseCase");
                    assertThat(context).doesNotHaveBean("unavailableQueryRechargeOrderUseCase");
                    assertThat(context).doesNotHaveBean("unavailableInvalidRechargeOrderUseCase");
                    assertThat(context).hasSingleBean(QueryRechargeOrderUseCase.class);
                    assertThat(context).hasSingleBean(InvalidRechargeOrderUseCase.class);
                });
    }

    @org.springframework.boot.test.context.TestConfiguration
    static class StubBeans {
        @Bean
        com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties tradeEngineProperties() {
            return new com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties();
        }

        /**
         * 协议预检 stub，仅满足 Bean 依赖链，不接受方法调用。
         */
        @Bean
        V1ProtocolPrecheckService v1ProtocolPrecheckService() {
            return new V1ProtocolPrecheckService(null, null, null, null);
        }

        /**
         * 订单号生成器 stub，仅满足 Bean 依赖链。
         */
        @Bean
        RechargeOrderIdGenerator rechargeOrderIdGenerator() {
            return () -> "stub-order-id";
        }

        /**
         * DB 执行器 stub，仅满足 Bean 依赖链。
         */
        @Bean
        @Qualifier("tradeDbTaskExecutor")
        Executor tradeDbTaskExecutor() {
            return Runnable::run;
        }
    }
}
