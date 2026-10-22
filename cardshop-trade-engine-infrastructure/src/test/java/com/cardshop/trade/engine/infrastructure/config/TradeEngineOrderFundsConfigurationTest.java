package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.application.v1.V1MerchantProfilePort;
import java.time.Clock;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.test.util.ReflectionTestUtils;

/** 履约Worker必须使用独立资金端口，不能在缺端口时静默降级；参见详细设计独立资金协议。 */
class TradeEngineOrderFundsConfigurationTest {
    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(TradeEngineFulfillmentAutoConfiguration.class))
                .withBean(TradeEngineProperties.class, TradeEngineProperties::new)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(RechargeOutboxPort.class, () -> mock(RechargeOutboxPort.class))
                .withBean(RechargeFulfillmentOrderPort.class, () -> mock(RechargeFulfillmentOrderPort.class))
                .withBean(RechargeSupplierGatewayPort.class, () -> mock(RechargeSupplierGatewayPort.class))
                .withBean(SupplierQueryOutboxPort.class, () -> mock(SupplierQueryOutboxPort.class))
                .withBean(DirectRechargeFrequencyPort.class, () -> mock(DirectRechargeFrequencyPort.class))
                .withBean(BalanceDeltaFlushOutboxPort.class, () -> mock(BalanceDeltaFlushOutboxPort.class))
                .withBean(BalanceReservationReleasePort.class, () -> mock(BalanceReservationReleasePort.class))
                .withBean(RechargeNotifyTaskPort.class, () -> mock(RechargeNotifyTaskPort.class))
                .withBean(V1MerchantProfilePort.class, () -> mock(V1MerchantProfilePort.class));
    }
    @Test void workerInjectsRequiredFundsPort() {
        runner().withBean(RechargeOrderFundsPort.class, () -> mock(RechargeOrderFundsPort.class))
                .run(context -> {
                    assertThat(context).hasNotFailed().hasSingleBean(RechargeOutboxWorkerService.class);
                    assertThat(ReflectionTestUtils.getField(context.getBean(RechargeOutboxWorkerService.class), "orderFunds"))
                            .isSameAs(context.getBean(RechargeOrderFundsPort.class));
                });
    }
    @Test void missingTransactionInfrastructureCannotEnableUnsafeWorker() {
        runner().run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(RechargeOutboxWorkerService.class));
    }
}
