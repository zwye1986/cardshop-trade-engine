package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.application.v1.crypto.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Clock;

/** 默认隔离、晚装配和缺依赖失败关闭；参见详细设计9.5。 */
class MinisoWriteOffAutoConfigurationTest {
    ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(MinisoWriteOffAutoConfiguration.class));

    @Test
    void disabledDoesNotRequireDatabaseOrSecrets() {
        runner.run(
                c -> {
                    assertThat(c).hasNotFailed();
                    assertThat(c).doesNotHaveBean(MinisoWriteOffStore.class);
                });
    }

    @Test
    void receptionWithoutProcessingAndProcessingWithoutDatabaseFailClosed() {
        runner.withPropertyValues("trade.engine.supplier.miniso.callback.enabled=true")
                .run(c -> assertThat(c).hasFailed());
        runner.withPropertyValues("trade.engine.supplier.miniso.callback.processing-enabled=true")
                .run(c -> assertThat(c).hasFailed());
    }

    @Test
    void processingContinuesWithNoInboundKeyOrOutboundHttp() {
        configured()
                .withPropertyValues("trade.engine.supplier.miniso.callback.processing-enabled=true")
                .run(
                        c -> {
                            assertThat(c).hasNotFailed();
                            assertThat(c).hasSingleBean(MinisoWriteOffStore.class);
                            assertThat(c).hasSingleBean(MinisoWriteOffProtection.class);
                            assertThat(c).doesNotHaveBean(ReceiveMinisoWriteOffService.class);
                        });
    }

    @Test
    void stoppingProcessingMustKeepFinancialProtectionAvailable() {
        configured()
                .withPropertyValues(
                        "trade.engine.supplier.miniso.callback.processing-enabled=false")
                .run(
                        c -> {
                            assertThat(c).hasNotFailed();
                            assertThat(c).doesNotHaveBean(MinisoWriteOffStore.class);
                            assertThat(c).hasSingleBean(MinisoWriteOffProtection.class);
                            assertThat(c)
                                    .hasSingleBean(
                                            com.cardshop.trade.engine.infrastructure.persistence
                                                    .MinisoReceiptStore.class);
                        });
    }

    @Test
    void enabledReceptionRequiresIndependentKey() {
        configured()
                .withPropertyValues(
                        "trade.engine.supplier.miniso.callback.enabled=true",
                        "trade.engine.supplier.miniso.callback.processing-enabled=true")
                .run(c -> assertThat(c).hasFailed());
        configured()
                .withPropertyValues(
                        "trade.engine.supplier.miniso.callback.enabled=true",
                        "trade.engine.supplier.miniso.callback.processing-enabled=true",
                        "trade.engine.supplier.miniso.callback.key2=test-key")
                .run(
                        c -> {
                            assertThat(c).hasNotFailed();
                            assertThat(c).hasSingleBean(ReceiveMinisoWriteOffService.class);
                        });
    }

    ApplicationContextRunner configured() {
        return runner.withBean(
                        org.apache.ibatis.session.SqlSessionFactory.class,
                        () -> mock(org.apache.ibatis.session.SqlSessionFactory.class))
                .withBean(MinisoReceiptMapper.class, () -> mock(MinisoReceiptMapper.class))
                .withBean(MinisoCouponGuardMapper.class, () -> mock(MinisoCouponGuardMapper.class))
                .withBean(MinisoSharedCallbackInboxMapper.class, () -> mock(MinisoSharedCallbackInboxMapper.class))
                .withBean(
                        MinisoWriteOffEventMapper.class,
                        () -> mock(MinisoWriteOffEventMapper.class))
                .withBean(RechargeNotifyTaskPort.class, () -> mock(RechargeNotifyTaskPort.class))
                .withBean(
                        PlatformTransactionManager.class,
                        () -> mock(PlatformTransactionManager.class))
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(
                        V12SigningCredentialStorePort.class,
                        () ->
                                new V12SigningCredentialStorePort() {
                                    public V12SigningCredential getOrCreateCurrent(String org) {
                                        throw new UnsupportedOperationException();
                                    }

                                    public java.util.Optional<V12SigningCredential> findCurrent(
                                            String org) {
                                        return java.util.Optional.empty();
                                    }

                                    public java.util.Optional<V12SigningCredential> findForCallback(
                                            String org, String id) {
                                        return java.util.Optional.empty();
                                    }

                                    public V12SigningCredential rotate(String org) {
                                        throw new UnsupportedOperationException();
                                    }

                                    public boolean revoke(String org, String id) {
                                        return false;
                                    }
                                });
    }
}
