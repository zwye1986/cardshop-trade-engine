package com.cardshop.trade.engine.infrastructure.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.application.v1.crypto.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
                    assertThat(c).doesNotHaveBean(ReceiveMinisoWriteOffService.class);
                    assertThat(c.getBean(MinisoWriteOffProperties.class).isEnabled()).isFalse();
                    assertThat(c.getBean(MinisoWriteOffProperties.class).isProcessingEnabled()).isFalse();
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

    @Test
    void receptionCanPersistWhileProcessingIsPaused() {
        configured().withPropertyValues(
                "trade.engine.supplier.miniso.callback.enabled=true",
                "trade.engine.supplier.miniso.callback.processing-enabled=false",
                "trade.engine.supplier.miniso.callback.key2=test-key")
                .run(c -> {
                    assertThat(c).hasNotFailed();
                    assertThat(c).hasSingleBean(ReceiveMinisoWriteOffService.class);
                    assertThat(c).hasSingleBean(MinisoWriteOffStore.class);
                    assertThat(c).hasSingleBean(MinisoWriteOffProtection.class);
                });
    }

    @Test
    void protectionStillRejectsUnreadableEvidenceWhenProcessingIsOff() {
        configured().run(c -> {
            var receipts = c.getBean(MinisoReceiptMapper.class);
            var failure = new org.springframework.dao.DataAccessResourceFailureException("private SQL");
            when(receipts.lockReceipt("order")).thenThrow(failure);
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
            try {
                assertThatThrownBy(() -> c.getBean(MinisoWriteOffProtection.class).assertVoidAllowed("order"))
                        .isSameAs(failure);
            } finally {
                org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"events", "terminal-decision"})
    void receiveOnlyProbeFailsClosedOnMissingPersistentStructure(String structure) {
        configured().withPropertyValues(
                "trade.engine.supplier.miniso.callback.enabled=true",
                "trade.engine.supplier.miniso.callback.key2=test-key")
                .run(c -> {
                    assertThat(c).hasNotFailed();
                    var mapper=c.getBean(MinisoWriteOffEventMapper.class);
                    var failure = new org.springframework.dao.DataAccessResourceFailureException("private SQL");
                    if (structure.equals("events")) when(mapper.due(any(),anyInt(),anyInt(),anyInt())).thenThrow(failure);
                    else when(mapper.terminalDecisionPresent(anyString())).thenThrow(failure);
                    assertThatThrownBy(() -> c.getBean("minisoWriteOffSchemaProbe",org.springframework.boot.ApplicationRunner.class)
                            .run(new org.springframework.boot.DefaultApplicationArguments()))
                            .isInstanceOf(MinisoWriteOffException.class).hasMessageStartingWith("DB_UNAVAILABLE")
                            .hasMessageNotContaining("private SQL");
                });
    }

    @Test
    void processingOffStillRejectsSharedEvidenceBeforeFirstRefund() {
        configured().run(c -> {
            var receipts=c.getBean(MinisoReceiptMapper.class);
            var inbox=c.getBean(MinisoSharedCallbackInboxMapper.class);
            when(receipts.lockReceipt("order")).thenReturn(new MinisoFulfillmentReceipt(
                    "order","org",1,"coupon","10000000000","2026-10-01 00:00:00","2026-11-01 00:00:00",0));
            when(inbox.lockByCoupon("coupon")).thenReturn(new MinisoSharedCallbackInboxMapper.Evidence(
                    "event",java.time.LocalDateTime.of(2026,10,22,10,0),"MANUAL_REQUIRED"));
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
            try {
                assertThatThrownBy(() -> c.getBean(MinisoWriteOffProtection.class).assertVoidAllowed("order"))
                        .isInstanceOf(MinisoWriteOffException.class).hasMessage("REDEEM_VOID_CONFLICT");
            } finally {
                org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
            }
        });
    }

    /** 交行准备器与桥接开关组合，接收/银行处理不能错误要求商户HMAC。参见详细设计9.8和2104实施细化。 */
    @Test
    void bankPreparationAndReceiveOnlyWorkWithoutMerchantCredentialStore() {
        var noKeys = configured().withBean("unavailableMerchantKeys", V12SigningCredentialStorePort.class,
                V12SigningCredentialStorePort::unavailable, definition -> definition.setPrimary(true));
        noKeys.withPropertyValues("trade.engine.supplier.miniso.callback.enabled=true",
                "trade.engine.supplier.miniso.callback.key2=local-key").run(c -> assertThat(c).hasNotFailed());
        noKeys.withPropertyValues("trade.engine.supplier.miniso.callback.processing-enabled=true")
                .run(c -> assertThat(c).hasFailed());
        noKeys.withBean(MerchantNotifyPreparationService.class, () -> mock(MerchantNotifyPreparationService.class))
                .withPropertyValues("trade.engine.supplier.miniso.callback.processing-enabled=true")
                .run(c -> {
                    assertThat(c).hasNotFailed();
                    assertThat(c).hasSingleBean(MinisoWriteOffStore.class);
                    assertThat(c).hasSingleBean(MinisoWriteOffProtection.class);
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
