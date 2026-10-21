package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisMinisoWriteOffStore;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.cardshop.trade.engine.infrastructure.supplier.miniso.MinisoWriteOffAuthenticatorAdapter;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.*;

/** 名创核销晚装配，显式启用后依赖全部必需，禁止条件缺失退化noop。 参见docs/交行直充业务系统详细设计.md「9.5」。 */
@AutoConfiguration(
        afterName =
                "com.cardshop.trade.engine.infrastructure.config.TradeEngineMybatisPersistenceAutoConfiguration")
@EnableConfigurationProperties(MinisoWriteOffProperties.class)
public class MinisoWriteOffAutoConfiguration {
    @Bean
    public MinisoWriteOffObserver minisoWriteOffObserver(
            org.springframework.beans.factory.ObjectProvider<
                            io.micrometer.core.instrument.MeterRegistry>
                    registry,
            org.springframework.beans.factory.ObjectProvider<MinisoWriteOffEventMapper> events,
            org.springframework.beans.factory.ObjectProvider<Clock> clock) {
        var meters = registry.getIfAvailable();
        return meters == null
                ? MinisoWriteOffObserver.NOOP
                : new com.cardshop.trade.engine.infrastructure.observability.MinisoWriteOffMetrics(
                        meters, events, clock.getIfAvailable(Clock::systemUTC));
    }

    /** Mapper扫描晚于普通配置条件判断，凭据Store须在SqlSessionFactory之后兜底装配。 */
    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnBean(
            org.apache.ibatis.session.SqlSessionFactory.class)
    @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean(
            com.cardshop.trade.engine.infrastructure.persistence.MinisoReceiptStore.class)
    public com.cardshop.trade.engine.infrastructure.persistence.MinisoReceiptStore
            minisoReceiptStore(
                    MinisoReceiptMapper receipts,
                    MinisoCouponGuardMapper guards,
                    Clock clock,
                    org.springframework.beans.factory.ObjectProvider<MinisoWriteOffProtection>
                            protection) {
        return new com.cardshop.trade.engine.infrastructure.persistence.MinisoReceiptStore(
                receipts, guards, clock, protection::getObject);
    }

    /** 作废保护随真实持久化装配，关闭恢复Job不能绕过既有事件；参见docs/交行直充业务系统详细设计.md「9.5」。 */
    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnBean(
            org.apache.ibatis.session.SqlSessionFactory.class)
    public MinisoWriteOffProtection minisoWriteOffProtection(
            MinisoWriteOffEventMapper events,
            MinisoReceiptMapper receipts,
            MinisoCouponGuardMapper guards,
            MinisoSharedCallbackInboxMapper inbox,
            PlatformTransactionManager tm,
            Clock clock,
            MinisoWriteOffProperties settings) {
        return new com.cardshop.trade.engine.infrastructure.persistence
                .MybatisMinisoWriteOffProtection(
                events, receipts, guards, inbox, tm, clock, settings.getTransactionTimeoutSeconds());
    }

    @Bean
    @ConditionalOnProperty(
            prefix = "trade.engine.supplier.miniso.callback",
            name = "processing-enabled",
            havingValue = "true")
    public MybatisMinisoWriteOffStore minisoWriteOffStore(
            MinisoWriteOffEventMapper events,
            MinisoReceiptMapper receipts,
            MinisoCouponGuardMapper guards,
            RechargeNotifyTaskPort tasks,
            V12SigningCredentialStorePort credentials,
            PlatformTransactionManager tm,
            Clock clock,
            MinisoWriteOffProperties settings,
            MinisoWriteOffObserver observer) {
        if (!credentials.available())
            throw new IllegalStateException(
                    "MINISO processing requires persistent merchant signing-key store");
        return new MybatisMinisoWriteOffStore(
                events,
                receipts,
                guards,
                tasks,
                new V12RedemptionNotifyPayloadSigner(credentials),
                tm,
                clock,
                settings.getMaxRetryCount(),
                Duration.ofSeconds(settings.getRetryBackoffSeconds()),
                settings.getTransactionTimeoutSeconds(),
                observer);
    }

    /** 启动后校验0021关键列并初始化积压指标；只读，不标记一次Job成功。 */
    @Bean
    @ConditionalOnProperty(
            prefix = "trade.engine.supplier.miniso.callback",
            name = "processing-enabled",
            havingValue = "true")
    public org.springframework.boot.ApplicationRunner minisoWriteOffSchemaProbe(
            MinisoWriteOffEventMapper events,
            MinisoReceiptMapper receipts,
            MinisoWriteOffObserver observer,
            Clock clock) {
        return args -> {
            try {
                events.due(
                        LocalDateTime.ofInstant(clock.instant(), ZoneId.of("Asia/Shanghai")),
                        1,
                        0,
                        1);
                receipts.useTime("__miniso_schema_probe__");
            } catch (RuntimeException ex) {
                throw com.cardshop.trade.engine.infrastructure.persistence.MinisoWriteOffFailures
                        .storage(ex);
            }
            observer.started();
        };
    }

    @Bean
    @ConditionalOnProperty(
            prefix = "trade.engine.supplier.miniso.callback",
            name = "enabled",
            havingValue = "true")
    public ReceiveMinisoWriteOffService receiveMinisoWriteOffService(
            MinisoWriteOffStore store,
            MinisoWriteOffProperties settings,
            MinisoWriteOffObserver observer) {
        return new ReceiveMinisoWriteOffService(
                new MinisoWriteOffAuthenticatorAdapter(settings.getKey2()), store, observer);
    }
}
