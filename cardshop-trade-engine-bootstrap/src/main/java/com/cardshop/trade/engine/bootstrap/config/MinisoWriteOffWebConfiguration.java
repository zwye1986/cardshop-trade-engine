package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.api.miniso.MinisoWriteOffEndpointSettings;
import com.cardshop.trade.engine.infrastructure.config.MinisoWriteOffProperties;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;

import reactor.core.scheduler.*;

import java.time.Duration;
import java.util.concurrent.*;

/** 核销入口独立有界池，保护Netty及既有派单/通知资源；参见docs/交行直充业务系统详细设计.md「9.5」。 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
        prefix = "trade.engine.supplier.miniso.callback",
        name = "enabled",
        havingValue = "true")
public class MinisoWriteOffWebConfiguration {
    @Bean(destroyMethod = "shutdownNow")
    public ThreadPoolExecutor minisoWriteOffExecutor(MinisoWriteOffProperties settings) {
        return new ThreadPoolExecutor(
                settings.getReceiveThreads(),
                settings.getReceiveThreads(),
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(settings.getReceiveQueueCapacity()),
                Thread.ofPlatform().name("miniso-callback-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    @Bean(destroyMethod = "dispose")
    public Scheduler minisoWriteOffScheduler(
            @org.springframework.beans.factory.annotation.Qualifier("minisoWriteOffExecutor")
                    ThreadPoolExecutor minisoWriteOffExecutor) {
        return Schedulers.fromExecutorService(minisoWriteOffExecutor);
    }

    @Bean
    public MinisoWriteOffEndpointSettings minisoWriteOffEndpointSettings(
            MinisoWriteOffProperties settings) {
        return new MinisoWriteOffEndpointSettings(
                settings.getMaxBodyBytes(),
                Duration.ofSeconds(settings.getTransactionTimeoutSeconds()));
    }
}
