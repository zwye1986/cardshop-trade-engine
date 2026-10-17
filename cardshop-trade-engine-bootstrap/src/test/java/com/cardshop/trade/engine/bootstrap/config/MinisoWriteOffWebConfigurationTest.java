package com.cardshop.trade.engine.bootstrap.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.cardshop.trade.engine.api.miniso.*;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.config.MinisoWriteOffProperties;
import com.cardshop.trade.engine.job.recharge.MinisoWriteOffProcessJob;
import com.xxl.job.core.handler.annotation.XxlJob;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ReactiveWebApplicationContextRunner;
import org.springframework.context.annotation.*;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.config.EnableWebFlux;

import java.time.LocalDateTime;
import java.util.concurrent.ThreadPoolExecutor;

/** 真正WebFlux路由/有界池与XXL门面装配，默认关闭不注册入口；参见详细设计9.5。 */
class MinisoWriteOffWebConfigurationTest {
    ReactiveWebApplicationContextRunner runner =
            new ReactiveWebApplicationContextRunner()
                    .withUserConfiguration(
                            Beans.class,
                            MinisoWriteOffWebConfiguration.class,
                            MinisoWriteOffCallbackController.class,
                            MinisoWriteOffJobConfiguration.class);

    @Test
    void disabledRouteReturns404() {
        runner.run(
                c -> {
                    assertThat(c).hasNotFailed();
                    assertThat(c).doesNotHaveBean(MinisoWriteOffProcessJob.class);
                    WebTestClient.bindToApplicationContext(c)
                            .build()
                            .post()
                            .uri("/api/common/mcNotify")
                            .exchange()
                            .expectStatus()
                            .isNotFound();
                });
    }

    @Test
    void enabledRouteUsesBoundedExecutorAndRegistersRecoveryEvenWithoutOutboundHttp() {
        runner.withPropertyValues(
                        "trade.engine.supplier.miniso.callback.enabled=true",
                        "trade.engine.supplier.miniso.callback.processing-enabled=true",
                        "trade.engine.supplier.miniso.callback.key2=local-test")
                .run(
                        c -> {
                            assertThat(c).hasNotFailed();
                            var pool = c.getBean(ThreadPoolExecutor.class);
                            assertThat(pool.getMaximumPoolSize()).isEqualTo(4);
                            assertThat(pool.getQueue().remainingCapacity()).isEqualTo(256);
                            WebTestClient.bindToApplicationContext(c)
                                    .build()
                                    .post()
                                    .uri("/api/common/mcNotify")
                                    .header("Content-Type", "application/json")
                                    .bodyValue("{}")
                                    .exchange()
                                    .expectStatus()
                                    .isOk()
                                    .expectBody()
                                    .jsonPath("$.code")
                                    .isEqualTo(200);
                            var handler = c.getBean(MinisoWriteOffXxlHandler.class);
                            assertThat(
                                            MinisoWriteOffXxlHandler.class
                                                    .getMethod("execute")
                                                    .getAnnotation(XxlJob.class)
                                                    .value())
                                    .isEqualTo("minisoWriteOffProcessJob");
                            handler.runOnce(4, 2, "limit=25,workerId=test-worker");
                            verify(c.getBean(MinisoWriteOffStore.class))
                                    .processBatch(4, 2, 25, "test-worker");
                        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableWebFlux
    @EnableConfigurationProperties(MinisoWriteOffProperties.class)
    static class Beans {
        @Bean
        MinisoWriteOffObserver observer() {
            return MinisoWriteOffObserver.NOOP;
        }

        @Bean
        MinisoWriteOffStore store() {
            return mock(MinisoWriteOffStore.class);
        }

        @Bean
        ReceiveMinisoWriteOffService receive(MinisoWriteOffStore store) {
            return new ReceiveMinisoWriteOffService(
                    (body, headers) ->
                            new MinisoWriteOffNotification(
                                    "C1", LocalDateTime.of(2026, 10, 17, 9, 29)),
                    store);
        }
    }
}
