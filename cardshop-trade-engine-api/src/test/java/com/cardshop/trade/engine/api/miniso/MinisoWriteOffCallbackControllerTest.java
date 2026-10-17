package com.cardshop.trade.engine.api.miniso;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.cardshop.trade.engine.application.recharge.*;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.*;

/** WebFlux入站确认和字节限额测试；参见详细设计9.5。 */
class MinisoWriteOffCallbackControllerTest {
    @Test
    void respondsOnlyAfterStoreCompletesAndRunsOffEventLoop() {
        var auth = mock(MinisoWriteOffAuthenticator.class);
        var store = mock(MinisoWriteOffStore.class);
        var thread = new java.util.concurrent.atomic.AtomicReference<String>();
        doAnswer(
                        call -> {
                            thread.set(Thread.currentThread().getName());
                            return null;
                        })
                .when(store)
                .accept(any());
        when(auth.authenticate(anyString(), any()))
                .thenReturn(
                        new MinisoWriteOffNotification(
                                "C1", java.time.LocalDateTime.of(2026, 10, 17, 9, 29)));
        try (var pool =
                Executors.newFixedThreadPool(1, r -> new Thread(r, "miniso-callback-test"))) {
            var scheduler = Schedulers.fromExecutorService(pool);
            try {
                var controller =
                        new MinisoWriteOffCallbackController(
                                new ReceiveMinisoWriteOffService(auth, store),
                                scheduler,
                                4096,
                                Duration.ofSeconds(5));
                WebTestClient.bindToController(controller)
                        .build()
                        .post()
                        .uri("/api/common/mcNotify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue("{}")
                        .exchange()
                        .expectStatus()
                        .isOk()
                        .expectBody()
                        .jsonPath("$.code")
                        .isEqualTo(200)
                        .jsonPath("$.msg")
                        .isEqualTo("接收成功");
                assertThat(thread.get()).startsWith("miniso-callback-test");
            } finally {
                scheduler.dispose();
            }
        }
    }

    @Test
    void storageFailureDoesNotAcknowledgeAndOverflowDoesNotInvokeUseCase() {
        var auth = mock(MinisoWriteOffAuthenticator.class);
        var store = mock(MinisoWriteOffStore.class);
        when(auth.authenticate(anyString(), any()))
                .thenReturn(
                        new MinisoWriteOffNotification(
                                "C1", java.time.LocalDateTime.of(2026, 10, 17, 9, 29)));
        doThrow(new MinisoWriteOffException(MinisoWriteOffException.Reason.DB_UNAVAILABLE))
                .when(store)
                .accept(any());
        var scheduler = Schedulers.newSingle("miniso-test");
        try {
            var client =
                    WebTestClient.bindToController(
                                    new MinisoWriteOffCallbackController(
                                            new ReceiveMinisoWriteOffService(auth, store),
                                            scheduler,
                                            16,
                                            Duration.ofSeconds(5)))
                            .build();
            client.post()
                    .uri("/api/common/mcNotify")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue("{}")
                    .exchange()
                    .expectStatus()
                    .isEqualTo(503)
                    .expectBody()
                    .jsonPath("$.code")
                    .isEqualTo(500);
            clearInvocations(auth, store);
            client.post()
                    .uri("/api/common/mcNotify")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(
                            Flux.just(new DefaultDataBufferFactory().wrap(new byte[32])),
                            org.springframework.core.io.buffer.DataBuffer.class)
                    .exchange()
                    .expectStatus()
                    .isEqualTo(413);
            verifyNoInteractions(auth, store);
        } finally {
            scheduler.dispose();
        }
    }

    @Test
    void successIsNotReturnedBeforePersistenceCompletes() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var auth = mock(MinisoWriteOffAuthenticator.class);
        var store = mock(MinisoWriteOffStore.class);
        when(auth.authenticate(anyString(), any()))
                .thenReturn(
                        new MinisoWriteOffNotification(
                                "C1", java.time.LocalDateTime.of(2026, 10, 17, 9, 29)));
        doAnswer(
                        call -> {
                            entered.countDown();
                            if (!release.await(3, TimeUnit.SECONDS))
                                throw new AssertionError("test latch expired");
                            return null;
                        })
                .when(store)
                .accept(any());
        var scheduler = Schedulers.newSingle("miniso-commit-barrier");
        try (var clientExecutor = Executors.newSingleThreadExecutor()) {
            var client =
                    WebTestClient.bindToController(
                                    new MinisoWriteOffCallbackController(
                                            new ReceiveMinisoWriteOffService(auth, store),
                                            scheduler,
                                            4096,
                                            Duration.ofSeconds(5)))
                            .build();
            var future =
                    clientExecutor.submit(
                            () ->
                                    client.post()
                                            .uri("/api/common/mcNotify")
                                            .contentType(MediaType.APPLICATION_JSON)
                                            .bodyValue("{}")
                                            .exchange()
                                            .expectStatus()
                                            .isOk());
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(future.isDone()).isFalse();
            release.countDown();
            future.get(3, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            scheduler.dispose();
        }
    }
}
