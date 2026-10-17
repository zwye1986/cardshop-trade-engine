package com.cardshop.trade.engine.bootstrap.admin;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.job.recharge.MinisoWriteOffProcessJob;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import java.net.InetSocketAddress;

/** 人工入口拒绝远程/代理及大批量调用；参见详细设计9.5。 */
class MinisoWriteOffManualJobControllerTest {
    @Test
    void acceptsOnlyLoopbackWithoutForwardingHeadersAndUsesExistingFacade() {
        var store = mock(MinisoWriteOffStore.class);
        when(store.processBatch(1, 0, 10, "worker"))
                .thenReturn(new MinisoWriteOffBatchResult(1, 1, 0, 0, 0, 0));
        var beans = new StaticListableBeanFactory();
        beans.addBean("job", new MinisoWriteOffProcessJob(store));
        var controller =
                new MinisoWriteOffManualJobController(
                        beans.getBeanProvider(MinisoWriteOffProcessJob.class));
        var request = new MinisoWriteOffManualJobController.Request(1, 0, 10, "worker");
        assertThat(
                        controller
                                .run(request, exchange("192.0.2.1", false))
                                .block()
                                .getStatusCode()
                                .value())
                .isEqualTo(403);
        assertThat(
                        controller
                                .run(request, exchange("127.0.0.1", true))
                                .block()
                                .getStatusCode()
                                .value())
                .isEqualTo(403);
        assertThat(
                        controller
                                .run(
                                        new MinisoWriteOffManualJobController.Request(
                                                1, 0, 101, "worker"),
                                        exchange("127.0.0.1", false))
                                .block()
                                .getStatusCode()
                                .value())
                .isEqualTo(400);
        verifyNoInteractions(store);
        assertThat(
                        controller
                                .run(request, exchange("127.0.0.1", false))
                                .block()
                                .getStatusCode()
                                .value())
                .isEqualTo(200);
        verify(store).processBatch(1, 0, 10, "worker");
    }

    static MockServerWebExchange exchange(String host, boolean forwarded) {
        var request =
                MockServerHttpRequest.post("/internal/integration/recharge/jobs/miniso-writeoff")
                        .remoteAddress(new InetSocketAddress(host, 12345));
        if (forwarded) request.header("X-Forwarded-For", "192.0.2.1");
        return MockServerWebExchange.from(request.build());
    }
}
