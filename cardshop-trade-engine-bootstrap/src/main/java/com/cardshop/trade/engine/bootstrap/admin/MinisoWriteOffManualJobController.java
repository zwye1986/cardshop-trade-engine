package com.cardshop.trade.engine.bootstrap.admin;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.job.recharge.MinisoWriteOffProcessJob;

import org.slf4j.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/** 仅dev/test本机且拒绝代理头的核销恢复人工入口；参见docs/交行直充业务系统详细设计.md「9.5」、10.1。 */
@RestController
@Profile({"dev", "test"})
@ConditionalOnProperty(
        prefix = "trade.engine.integration",
        name = "manual-job-enabled",
        havingValue = "true")
public final class MinisoWriteOffManualJobController {
    private static final Logger LOG =
            LoggerFactory.getLogger(MinisoWriteOffManualJobController.class);
    private final ObjectProvider<MinisoWriteOffProcessJob> jobs;
    private final LogStormGuard logs = LogStormGuard.defaultGuard();

    public MinisoWriteOffManualJobController(ObjectProvider<MinisoWriteOffProcessJob> jobs) {
        this.jobs = jobs;
    }

    /** 只接受固定job分片和小批量请求，不提供任意方法调用。 */
    public record Request(int shardTotal, int shardIndex, int limit, String workerId) {}

    @PostMapping("/internal/integration/recharge/jobs/miniso-writeoff")
    public Mono<ResponseEntity<Object>> run(
            @RequestBody Request request, ServerWebExchange exchange) {
        return Mono.defer(
                        () -> {
                            var address = exchange.getRequest().getRemoteAddress();
                            var headers = exchange.getRequest().getHeaders();
                            if (address == null
                                    || address.getAddress() == null
                                    || !address.getAddress().isLoopbackAddress()
                                    || headers.containsKey("Forwarded")
                                    || headers.containsKey("X-Forwarded-For"))
                                return response(403, "Loopback only");
                            if (request.limit() < 1
                                    || request.limit() > 100
                                    || request.shardTotal() < 1
                                    || request.shardTotal() > 128
                                    || request.shardIndex() < 0
                                    || request.shardIndex() >= request.shardTotal()
                                    || request.workerId() == null
                                    || !request.workerId().matches("[A-Za-z0-9._:-]{1,80}"))
                                return response(400, "Invalid parameters");
                            var job = jobs.getIfAvailable();
                            if (job == null) return response(503, "MINISO processor disabled");
                            return Mono.fromCallable(
                                            () ->
                                                    ResponseEntity.ok(
                                                            (Object)
                                                                    job.runOnce(
                                                                            request.shardTotal(),
                                                                            request.shardIndex(),
                                                                            request.limit(),
                                                                            request.workerId())))
                                    .subscribeOn(Schedulers.boundedElastic())
                                    .timeout(Duration.ofSeconds(30));
                        })
                .onErrorResume(
                        ex -> {
                            if (logs.shouldLog("miniso.manual.failed").shouldLog())
                                LOG.error(
                                        "名创核销人工恢复失败",
                                        new MinisoWriteOffException(
                                                MinisoWriteOffException.Reason.DB_UNAVAILABLE));
                            return response(503, "MINISO processing failed");
                        });
    }

    private static Mono<ResponseEntity<Object>> response(int status, String message) {
        return Mono.just(ResponseEntity.status(status).body(message));
    }
}
