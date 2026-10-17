package com.cardshop.trade.engine.api.miniso;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.recharge.*;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.buffer.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.nio.charset.*;
import java.time.Duration;
import java.util.*;

/** 独立名创核销入站；参见docs/交行直充业务系统详细设计.md「9.5」。 字节限额在排队前执行，认证/数据库在有界专用池执行；只有T1提交后才返回成功。 */
@RestController
@ConditionalOnProperty(
        prefix = "trade.engine.supplier.miniso.callback",
        name = "enabled",
        havingValue = "true")
public final class MinisoWriteOffCallbackController {
    private static final Logger LOG =
            LoggerFactory.getLogger(MinisoWriteOffCallbackController.class);
    private final MinisoWriteOffObserver observer;
    private final ReceiveMinisoWriteOffService service;
    private final Scheduler scheduler;
    private final int maxBytes;
    private final Duration timeout;
    private final LogStormGuard logs = LogStormGuard.defaultGuard();

    @Autowired
    public MinisoWriteOffCallbackController(
            ReceiveMinisoWriteOffService service,
            @Qualifier("minisoWriteOffScheduler") Scheduler scheduler,
            MinisoWriteOffEndpointSettings settings,
            MinisoWriteOffObserver observer) {
        this(service, scheduler, settings.maxBodyBytes(), settings.timeout(), observer);
    }

    public MinisoWriteOffCallbackController(
            ReceiveMinisoWriteOffService service,
            Scheduler scheduler,
            int maxBytes,
            Duration timeout) {
        this(service, scheduler, maxBytes, timeout, MinisoWriteOffObserver.NOOP);
    }

    public MinisoWriteOffCallbackController(
            ReceiveMinisoWriteOffService service,
            Scheduler scheduler,
            int maxBytes,
            Duration timeout,
            MinisoWriteOffObserver observer) {
        this.observer = Objects.requireNonNull(observer);
        this.service = Objects.requireNonNull(service);
        this.scheduler = Objects.requireNonNull(scheduler);
        this.maxBytes = maxBytes;
        this.timeout = Objects.requireNonNull(timeout);
    }

    /** 固定回执，无data或签名字段；数字code与旧卡管一致。 */
    public record Reply(int code, String msg) {}

    @PostMapping("/api/common/mcNotify")
    public Mono<ResponseEntity<Reply>> receive(ServerWebExchange exchange) {
        return Mono.defer(
                        () -> {
                            var type = exchange.getRequest().getHeaders().getContentType();
                            if (type == null
                                    || !MediaType.APPLICATION_JSON.isCompatibleWith(type)
                                    || type.getCharset() != null
                                            && !StandardCharsets.UTF_8.equals(type.getCharset())) {
                                observer.event("invalid");
                                return Mono.just(reply(415, 500, "参数不合规"));
                            }
                            Map<String, List<String>> headers = new LinkedHashMap<>();
                            exchange.getRequest()
                                    .getHeaders()
                                    .forEach(
                                            (name, values) ->
                                                    headers.put(name, List.copyOf(values)));
                            return DataBufferUtils.join(exchange.getRequest().getBody(), maxBytes)
                                    .switchIfEmpty(
                                            Mono.error(
                                                    new MinisoWriteOffException(
                                                            MinisoWriteOffException.Reason
                                                                    .INVALID)))
                                    .map(
                                            buffer -> {
                                                try {
                                                    return StandardCharsets.UTF_8
                                                            .newDecoder()
                                                            .onMalformedInput(
                                                                    CodingErrorAction.REPORT)
                                                            .decode(buffer.asByteBuffer())
                                                            .toString();
                                                } catch (CharacterCodingException ex) {
                                                    throw new MinisoWriteOffException(
                                                            MinisoWriteOffException.Reason.INVALID);
                                                } finally {
                                                    DataBufferUtils.release(buffer);
                                                }
                                            })
                                    .flatMap(
                                            body ->
                                                    Mono.fromRunnable(
                                                                    () ->
                                                                            service.receive(
                                                                                    body, headers))
                                                            .subscribeOn(scheduler)
                                                            .timeout(timeout)
                                                            .thenReturn(reply(200, 200, "接收成功")));
                        })
                .onErrorResume(
                        ex -> {
                            int status = 503;
                            String message = "接收失败，请重试";
                            var reason =
                                    ex instanceof MinisoWriteOffException e
                                            ? e.reason()
                                            : MinisoWriteOffException.Reason.DB_UNAVAILABLE;
                            if (ex instanceof DataBufferLimitException) {
                                observer.event("rejected");
                                status = 413;
                                message = "参数不合规";
                            } else if (!(ex instanceof MinisoWriteOffException))
                                observer.event("rejected");
                            if (reason == MinisoWriteOffException.Reason.INVALID) {
                                status = 200;
                                message = "参数不合规";
                            } else if (reason == MinisoWriteOffException.Reason.SIGNATURE_FAILED) {
                                status = 200;
                                message = "验签失败";
                            } else if (reason == MinisoWriteOffException.Reason.CONFLICT) {
                                status = 200;
                                message = "核销信息冲突";
                            }
                            var sample = logs.shouldLog("miniso.callback." + reason.name());
                            // 原异常可能含请求或SQL，不传递cause；使用固定类别安全栈，避免错误回执泄密和日志风暴。
                            if (sample.shouldLog()) {
                                var safe =
                                        ex instanceof MinisoWriteOffException classified
                                                ? classified
                                                : new MinisoWriteOffException(reason);
                                if (status == 503)
                                    LOG.error(
                                            "名创核销接收失败 category={}, occurrence={}",
                                            reason,
                                            sample.occurrence(),
                                            safe);
                                else
                                    LOG.warn(
                                            "名创核销接收拒绝 category={}, occurrence={}",
                                            reason,
                                            sample.occurrence(),
                                            safe);
                            }
                            return Mono.just(reply(status, 500, message));
                        });
    }

    private static ResponseEntity<Reply> reply(int http, int code, String msg) {
        return ResponseEntity.status(http).body(new Reply(code, msg));
    }
}
