package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderStatus;
import com.cardshop.trade.engine.application.recharge.RedisHaGuardPort;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

/**
 * Redis 接单适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
 * 适配器必须在 Redis 写开关关闭时 fail closed；写开关打开时只能调用固定 Lua 脚本并映射返回结果。
 */
class RedisAcceptRechargeOrderAdapterTest {

    @Test
    void returnsWriteDisabledWithoutCallingRedisWhenSwitchClosed() {
        var executor = new RecordingScriptExecutor(Mono.error(new AssertionError("should not call redis")));
        var properties = properties(false);
        var adapter = new RedisAcceptRechargeOrderAdapter(
                executor,
                new RechargeRedisKeyBuilder("cardshop:trade", 128),
                new RechargeRedisScriptLoader(),
                properties);

        var result = adapter.accept(sampleCommand()).toCompletableFuture().join();

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.REDIS_WRITE_DISABLED);
        assertThat(result.orderWritten()).isFalse();
        assertThat(executor.calls).isZero();
    }

    @Test
    void returnsWriteDisabledWithoutCallingRedisWhenHaGuardClosed() {
        var executor = new RecordingScriptExecutor(Mono.error(new AssertionError("should not call redis")));
        var adapter = new RedisAcceptRechargeOrderAdapter(
                executor,
                new RechargeRedisKeyBuilder("cardshop:trade", 128),
                new RechargeRedisScriptLoader(),
                properties(true),
                closedGuard());

        var result = adapter.accept(sampleCommand()).toCompletableFuture().join();

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.REDIS_WRITE_DISABLED);
        assertThat(result.orderWritten()).isFalse();
        assertThat(executor.calls).isZero();
    }

    @Test
    void executesLuaAndMapsSuccessfulResult() {
        var executor = new RecordingScriptExecutor(Mono.just(List.of(
                        "code", "ACCEPTED",
                        "order_id", "202607051337458961474292",
                        "validation_status", "PASSED",
                        "fail_code", "",
                        "fail_reason", "",
                        "dispatch_allowed", "1",
                        "balance_reserved", "1",
                        "order_version", "1")));
        var adapter = new RedisAcceptRechargeOrderAdapter(
                executor,
                new RechargeRedisKeyBuilder("cardshop:trade", 128),
                new RechargeRedisScriptLoader(),
                properties(true));

        var result = adapter.accept(sampleCommand()).toCompletableFuture().join();

        assertThat(result.status()).isEqualTo(AcceptRechargeOrderStatus.ACCEPTED);
        assertThat(result.orderId()).isEqualTo("202607051337458961474292");
        assertThat(result.dispatchAllowed()).isTrue();
        assertThat(result.balanceReserved()).isTrue();
        assertThat(executor.calls).isEqualTo(1);
        assertThat(executor.keys).hasSize(12);
        assertThat(executor.args).hasSize(28);
    }

    private static TradeEngineProperties properties(boolean writeEnabled) {
        var properties = new TradeEngineProperties();
        properties.getRedis().setWriteEnabled(writeEnabled);
        return properties;
    }

    private static RedisHaGuardPort closedGuard() {
        return new RedisHaGuardPort() {
            @Override
            public boolean isWriteAllowed() {
                return false;
            }

            @Override
            public void closeWrite(String reason, java.util.Map<String, Object> context) {
            }
        };
    }

    private static AcceptRechargeOrderCommand sampleCommand() {
        return new AcceptRechargeOrderCommand(
                "A36",
                "app-1",
                "OUT-1001",
                "202607051337458961474292",
                "73301280",
                "73301289",
                "18186222336",
                "http://merchant.example/callback",
                "nonce-12345678901234567890123456",
                1,
                5200L,
                "话费50元",
                5000L,
                1,
                "codekey",
                "TF_KA",
                "hash-v1",
                "v1",
                "SHA-256",
                ValidationStatus.PASSED,
                "",
                "",
                7L,
                11L,
                1783224958000L,
                1783225258000L);
    }

    private static final class RecordingScriptExecutor implements RechargeRedisScriptExecutor {

        private final Mono<Object> result;
        private int calls;
        private List<String> keys = List.of();
        private List<String> args = List.of();

        private RecordingScriptExecutor(Mono<Object> result) {
            this.result = result;
        }

        @Override
        public Mono<Object> execute(RedisScript<?> script, List<String> keys, List<String> args) {
            this.calls++;
            this.keys = keys;
            this.args = args;
            return result;
        }
    }
}
