package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.v1.V1CreateOrderStageObserver;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import io.lettuce.core.api.reactive.RedisScriptingReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

/**
 * 基于 Lettuce 原生命令的 Redis 会话工厂。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「14.2 Redis WAIT 协议」。Spring Data Redis
 * 的 reactive server commands 当前没有公开 WAIT，因此 D4 的可靠接单路径使用 Lettuce 原生命令执行 Lua 与 WAIT。
 * 这样可以保证同一个 session 上先写订单、再执行 WAIT，避免在另一条连接上得到无关的复制确认结果。
 */
public class LettuceRechargeRedisSessionFactory implements RechargeRedisSessionFactory {

    private final LettuceConnectionFactory connectionFactory;
    private final Supplier<V1CreateOrderStageObserver> stageObserverSupplier;

    public LettuceRechargeRedisSessionFactory(LettuceConnectionFactory connectionFactory) {
        this(connectionFactory, V1CreateOrderStageObserver.noop());
    }

    public LettuceRechargeRedisSessionFactory(
            LettuceConnectionFactory connectionFactory,
            V1CreateOrderStageObserver stageObserver) {
        this(connectionFactory, () -> stageObserver == null
                ? V1CreateOrderStageObserver.noop()
                : stageObserver);
    }

    public LettuceRechargeRedisSessionFactory(
            LettuceConnectionFactory connectionFactory,
            Supplier<V1CreateOrderStageObserver> stageObserverSupplier) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory must not be null");
        Objects.requireNonNull(stageObserverSupplier, "stageObserverSupplier must not be null");
        this.stageObserverSupplier = () -> {
            V1CreateOrderStageObserver observer = stageObserverSupplier.get();
            return observer == null ? V1CreateOrderStageObserver.noop() : observer;
        };
    }

    @Override
    public RechargeRedisSession openSession() {
        long startedNanos = System.nanoTime();
        try {
            AbstractRedisClient nativeClient = connectionFactory.getRequiredNativeClient();
            if (nativeClient instanceof RedisClusterClient clusterClient) {
                var connection = clusterClient.connect(StringCodec.UTF8);
                return new LettuceRechargeRedisSession(connection, connection.reactive(), connection.reactive());
            }
            if (nativeClient instanceof RedisClient redisClient) {
                var connection = redisClient.connect(StringCodec.UTF8);
                return new LettuceRechargeRedisSession(connection, connection.reactive(), connection.reactive());
            }
            throw new IllegalStateException("Unsupported Lettuce native client: " + nativeClient.getClass().getName());
        } finally {
            stageObserverSupplier.get().recordStage(
                    V1CreateOrderStageObserver.STAGE_REDIS_SESSION_CONNECT,
                    Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNanos)));
        }
    }

    private static final class LettuceRechargeRedisSession implements RechargeRedisSession {

        private final StatefulConnection<String, String> connection;
        private final RedisScriptingReactiveCommands<String, String> scriptingCommands;
        private final BaseRedisReactiveCommands<String, String> baseCommands;

        private LettuceRechargeRedisSession(
                StatefulConnection<String, String> connection,
                RedisScriptingReactiveCommands<String, String> scriptingCommands,
                BaseRedisReactiveCommands<String, String> baseCommands) {
            this.connection = connection;
            this.scriptingCommands = scriptingCommands;
            this.baseCommands = baseCommands;
        }

        @Override
        public Mono<Object> execute(RedisScript<?> script, List<String> keys, List<String> args) {
            String[] keyArray = keys.toArray(String[]::new);
            String[] argArray = args.toArray(String[]::new);
            return scriptingCommands
                    .eval(script.getScriptAsString(), ScriptOutputType.MULTI, keyArray, argArray)
                    .collectList()
                    .cast(Object.class);
        }

        @Override
        public Mono<Long> waitForReplication(int replicas, Duration timeout) {
            long timeoutMillis = Math.max(1L, timeout.toMillis());
            return baseCommands.waitForReplication(replicas, timeoutMillis);
        }

        @Override
        public void close() {
            // TODO(D5B): 引入专用连接池或共享串行 WAIT 通道，降低每笔接单新建连接的成本；参见 docs/交行直充业务系统高可用方案设计.md「14.2 Redis WAIT 协议」。
            connection.close();
        }
    }
}
